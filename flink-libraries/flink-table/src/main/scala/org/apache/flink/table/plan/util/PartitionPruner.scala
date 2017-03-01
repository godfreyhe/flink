/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.util

import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rex.{RexCall, RexNode, RexVisitorImpl}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenerator, Compiler}
import org.apache.flink.table.expressions.{And, Expression, ResolvedFieldReference,
UnresolvedFieldReference}
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.sources.Partition
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq

/**
  * The base class for partition pruning.
  *
  * Creates partition filter instance (a [[FlatMapFunction]]) with partition predicate by code-gen,
  * and then evaluates all partition values against the partition filter to get final partitions.
  *
  */
abstract class PartitionPruner extends Compiler[FlatMapFunction[Row, Boolean]] {

  /**
    * get pruned partitions from all partitions by partition filters
    *
    * @param partitionFieldNames partition field names
    * @param partitionFieldTypes partition field types
    * @param allPartitions       all partition values
    * @param partitionPredicate  a filter expression that will be applied to partition values
    * @param relBuilder          Builder for relational expressions
    * @return pruned partitions
    */
  def getPrunedPartitions(
    partitionFieldNames: Array[String],
    partitionFieldTypes: Array[TypeInformation[_]],
    allPartitions: Seq[Partition],
    partitionPredicate: Array[Expression],
    relBuilder: RelBuilder): Seq[Partition] = {

    if (allPartitions.isEmpty) {
      return allPartitions
    }

    val newPredicate = rewritePartitionPredicate(partitionPredicate)

    // convert predicate to RexNode
    val typeFactory = new FlinkTypeFactory(RelDataTypeSystem.DEFAULT)
    val relDataType = typeFactory.buildRowDataType(partitionFieldNames, partitionFieldTypes)
    val predicateRexNode = convertPredicateToRexNode(newPredicate, relBuilder, relDataType)

    // Validating whether ScalaFunctions contain overriding open and close methods
    predicateRexNode.accept(new ScalarFunctionValidator())

    val config = new TableConfig
    val rowType = new RowTypeInfo(partitionFieldTypes, partitionFieldNames)
    val returnType = BasicTypeInfo.BOOLEAN_TYPE_INFO.asInstanceOf[TypeInformation[Any]]

    val generator = new CodeGenerator(config, false, rowType.asInstanceOf[TypeInformation[Any]])

    val filterExpression = generator.generateExpression(predicateRexNode)

    val filterFunctionBody =
      s"""
         |${filterExpression.code}
         |if (${filterExpression.resultTerm}) {
         |  ${generator.collectorTerm}.collect(true);
         |} else {
         |  ${generator.collectorTerm}.collect(false);
         |}
         |""".stripMargin

    val genFunction = generator.generateFunction(
      "PartitionPruner",
      classOf[FlatMapFunction[Row, Boolean]],
      filterFunctionBody,
      returnType)

    // create filter class instance
    val clazz = compile(getClass.getClassLoader, genFunction.name, genFunction.code)
    val function = clazz.newInstance()

    val results: util.List[Boolean] = new util.ArrayList[Boolean](allPartitions.size)
    val collector = new ListCollector[Boolean](results)

    // do filter
    allPartitions.foreach {
      case p =>
        val row = partitionToRow(partitionFieldNames, p)
        function.flatMap(row, collector)
    }

    // get pruned partitions
    allPartitions.zipWithIndex.filter {
      case (_, index) => results.get(index)
    }.unzip._1
  }

  /**
    * Sub-class can rewrite the partition predicate to meet different needs of partition-format.
    *
    * For example:
    * partition format is a time interval which represents a period of time between two instants.
    * interval partitions look like:
    * [2017-01-01T00:00:00.000, 2017-01-03T00:00:00.000)
    * [2017-01-03T00:00:00.000, 2017-01-05T00:00:00.000)
    * [2017-01-05T00:00:00.000, 2017-01-07T00:00:00.000)
    *
    * if the origin expression is "partition_field = '2017-01-01'",
    * you can use a ScalarFunction (such as UDF_INTERVAL_INCLUDE) to replace the expression's logic,
    * rewritten expression could be "UDF_INTERVAL_INCLUDE(partition_field, '2017-01-01') === true"
    *
    * @param partitionPredicate The origin partition predicate
    * @return The rewritten partition predicate
    */
  def rewritePartitionPredicate(partitionPredicate: Array[Expression]): Array[Expression]

  /**
    * create new Row from partition, set partition values to corresponding positions of row.
    */
  private def partitionToRow(
    partitionFieldNames: Array[String],
    partition: Partition): Row = {

    val row = new Row(partitionFieldNames.length)
    partitionFieldNames.zipWithIndex.foreach {
      case (fieldName, index) =>
        val value = partition.getFieldValue(fieldName)
        row.setField(index, value)
    }
    row
  }

  private def convertPredicateToRexNode(
    predicate: Array[Expression],
    relBuilder: RelBuilder,
    relDataType: RelDataType): RexNode = {

    relBuilder.values(relDataType)
    val resolvedExps = resolveFields(predicate, relDataType)
    val fullExp: Option[Expression] = conjunct(resolvedExps)
    if (fullExp.isDefined) {
      fullExp.get.toRexNode(relBuilder)
    } else {
      null
    }
  }

  private def resolveFields(
    predicate: Array[Expression],
    inType: RelDataType): Array[Expression] = {
    val fieldTypes: Map[String, TypeInformation[_]] = inType.getFieldList
      .map(f => f.getName -> FlinkTypeFactory.toTypeInfo(f.getType))
      .toMap
    val rule: PartialFunction[Expression, Expression] = {
      case u@UnresolvedFieldReference(name) =>
        ResolvedFieldReference(name, fieldTypes(name))
    }
    predicate.map(_.postOrderTransform(rule))
  }

  private def conjunct(exps: Array[Expression]): Option[Expression] = {
    def overIndexes(): IndexedSeq[Expression] = {
      for {
        i <- exps.indices by 2
      } yield {
        if (i + 1 < exps.length) {
          And(exps(i), exps(i + 1))
        } else {
          exps(i)
        }
      }
    }
    exps.length match {
      case 0 =>
        None
      case 1 =>
        Option(exps(0))
      case _ =>
        conjunct(overIndexes().toArray)
    }
  }

}

/**
  * Validating whether ScalaFunctions contain overriding open and close methods
  *
  * open and close methods in [[org.apache.flink.table.functions.UserDefinedFunction]]
  * must be evaluated with [[org.apache.flink.api.common.functions.RuntimeContext]]
  */
class ScalarFunctionValidator extends RexVisitorImpl[RexNode](true) {

  override def visitCall(call: RexCall): RexNode = {

    call.getOperator match {
      case e: ScalarSqlFunction =>
        val udfClass = e.getScalarFunction.getClass
        try {
          // check open method
          udfClass.getDeclaredMethod("open", classOf[FunctionContext])
          throw new TableException(s"overriding 'open' method in ${udfClass.getCanonicalName}" +
            " can't be evaluated when applying partition pruning")
        } catch {
          case ex: NoSuchMethodException => // do nothing
        }
        try {
          // check close method
          udfClass.getDeclaredMethod("close")
          throw new TableException(s"overriding 'close' method in ${udfClass.getCanonicalName}" +
            " can't be evaluated when applying partition pruning")
        } catch {
          case ex: NoSuchMethodException => // do nothing
        }
      case _ => // do nothing
    }

    super.visitCall(call)
  }
}

/**
  * Default implementation of PartitionPruner
  */
class DefaultPartitionPrunerImpl extends PartitionPruner {

  // by default returns all predicates.
  override def rewritePartitionPredicate(
    partitionPredicate: Array[Expression]): Array[Expression] = partitionPredicate
}

object PartitionPruner {
  val DEFAULT = new DefaultPartitionPrunerImpl
}
