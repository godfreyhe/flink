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

package org.apache.flink.table.plan.rules.util

import java.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexNode, RexVisitorImpl}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenerator, Compiler}
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.sources.Partition
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

abstract class PartitionPruner extends Compiler[FlatMapFunction[Row, Boolean]] {

  def getPrunedPartitions(
    partitionFieldNames: Array[String],
    logicalRowType: RelDataType,
    allPartitions: Seq[Partition],
    partitionCondition: RexNode): Seq[Partition] = {

    if (allPartitions.isEmpty) {
      return allPartitions
    }

    val newCondition = rewritePartitionCondition(partitionCondition)
    // Validating whether ScalaFunctions contain overriding open and close methods
    newCondition.accept(new ScalarFunctionValidator())

    val config = new TableConfig
    // convert to type information
    val allFieldTypes = logicalRowType.getFieldList map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // get field names
    val allFieldNames = logicalRowType.getFieldNames.toList
    val rowType = new RowTypeInfo(allFieldTypes.toArray, allFieldNames.toArray)
    val returnType = BasicTypeInfo.BOOLEAN_TYPE_INFO.asInstanceOf[TypeInformation[Any]]

    val generator = new CodeGenerator(config, false, rowType.asInstanceOf[TypeInformation[Any]])

    val filterExpression = generator.generateExpression(newCondition)

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
        val row = partitionToRow(allFieldNames, partitionFieldNames, p)
        function.flatMap(row, collector)
    }

    // get pruned partitions
    allPartitions.zipWithIndex.filter {
      case (_, index) => results.get(index)
    }.unzip._1
  }

  /**
    * You can rewrite the partition condition to meet different needs of partition-format
    *
    * For example:
    * partition format is a time interval which represents a period of time between two instants.
    * interval partitions look like:
    * [2017-01-01T00:00:00.000, 2017-01-03T00:00:00.000)
    * [2017-01-03T00:00:00.000, 2017-01-05T00:00:00.000)
    * [2017-01-05T00:00:00.000, 2017-01-07T00:00:00.000)
    *
    * if the origin partition condition is "partition_field = '2017-01-01'",
    * you can use udf_interval_include to replace the logic of "partition_field = '2017-01-01'",
    * rewritten partition condition could be "udf_interval_include(partition_field, '2017-01-01')"
    *
    * Notes: do not change the index value of RexInputRef
    *
    * @param partitionCondition The origin partition condition
    * @return The rewritten partition condition
    */
  def rewritePartitionCondition(partitionCondition: RexNode): RexNode

  /**
    * create new Row from partition
    * set partition values to corresponding positions of row, and set null value to left positions.
    *
    * For example:
    * There are 4 fields: 'f1', 'f2', 'f3', 'f4'. 'f3' is the partition field.
    * the return row will look like as following:
    * +------+------+-----------------+------+
    * | null | null | partition-value | null |
    * +------+------+-----------------+------+
    */
  private def partitionToRow(
    allFieldNames: List[String],
    partitionFieldNames: Array[String],
    partition: Partition): Row = {

    val row = new Row(allFieldNames.length)
    allFieldNames.zipWithIndex.foreach {
      case (fieldName, index) =>
        val value = if (partitionFieldNames.contains(fieldName)) {
          partition.getFieldValue(fieldName)
        } else {
          null
        }
        row.setField(index, value)
    }
    row
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

class DefaultPartitionPrunerImpl extends PartitionPruner {

  override def rewritePartitionCondition(partitionCondition: RexNode): RexNode = partitionCondition
}

object PartitionPruner {
  val DEFAULT = new DefaultPartitionPrunerImpl
}
