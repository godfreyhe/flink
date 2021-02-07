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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Calc, Filter, Project}
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rel.rules.ProjectRemoveRule
import org.apache.calcite.rex.{RexNode, RexProgram}
import org.apache.calcite.sql.dialect.AnsiSqlDialect

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for [[Calc]].
 */
class StreamPhysicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamPhysicalCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamPhysicalCalc(cluster, traitSet, child, program, outputRowType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val condition = if (calcProgram.getCondition != null) {
      calcProgram.expandLocalRef(calcProgram.getCondition)
    } else {
      null
    }

    testProject(projection)
    testFilter(condition)

    new StreamExecCalc(
      calcProgram,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }

  private def testProject(projection: Seq[RexNode]): Unit = {
    val converter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val context = getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val relBuilder = context.getPlanner.asInstanceOf[PlannerBase].getRelBuilder
    relBuilder.values(getInput.getRowType)
    relBuilder.project(projection: _*)
    val project = relBuilder.build()
    val result = converter.visitRoot(project)
    val sqlNode = result.asStatement
    val sql = sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
    println(sql)
    println()
    val operation = context.getPlanner.getParser.parse(sql).head
    operation match {
      case o: PlannerQueryOperation =>
        o.getCalciteTree match {
          case p: Project =>
            Preconditions.checkArgument(p.getProjects.size()== projection.size)
            projection.zip(p.getProjects).foreach {
              case (oldProject, newProject) =>
              assertRexNode(oldProject, newProject)
            }

          case _ => throw new TableException("expected project, actually is " + o.getCalciteTree)
        }
      case _ => throw new TableException("expected PlannerQueryOperation, actually is " + operation)
    }

  }

  private def testFilter(condition: RexNode): Unit = {
    if (condition == null) {
      return
    }
    val converter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val context = getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val relBuilder = context.getPlanner.asInstanceOf[PlannerBase].getRelBuilder
    relBuilder.values(getInput.getRowType)
    relBuilder.filter(condition)
    val filter = relBuilder.build()
    val result = converter.visitRoot(filter)
    val sqlNode = result.asStatement
    val sql = sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
    println(sql)
    println()
    val operation = context.getPlanner.getParser.parse(sql).head
    operation match {
      case o: PlannerQueryOperation =>
        o.getCalciteTree match {
          case f: Filter =>
            assertRexNode(condition, f.getCondition)
          case p: Project if ProjectRemoveRule.isTrivial(p) && p.getInput.isInstanceOf[Filter] =>
            val f = p.getInput.asInstanceOf[Filter]
            assertRexNode(condition, f.getCondition)
          case _ => throw new TableException("expected filter, actually is " + o.getCalciteTree)
        }
      case _ => throw new TableException("expected PlannerQueryOperation, actually is " + operation)
    }

  }

  private def assertRexNode(expected: RexNode, actual: RexNode): Unit = {
//    val simplified = FlinkRexUtil.simplify(getCluster.getRexBuilder, actual)
    if (!actual.toString.equals(expected.toString)) {
      throw new TableException(
        "\n" + actual.toString + "\n -- \n" + expected.toString + "\nare not match")
    }
  }

}
