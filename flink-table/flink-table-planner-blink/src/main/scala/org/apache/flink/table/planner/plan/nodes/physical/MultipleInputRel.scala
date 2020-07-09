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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{AbstractRelNode, RelNode, RelWriter}
import java.util

import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import scala.collection.JavaConversions._

/**
 * Base class for flink multiple input relational expression.
 * TODO this is a temporary solution.
 */
class MultipleInputRel(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode)
  extends AbstractRelNode(cluster, traitSet)
  with FlinkPhysicalRel {

  override def getInputs: util.List[RelNode] = inputRels.toList

  override def deriveRowType(): RelDataType = outputRel.getRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    inputRels.zipWithIndex.map {
      case (rel, index) =>
        pw.input("input" + index, rel)
    }
    pw.item("output", outputRel)
    pw.item("members", FlinkRelOptUtil.toString(outputRel, borders = inputRels).replace("\n", "\\n"))
  }
}
