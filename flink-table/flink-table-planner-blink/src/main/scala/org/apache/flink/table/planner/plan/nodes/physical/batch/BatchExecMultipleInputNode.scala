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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

class BatchExecMultipleInputNode[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode, // the root node of the sub-tree (from root node to input nodes)
    readOrder: Array[Int])
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel)
  with BatchExecNode[_] {

  override def getDamBehavior: DamBehavior = {
    val inputNodes = inputRels.map(_.asInstanceOf[BatchExecNode[_]])
    if (inputNodes.forall(n => n.getDamBehavior == DamBehavior.PIPELINED)) {
      DamBehavior.PIPELINED
    } else if (inputNodes.exists(n => n.getDamBehavior == DamBehavior.FULL_DAM)) {
      DamBehavior.FULL_DAM
    } else {
      DamBehavior.MATERIALIZING
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[BatchPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def translateToPlanInternal(planner: BatchPlanner): Transformation[_] = ???

}
