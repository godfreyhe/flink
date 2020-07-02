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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import java.util

import org.apache.flink.configuration.MemorySize
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.operators.multipleinput.{BatchMultipleInputOperatorFactory, StreamingMultipleInputOperatorFactory, TransformationConverter}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import scala.collection.JavaConversions._

class BatchExecMultipleInputNode(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode, // the root node of the sub-tree (from root node to input nodes)
    readOrder: Array[Int])
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel)
  with BatchExecNode[RowData] {

  override def getDamBehavior: DamBehavior = {
    // TODO this is not correct
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

  override protected def translateToPlanInternal(planner: BatchPlanner): Transformation[RowData] = {
    val inputTransforms = getInputNodes.map(n => n.translateToPlan(planner))
    val tailTransform = outputRel.asInstanceOf[BatchExecNode[_]].translateToPlan(planner)

    val outputType = RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val converter = new TransformationConverter(inputTransforms, tailTransform, readOrder)
    val tailOpAndHeadOps = converter.convert()

    val multipleTransform = new MultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new BatchMultipleInputOperatorFactory(
        inputTransforms.size,
        tailOpAndHeadOps.f1,
        tailOpAndHeadOps.f0),
      outputType,
      tailTransform.getParallelism)

    // add inputs
    inputTransforms.foreach(input => multipleTransform.addInput(input))

    if (tailTransform.getMaxParallelism > 0) {
      multipleTransform.setMaxParallelism(tailTransform.getMaxParallelism)
    }

    val minResources = converter.getMinResources
    val preferredResources = converter.getPreferredResources
    multipleTransform.setResources(minResources, preferredResources)
    ExecNode.setManagedMemoryWeight(multipleTransform, MemorySize.parseBytes("128mb"))

    multipleTransform
  }

}
