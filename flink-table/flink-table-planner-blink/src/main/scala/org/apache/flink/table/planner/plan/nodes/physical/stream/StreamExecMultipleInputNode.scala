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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{StreamMultipleInputStreamOperatorFactory, StreamOperatorNodeGenerator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for multiple input rel.
 */
class StreamExecMultipleInputNode(
    cluster: RelOptCluster,
    traitSet: RelTraitSet, // TODO trait ?
    inputRels: Array[RelNode],
    outputRel: RelNode) // the root node of the sub-tree (from output node to input nodes)
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel)
  with StreamExecNode[RowData]
  with StreamPhysicalRel {

  def getOutputRel: RelNode = outputRel

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransforms = getInputNodes.map(n => n.translateToPlan(planner))
    val tailTransform = outputRel.asInstanceOf[StreamExecNode[_]].translateToPlan(planner)

    val outputType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val generator = new StreamOperatorNodeGenerator(inputTransforms, tailTransform)
    generator.generate()

    val multipleInputTransform = new KeyedMultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new StreamMultipleInputStreamOperatorFactory(
        generator.getInputSpecs,
        generator.getHeadNodes,
        generator.getTailNode),
      outputType,
      generator.getParallelism,
      Preconditions.checkNotNull(generator.getStateKeyType)
    )

    // add inputs as the order of input specs
    generator.getOrderedInputTransforms.zip(generator.getOrderedKeySelectors).foreach {
      case (input, keySelector) => multipleInputTransform.addInput(input, keySelector)
    }

    if (generator.getMaxParallelism > 0) {
      multipleInputTransform.setMaxParallelism(generator.getMaxParallelism)
    }

    // set resources
    multipleInputTransform.setResources(generator.getMinResources, generator.getPreferredResources)
    val memoryKB = generator.getManagedMemoryWeight
    ExecNode.setManagedMemoryWeight(multipleInputTransform, memoryKB * 1024)

    multipleInputTransform
  }

  /**
   * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
   */
  override def requireWatermark: Boolean = {
    val visited = new java.util.HashSet[RelNode]()
    var ret = false

    def dfs(node: RelNode): Unit = {
      if (visited.contains(node)) {
        return
      }

      visited.add(node)
      node match {
        case rel: StreamPhysicalRel =>
          ret = ret || rel.requireWatermark
        case _ =>
      }

      val it = visited.iterator()
      while (it.hasNext) {
        dfs(it.next())
      }
    }

    dfs(outputRel)
    ret
  }
}
