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
import org.apache.flink.configuration.MemorySize
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{BatchMultipleInputStreamOperatorFactory, StreamOperatorWrapperGenerator}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * Batch physical RelNode for multiple input rel which contains a sub-graph.
 * The root node of the sub-graph is [[outputRel]], and the leaf nodes of the sub-graph are
 * the output nodes of the [[inputRels]], which means the multiple input rel does not
 * contain [[inputRels]].
 *
 * @param inputRels the input rels of multiple input rel,
 *                  which are not a part of the multiple input rel.
 * @param outputRel the root rel of the sub-graph of the multiple input rel.
 * @param readOrders the read order corresponding each input. The values is lower,
 *                   the read priority is higher. Note that, if a input has multiple output,
 *                   their read order should be same. so each read order corresponds to an input.
 */
class BatchExecMultipleInputNode(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode,
    readOrders: Array[Int])
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel)
  with BatchExecNode[RowData] {

  override def getDamBehavior: DamBehavior = {
    throw new UnsupportedOperationException()
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

    val generator = new StreamOperatorWrapperGenerator(inputTransforms, tailTransform, readOrders)
    generator.generate()

    val multipleInputTransform = new MultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new BatchMultipleInputStreamOperatorFactory(
        generator.getInputSpecs,
        generator.getHeadOperatorWrappers,
        generator.getTailOperatorWrapper),
      outputType,
      generator.getParallelism)

    // add inputs as the order of input specs
    generator.getOrderedInputTransforms.foreach(input => multipleInputTransform.addInput(input))

    if (generator.getMaxParallelism > 0) {
      multipleInputTransform.setMaxParallelism(generator.getMaxParallelism)
    }

    // set resources
    multipleInputTransform.setResources(generator.getMinResources, generator.getPreferredResources)
    // TODO
//    val memoryKB = generator.getManagedMemoryWeight
//    ExecNode.setManagedMemoryWeight(multipleInputTransform, memoryKB * 1024)
    ExecNode.setManagedMemoryWeight(multipleInputTransform, MemorySize.parseBytes("128m"))

    multipleInputTransform
  }

}
