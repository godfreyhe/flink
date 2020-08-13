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
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{BatchMultipleInputStreamOperatorFactory, StreamOperatorNodeGenerator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

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

  def getOutputRel: RelNode = outputRel

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

    val outputType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val generator = new StreamOperatorNodeGenerator(inputTransforms, tailTransform, readOrders)
    generator.generate()

    val multipleInputTransform = new MultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new BatchMultipleInputStreamOperatorFactory(
        generator.getInputSpecs,
        generator.getHeadNodes,
        generator.getTailNode),
      outputType,
      getParallelism(generator.getParallelism, planner.getTableConfig))

    // add inputs as the order of input specs
    generator.getOrderedInputTransforms.foreach(input => multipleInputTransform.addInput(input))

    if (generator.getMaxParallelism > 0) {
      multipleInputTransform.setMaxParallelism(generator.getMaxParallelism)
    }

    // set resources
    multipleInputTransform.setResources(generator.getMinResources, generator.getPreferredResources)
    val memoryKB = generator.getManagedMemoryWeight
    val memoryFactor = planner.getTableConfig.getConfiguration.getDouble(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_MULTIPLE_INPUT_MEMORY_FACTOR)
    ExecNode.setManagedMemoryWeight(multipleInputTransform, (memoryKB * memoryFactor * 1024).toInt)

    multipleInputTransform
  }

  private def getParallelism(parallelism: Int, tableConfig: TableConfig): Int = {
    if (parallelism == 1) {
      return parallelism
    }
    val p = if (parallelism > 0) {
      parallelism
    } else {
      tableConfig.getConfiguration.getInteger(
        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM)
    }
    val numOfOperators = getNumberOfOperators(outputRel)
    val factor = tableConfig.getConfiguration.getDouble(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_MULTIPLE_INPUT_PARALLELISM_FACTOR)
    var r = p * numOfOperators * factor
    val rLim = tableConfig.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_MULTIPLE_INPUT_PARALLELISM_MAX)
    if (rLim > 0) {
      r = scala.math.min(r.toInt, rLim)
    }
    if (r > 0) r.toInt else -1
  }

  private def getNumberOfOperators(rel: RelNode): Int = {
    if (inputRels.contains(rel)) {
      return 0
    }
   val num = rel match  {
      case _: BatchExecGroupAggregateBase | _: BatchExecJoinBase => 1
      case _ => 0
    }
    rel.getInputs.map(getNumberOfOperators).sum + num
  }

}
