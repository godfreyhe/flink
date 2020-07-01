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
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.transformations.{KeyedMultipleInputTransformation, OneInputTransformation, TwoInputTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{StreamingMultipleInputOperatorFactory, TransformationConverter}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

class StreamExecMultipleInputNode(
    cluster: RelOptCluster,
    traitSet: RelTraitSet, // TODO trait ?
    inputRels: Array[RelNode],
    outputRel: RelNode) // the root node of the sub-tree (from root node to input nodes)
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel)
  with StreamExecNode[RowData] {

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
    // TODO handle union node
    val inputTransforms = getInputNodes.map(n => n.translateToPlan(planner))
    val tailTransform = outputRel.asInstanceOf[StreamExecNode[_]].translateToPlan(planner)

    val outputType = RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val converter = new TransformationConverter(inputTransforms, tailTransform)
    val tailOpAndHeadOps = converter.convert()

    val headTransforms = new mutable.ListBuffer[Transformation[_]]()
    val keySelectorMap = new util.HashMap[Transformation[_], KeySelector[_, _]]()
    visit(inputTransforms.toList, tailTransform, keySelectorMap, headTransforms)

    val multipleTransform = new KeyedMultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new StreamingMultipleInputOperatorFactory(
        inputTransforms.size,
        tailOpAndHeadOps.f1,
        tailOpAndHeadOps.f0),
      outputType,
      tailTransform.getParallelism, // TODO
      headTransforms.head.asInstanceOf[OneInputTransformation[_, _]].getStateKeyType // TODO
    )

    // add inputs
    inputTransforms.foreach(input => multipleTransform.addInput(input, keySelectorMap.get(input)))

    if (tailTransform.getMaxParallelism > 0) {
      multipleTransform.setMaxParallelism(tailTransform.getMaxParallelism)
    }

    // set resource
    // TODO

    multipleTransform
  }

  private def visit(
      inputTransforms: List[Transformation[_]],
      transform: Transformation[_],
      keySelectorMap: util.Map[Transformation[_], KeySelector[_, _]],
      headTransforms: mutable.ListBuffer[Transformation[_]]): Unit = {

    transform match {
      case t: OneInputTransformation[_, _] =>
        if (isMultipleOperatorInput(inputTransforms, t.getInput)) {
          keySelectorMap.put(t.getInput, t.getStateKeySelector)
          headTransforms.add(t)
        } else {
          visit(inputTransforms, t.getInput, keySelectorMap, headTransforms)
        }
      case t: TwoInputTransformation[_, _, _] =>
        val isMultipleOperatorInput1 = isMultipleOperatorInput(inputTransforms, t.getInput1)
        val isMultipleOperatorInput2 = isMultipleOperatorInput(inputTransforms, t.getInput2)
        if (isMultipleOperatorInput1 && isMultipleOperatorInput2) {
          keySelectorMap.put(t.getInput1, t.getStateKeySelector1)
          keySelectorMap.put(t.getInput2, t.getStateKeySelector2)
          headTransforms.add(t)
        } else if (isMultipleOperatorInput1) {
          visit(inputTransforms, t.getInput2, keySelectorMap, headTransforms)
          keySelectorMap.put(t.getInput1, t.getStateKeySelector1)
          headTransforms.add(t)
        } else if (isMultipleOperatorInput2) {
          visit(inputTransforms, t.getInput1, keySelectorMap, headTransforms)
          keySelectorMap.put(t.getInput2, t.getStateKeySelector2)
          headTransforms.add(t)
        } else {
          visit(inputTransforms, t.getInput2, keySelectorMap, headTransforms)
          visit(inputTransforms, t.getInput1, keySelectorMap, headTransforms)
        }
      case _ =>
        throw new TableException("Unsupported Transformation: " + transform)
    }
  }

  private def isMultipleOperatorInput(
      inputTransforms: List[Transformation[_]],
      transform: Transformation[_]) = {
    inputTransforms.contains(transform)
  }

}
