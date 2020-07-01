/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.process;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecMultipleInputNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecUnion;

import java.util.Arrays;
import java.util.List;

public class BatchMultipleInputCreationProcessor extends AbstractMultipleInputCreationProcessor {

	private static List<Class> BANNED_NODES = Arrays.asList(
		BatchExecTableSourceScan.class,
		BatchExecLegacyTableSourceScan.class,
		BatchExecExchange.class,
		BatchExecUnion.class);

	private static List<Class> ROOT_NODES = Arrays.asList(
		BatchExecHashJoin.class,
		BatchExecNestedLoopJoin.class,
		BatchExecSortMergeJoin.class);

	@Override
	protected boolean canBeMerged(ExecNode<?, ?> execNode) {
		return BANNED_NODES.stream().noneMatch(clazz -> clazz.isInstance(execNode));
	}

	@Override
	protected boolean canBeRoot(ExecNode<?, ?> execNode) {
		return ROOT_NODES.stream().anyMatch(clazz -> clazz.isInstance(execNode));
	}

	@Override
	protected ExecNode<?, ?> buildMultipleInputNode(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode[] inputs,
			RelNode output) {
		int[] readOrder = new int[inputs.length];
		return new BatchExecMultipleInputNode(cluster, traitSet, inputs, output, readOrder);
	}
}
