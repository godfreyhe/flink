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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecMultipleInputNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecUnion;

import java.util.Arrays;
import java.util.List;

public class StreamMultipleInputCreationProcessor extends AbstractMultipleInputCreationProcessor {

	private static List<Class> BANNED_NODES = Arrays.asList(
		StreamExecTableSourceScan.class,
		StreamExecLegacyTableSourceScan.class,
		StreamExecExchange.class,
		StreamExecUnion.class);

	private static List<Class> ROOT_NODES = Arrays.asList(
		StreamExecJoin.class,
		StreamExecIntervalJoin.class,
		StreamExecTemporalJoin.class);

	@Override
	protected boolean canBeMerged(ExecNode execNode) {
		return BANNED_NODES.stream().noneMatch(clazz -> clazz.isInstance(execNode));
	}

	@Override
	protected boolean canBeRoot(ExecNode execNode) {
		return ROOT_NODES.stream().anyMatch(clazz -> clazz.isInstance(execNode));
	}

	@Override
	protected ExecNode<?, ?> buildMultipleInputNode(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode[] inputs,
			RelNode output) {
		return new StreamExecMultipleInputNode(cluster, traitSet, inputs, output);
	}
}
