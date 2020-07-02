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
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	protected boolean canBeMerged(ExecNodeInfo nodeInfo) {
		if (BANNED_NODES.stream().anyMatch(clazz -> clazz.isInstance(nodeInfo.execNode))) {
			return false;
		}

		for (ExecNodeInfo outputInfo : nodeInfo.outputs) {
			boolean isProbe = false;
			if (outputInfo.execNode instanceof BatchExecHashJoin) {
				BatchExecHashJoin hashJoin = (BatchExecHashJoin) outputInfo.execNode;
				isProbe = (hashJoin.leftIsBuild() && hashJoin.getInputNodes().get(1) == nodeInfo.execNode) ||
					(!hashJoin.leftIsBuild() && hashJoin.getInputNodes().get(0) == nodeInfo.execNode);
			} else if (outputInfo.execNode instanceof BatchExecNestedLoopJoin) {
				BatchExecNestedLoopJoin nestedLoopJoin = (BatchExecNestedLoopJoin) outputInfo.execNode;
				isProbe = (nestedLoopJoin.leftIsBuild() && nestedLoopJoin.getInputNodes().get(1) == nodeInfo.execNode) ||
					(!nestedLoopJoin.leftIsBuild() && nestedLoopJoin.getInputNodes().get(0) == nodeInfo.execNode);
			}
			if (isProbe && hasSamePredecessor(
				outputInfo.execNode.getInputNodes().get(0), outputInfo.execNode.getInputNodes().get(1))) {
				return false;
			}
		}

		return true;
	}

	private boolean hasSamePredecessor(ExecNode<?, ?> a, ExecNode<?, ?> b) {
		// TODO optimize this
		Set<ExecNode> sa = new HashSet<>();
		dfs(a, sa);
		Set<ExecNode> sb = new HashSet<>();
		dfs(b, sb);

		for (ExecNode n : sa) {
			if (sb.contains(n)) {
				return true;
			}
		}
		return false;
	}

	private void dfs(ExecNode<?, ?> node, Set<ExecNode> visited) {
		if (visited.contains(node)) {
			return;
		}

		visited.add(node);
		node.getInputNodes().forEach(n -> dfs(n, visited));
	}

	@Override
	protected boolean canBeRoot(ExecNodeInfo nodeInfo) {
		return ROOT_NODES.stream().anyMatch(clazz -> clazz.isInstance(nodeInfo.execNode));
	}

	@Override
	protected ExecNode<?, ?> buildMultipleInputNode(ExecNode<?, ?> execNode, List<ExecNode> inputs) {
		RelNode rel = (RelNode) execNode;
		int[] readOrder = calculateReadOrder(execNode, inputs);
		return new BatchExecMultipleInputNode(
			rel.getCluster(),
			rel.getTraitSet(),
			inputs.toArray(new RelNode[0]),
			rel,
			readOrder);
	}

	private int[] calculateReadOrder(ExecNode<?, ?> execNode, List<ExecNode> inputs) {
		Set<ExecNode> inputSets = new HashSet<>(inputs);
		Map<ExecNode, String> orderStrings = new HashMap<>();

		dfs(execNode, "", inputSets, orderStrings, new HashMap<>());
		List<String> orders = new ArrayList<>(orderStrings.values());
		Collections.sort(orders);
		Map<String, Integer> string2Int = new HashMap<>();
		for (int i = 0; i < orders.size(); i++) {
			string2Int.put(orders.get(i), i);
		}

		int[] readOrder = new int[inputs.size()];
		for (int i = 0; i < inputs.size(); i++) {
			readOrder[i] = string2Int.get(orderStrings.get(inputs.get(i)));
		}
		return readOrder;
	}

	private void dfs(
			ExecNode<?, ?> execNode,
			String s, Set<ExecNode> inputSets,
			Map<ExecNode, String> orderStrings,
			Map<ExecNode, String> visited) {
		if (visited.containsKey(execNode)) {
			String order = visited.get(execNode);
			Preconditions.checkState(
				order.equals(s),
				"The same exec node has two different orders. This is a bug.");
			return;
		}

		visited.put(execNode, s);
		if (inputSets.contains(execNode)) {
			orderStrings.put(execNode, s);
			return;
		}

		if (execNode instanceof BatchExecHashJoin) {
			BatchExecHashJoin hashJoin = (BatchExecHashJoin) execNode;
			int buildIdx = hashJoin.leftIsBuild() ? 0 : 1;
			int probeIdx = 1 - buildIdx;
			dfs(hashJoin.getInputNodes().get(buildIdx), s + "b", inputSets, orderStrings, visited);
			dfs(hashJoin.getInputNodes().get(probeIdx), s + "p", inputSets, orderStrings, visited);
		} else if (execNode instanceof BatchExecNestedLoopJoin) {
			BatchExecNestedLoopJoin nestedLoopJoin = (BatchExecNestedLoopJoin) execNode;
			int buildIdx = nestedLoopJoin.leftIsBuild() ? 0 : 1;
			int probeIdx = 1 - buildIdx;
			dfs(nestedLoopJoin.getInputNodes().get(buildIdx), s + "b", inputSets, orderStrings, visited);
			dfs(nestedLoopJoin.getInputNodes().get(probeIdx), s + "p", inputSets, orderStrings, visited);
		} else {
			execNode.getInputNodes().forEach(n -> dfs(n, s + "b", inputSets, orderStrings, visited));
		}
	}
}
