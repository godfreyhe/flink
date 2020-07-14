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

package org.apache.flink.table.planner.plan.multipleinput;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecMultipleInputNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecMultipleInputNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.planner.plan.reuse.InputOrderCalculator;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MultipleInputNodeCreationProcessor implements DAGProcessor {

	private static final List<Class<?>> BANNED_MEMBER_CLASS = Arrays.asList(
		BatchExecTableSourceScan.class,
		BatchExecLegacyTableSourceScan.class,
		BatchExecBoundedStreamScan.class,
		BatchExecIntermediateTableScan.class,
		BatchExecUnion.class,
		BatchExecExchange.class,
		StreamExecTableSourceScan.class,
		StreamExecLegacyTableSourceScan.class,
		StreamExecDataStreamScan.class,
		StreamExecIntermediateTableScan.class,
		StreamExecUnion.class,
		StreamExecExchange.class);

	private static final List<Class<?>> ROOT_CLASS = Arrays.asList(
		BatchExecHashJoin.class,
		BatchExecSortMergeJoin.class,
		BatchExecNestedLoopJoin.class,
		StreamExecJoin.class,
		StreamExecIntervalJoin.class,
		StreamExecTemporalJoin.class);

	private final boolean isStreaming;

	public MultipleInputNodeCreationProcessor(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		InputOrderCalculator calculator = new InputOrderCalculator(
			sinkNodes,
			Collections.emptySet(),
			node ->
				node instanceof BatchExecExchange && ((BatchExecExchange) node).getShuffleMode() == ShuffleMode.BATCH,
			this::resolveConflict);
		calculator.checkConflict();

		ExecNodeWrapperProducer wrapperProducer = new ExecNodeWrapperProducer();
		List<ExecNodeWrapper> sinkWrappers = new ArrayList<>();
		sinkNodes.forEach(sinkNode -> sinkWrappers.add(wrapperProducer.visit(sinkNode)));

		List<ExecNodeWrapper> sortedWrappers = topologicalSort(sinkWrappers);
		maintainMultipleInputNodeInfo(sortedWrappers);
		createMultipleInputNodes(sortedWrappers);

		List<ExecNode<?, ?>> result = new ArrayList<>();
		for (ExecNode<?, ?> sinkNode : sinkNodes) {
			for (ExecNodeWrapper sinkWrapper : sinkWrappers) {
				if (sinkNode.equals(sinkWrapper.info.root)) {
					result.add(sinkWrapper.info.getMultipleInputNode(isStreaming));
				}
			}
		}
		return result;
	}

	private void resolveConflict(ExecNode<?, ?> node) {
		Preconditions.checkArgument(
			node instanceof BatchExecHashJoin || node instanceof BatchExecNestedLoopJoin);

		if (node instanceof BatchExecHashJoin) {
			BatchExecHashJoin hashJoin = (BatchExecHashJoin) node;
			JoinInfo joinInfo = hashJoin.getJoinInfo();
			ImmutableIntList columns = hashJoin.leftIsBuild() ? joinInfo.rightKeys : joinInfo.leftKeys;
			FlinkRelDistribution distribution = FlinkRelDistribution.hash(columns, true);
			rewriteJoin(hashJoin, hashJoin.leftIsBuild(), distribution);
		} else {
			BatchExecNestedLoopJoin nestedLoopJoin = (BatchExecNestedLoopJoin) node;
			rewriteJoin(nestedLoopJoin, nestedLoopJoin.leftIsBuild(), FlinkRelDistribution.ANY());
		}
	}

	private void rewriteJoin(BatchExecJoinBase join, boolean leftIsBuild, FlinkRelDistribution distribution) {
		int probeIndex = leftIsBuild ? 1 : 0;
		RelNode probe = join.getInput(probeIndex);

		BatchExecExchange exchange = new BatchExecExchange(
			probe.getCluster(),
			probe.getTraitSet().replace(distribution),
			probe,
			distribution,
			"multiple_input_mark");
		exchange.setRequiredShuffleMode(ShuffleMode.PIPELINED);
		join.replaceInputNode(probeIndex, exchange);
	}

	private List<ExecNodeWrapper> topologicalSort(List<ExecNodeWrapper> sinkWrappers) {
		List<ExecNodeWrapper> result = new ArrayList<>();
		Queue<ExecNodeWrapper> queue = new LinkedList<>(sinkWrappers);
		Map<ExecNodeWrapper, Integer> visitCountMap = new HashMap<>();

		while (!queue.isEmpty()) {
			ExecNodeWrapper wrapper = queue.poll();
			result.add(wrapper);
			for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
				 int visitCount = visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
				 if (visitCount == inputWrapper.outputs.size()) {
				 	queue.offer(inputWrapper);
				 }
			}
		}

		return result;
	}

	private void maintainMultipleInputNodeInfo(List<ExecNodeWrapper> sortedWrappers) {
		for (ExecNodeWrapper wrapper : sortedWrappers) {
			if (canBeMultipleInputMember(wrapper)) {
				wrapper.outputs.get(0).addMultipleInputNodeMember(wrapper);
			}
		}

		for (int i = sortedWrappers.size() - 1; i >= 0; i--) {
			ExecNodeWrapper wrapper = sortedWrappers.get(i);
			boolean differentMultipleInputNodeWithInput =
				wrapper.inputs.size() == 1 && !sameMultipleInputNode(wrapper, wrapper.inputs.get(0));
			boolean sameMultipleInputNodeWithOutput =
				wrapper.outputs.size() == 1 && sameMultipleInputNode(wrapper, wrapper.outputs.get(0));
			if (differentMultipleInputNodeWithInput && sameMultipleInputNodeWithOutput) {
				boolean inputIsExchange =
					wrapper.inputs.get(0).execNode instanceof BatchExecExchange ||
						wrapper.inputs.get(0).execNode instanceof StreamExecExchange;
				if (!inputIsExchange) {
					wrapper.outputs.get(0).removeMultipleInputNodeTail(wrapper);
				}
			}
		}
	}

	private boolean canBeMultipleInputMember(ExecNodeWrapper wrapper) {
		if (BANNED_MEMBER_CLASS.stream().anyMatch(clazz -> clazz.isInstance(wrapper.execNode))) {
			return false;
		}
		if (wrapper.outputs.isEmpty()) {
			return false;
		}

		MultipleInputNodeInfo info = wrapper.outputs.get(0).info;
		for (ExecNodeWrapper outputWrapper : wrapper.outputs) {
			if (!outputWrapper.info.canAddMember) {
				return false;
			}
			if (!info.equals(outputWrapper.info)) {
				return false;
			}
		}
		return true;
	}

	private void createMultipleInputNodes(List<ExecNodeWrapper> sortedWrappers) {
		for (int i = sortedWrappers.size() - 1; i >= 0; i--) {
			ExecNodeWrapper wrapper = sortedWrappers.get(i);
			for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
				if (!sameMultipleInputNode(inputWrapper, wrapper)) {
					wrapper.info.addInput(inputWrapper.info.getMultipleInputNode(isStreaming));
				}
			}
		}
	}

	private boolean sameMultipleInputNode(ExecNodeWrapper a, ExecNodeWrapper b) {
		return a.info.equals(b.info);
	}

	private static class ExecNodeWrapper {
		private final ExecNode<?, ?> execNode;
		private final List<ExecNodeWrapper> inputs;
		private final List<ExecNodeWrapper> outputs;

		private MultipleInputNodeInfo info;

		private ExecNodeWrapper(ExecNode<?, ?> execNode) {
			this.execNode = execNode;
			this.inputs = new ArrayList<>();
			this.outputs = new ArrayList<>();

			this.info = new MultipleInputNodeInfo(execNode);
		}

		private void addInput(ExecNodeWrapper input) {
			inputs.add(input);
		}

		private void addOutput(@Nullable ExecNodeWrapper output) {
			if (output != null) {
				outputs.add(output);
			}
		}

		private void addMultipleInputNodeMember(ExecNodeWrapper member) {
			Preconditions.checkArgument(
				info.canAddMember,
				"This MultipleInputNodeInfo cannot add member. This is a bug.");
			member.info = info;
			info.memberCount++;
		}

		private void removeMultipleInputNodeTail(ExecNodeWrapper tail) {
			Preconditions.checkArgument(
				tail.inputs.size() == 1,
				"Tail of multiple input node must have exactly 1 input. This is a bug.");

			tail.info = new MultipleInputNodeInfo(tail.execNode);
			info.memberCount--;
		}
	}

	private static class MultipleInputNodeInfo {
		private final ExecNode<?, ?> root;
		private final boolean canAddMember;
		private final List<ExecNode<?, ?>> inputs;

		private ExecNode<?, ?> multipleInputNode;
		private int memberCount;

		private MultipleInputNodeInfo(ExecNode<?, ?> root) {
			this.root = root;
			this.canAddMember = ROOT_CLASS.stream().anyMatch(clazz -> clazz.isInstance(root));
			this.inputs = new ArrayList<>();

			this.memberCount = 1;
		}

		private void addInput(ExecNode<?, ?> input) {
			inputs.add(input);
		}

		private ExecNode<?, ?> getMultipleInputNode(boolean isStreaming) {
			if (multipleInputNode != null) {
				return multipleInputNode;
			}

			if (memberCount == 1) {
				multipleInputNode = root;
				for (int i = 0; i < inputs.size(); i++) {
					ExecNode newInputNode = inputs.get(i);
					if (!root.getInputNodes().get(i).equals(newInputNode)) {
						root.replaceInputNode(i, newInputNode);
					}
				}
				return multipleInputNode;
			}

			if (isStreaming) {
				multipleInputNode = createStreamMultipleInputNode();
			} else {
				multipleInputNode = createBatchMultipleInputNode();
			}
			System.out.println(((FlinkRelNode) multipleInputNode).getRelDetailedDescription());
			return multipleInputNode;
		}

		private StreamExecMultipleInputNode createStreamMultipleInputNode() {
			RelNode outputRel = (RelNode) root;
			RelNode[] inputRels = new RelNode[inputs.size()];
			for (int i = 0; i < inputs.size(); i++) {
				inputRels[i] = (RelNode) inputs.get(i);
			}

			return new StreamExecMultipleInputNode(
				outputRel.getCluster(),
				outputRel.getTraitSet(),
				inputRels,
				outputRel);
		}

		private BatchExecMultipleInputNode createBatchMultipleInputNode() {
			InputOrderCalculator calculator = new InputOrderCalculator(
				Collections.singletonList(root),
				new HashSet<>(inputs),
				node -> false,
				node -> {
					throw new IllegalStateException("There is still conflict in multiple input node. This is a bug.");
				});
			Map<ExecNode<?, ?>, Integer> calculateResult = calculator.calculate();

			RelNode outputRel = (RelNode) root;
			RelNode[] inputRels = new RelNode[inputs.size()];
			int[] orders = new int[inputs.size()];
			for (int i = 0; i < inputs.size(); i++) {
				inputRels[i] = (RelNode) inputs.get(i);
				orders[i] = calculateResult.get(inputs.get(i));
			}

			return new BatchExecMultipleInputNode(
				outputRel.getCluster(),
				outputRel.getTraitSet(),
				inputRels,
				outputRel,
				orders);
		}
	}

	@VisibleForTesting
	static class ExecNodeWrapperProducer {
		private final Map<ExecNode<?, ?>, ExecNodeWrapper> wrapperMap;

		ExecNodeWrapperProducer() {
			this.wrapperMap = new HashMap<>();
		}

		ExecNodeWrapper visit(ExecNode<?, ?> node) {
			return visit(node, null);
		}

		ExecNodeWrapper visit(ExecNode<?, ?> node, @Nullable ExecNodeWrapper parent) {
			ExecNodeWrapper wrapper = wrapperMap.get(node);
			if (wrapper != null) {
				wrapper.addOutput(parent);
				return wrapper;
			}

			wrapper = new ExecNodeWrapper(node);
			wrapper.addOutput(parent);
			wrapperMap.put(node, wrapper);
			for (ExecNode<?, ?> input : node.getInputNodes()) {
				wrapper.addInput(visit(input, wrapper));
			}
			return wrapper;
		}
	}
}
