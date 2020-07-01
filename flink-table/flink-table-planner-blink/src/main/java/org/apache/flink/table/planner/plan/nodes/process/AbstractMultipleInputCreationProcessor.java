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

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeVisitorImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMultipleInputCreationProcessor implements DAGProcessor {

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		TopologicalGraph graph = new TopologicalGraph();
		sinkNodes.forEach(graph::visit);
		List<ExecNodeInfo> order = graph.getTopologicalOrder();

		mark(order);
		return createMultipleInputNode(order);
	}

	abstract protected boolean canBeMerged(ExecNodeInfo nodeInfo);

	abstract protected boolean canBeRoot(ExecNodeInfo nodeInfo);

	abstract protected ExecNode<?, ?> buildMultipleInputNode(ExecNode<?, ?> execNode, List<ExecNode> inputs);

	private void mark(List<ExecNodeInfo> order) {
		for (ExecNodeInfo nodeInfo : order) {
			Set<MultipleInputNodeInfo> outputMinis = nodeInfo.outputs.stream()
				.map(o -> o.mini)
				.collect(Collectors.toSet());
			if (outputMinis.size() == 1 && outputMinis.iterator().next() != null) {
				MultipleInputNodeInfo mini = outputMinis.iterator().next();
				if (canBeMerged(nodeInfo)) {
					// this node can be merged
					mini.size++;
					nodeInfo.mini = mini;
				}
			} else {
				// there are different multiple input nodes in the output or there is no multiple input node,
				// so this node cannot be merged into existing multiple input exec nodes
				if (canBeRoot(nodeInfo)) {
					// we can start building a new multiple input exec node from this node
					nodeInfo.mini = new MultipleInputNodeInfo(nodeInfo.execNode);
				}
			}
		}

		for (ExecNodeInfo nodeInfo : order) {
			if (nodeInfo.mini != null && nodeInfo.mini.size == 1) {
				// this multiple input exec node only has 1 member node,
				// so it does not need to be changed into a multiple input exec node
				nodeInfo.mini = null;
			}
		}
	}

	private List<ExecNode<?, ?>> createMultipleInputNode(List<ExecNodeInfo> order) {
		List<ExecNode<?, ?>> sinks = new ArrayList<>();

		for (int i = order.size() - 1; i >= 0; i--) {
			ExecNodeInfo nodeInfo = order.get(i);
			if (nodeInfo.mini != null && nodeInfo.mini.root == nodeInfo.execNode) {
				// this is the root of a multiple input exec node, so build it
				ExecNode<?, ?> multipleInputNode = buildMultipleInputNode(nodeInfo.execNode, nodeInfo.mini.inputs);
				reconnectToOutputs(nodeInfo, multipleInputNode, sinks, true);
			} else if (nodeInfo.mini == null) {
				// this is not a member of a multiple input exec node
				reconnectToOutputs(nodeInfo, nodeInfo.execNode, sinks, false);
			}
		}

		return sinks;
	}

	private void reconnectToOutputs(
			ExecNodeInfo nodeInfo,
			ExecNode<?, ?> input,
			List<ExecNode<?, ?>> sinks,
			boolean needToReplaceInput) {
		if (nodeInfo.outputs.isEmpty()) {
			sinks.add(input);
		} else {
			for (ExecNodeInfo outputInfo : nodeInfo.outputs) {
				if (outputInfo.mini != null) {
					outputInfo.mini.inputs.add(input);
				} else if (needToReplaceInput) {
					replaceInputNode(outputInfo.execNode, nodeInfo.execNode, input);
				}
			}
		}
	}

	private void replaceInputNode(ExecNode<?, ?> output, ExecNode<?, ?> inputBefore, ExecNode inputAfter) {
		for (int i = 0; i < output.getInputNodes().size(); i++) {
			if (output.getInputNodes().get(i) == inputBefore) {
				output.replaceInputNode(i, inputAfter);
			}
		}
	}

	/**
	 * A helper class which calculates the topological order of the exec node graph.
	 */
	private static class TopologicalGraph extends ExecNodeVisitorImpl {

		private Map<ExecNode, ExecNodeInfo> execNodeInfos;
		private Set<ExecNode> visited;

		private TopologicalGraph() {
			this.execNodeInfos = new HashMap<>();
			this.visited = new HashSet<>();
		}

		@Override
		public void visit(ExecNode<?, ?> node) {
			if (visited.contains(node)) {
				return;
			}
			visited.add(node);

			ExecNodeInfo nodeInfo = execNodeInfos.computeIfAbsent(node, ignore -> new ExecNodeInfo(node));
			node.getInputNodes().forEach(
				n -> execNodeInfos.computeIfAbsent(n, ignore -> new ExecNodeInfo(n)).outputs.add(nodeInfo));
			super.visit(node);
		}

		private List<ExecNodeInfo> getTopologicalOrder() {
			List<ExecNodeInfo> res = new ArrayList<>();
			Queue<ExecNode> queue = new LinkedList<>();

			Map<ExecNode, Integer> degs = new HashMap<>();
			execNodeInfos.forEach((k, v) -> {
				degs.put(k, v.outputs.size());
				if (v.outputs.size() == 0) {
					queue.offer(k);
				}
			});

			while (!queue.isEmpty()) {
				ExecNode<?, ?> node = queue.poll();
				res.add(execNodeInfos.get(node));
				node.getInputNodes().forEach(n -> {
					if (degs.compute(n, (k, v) -> v - 1) == 0) {
						queue.offer(n);
					}
				});
			}

			return res;
		}
	}

	/**
	 * A helper class recording some extra information of a {@link ExecNode}.
	 */
	protected static class ExecNodeInfo {

		protected ExecNode<?, ?> execNode;
		protected List<ExecNodeInfo> outputs;
		protected MultipleInputNodeInfo mini;

		protected ExecNodeInfo(ExecNode<?, ?> execNode) {
			this.execNode = execNode;
			this.outputs = new ArrayList<>();
			this.mini = null;
		}
	}

	/**
	 * A helper class recording the information needed for creating a multiple input exec node.
	 */
	protected static class MultipleInputNodeInfo {

		protected ExecNode<?, ?> root;
		protected int size;
		protected List<ExecNode> inputs;

		protected MultipleInputNodeInfo(ExecNode<?, ?> root) {
			this.root = root;
			this.size = 1;
			this.inputs = new ArrayList<>();
		}
	}
}
