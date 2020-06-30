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

	abstract protected boolean canBeMerged(ExecNode<?, ?> execNode);

	abstract protected boolean canBeRoot(ExecNode<?, ?> execNode);

	abstract protected ExecNode<?, ?> buildMultipleInputNode(
		RelOptCluster cluster, RelTraitSet traitSet, RelNode[] inputs, RelNode output);

	private void mark(List<ExecNodeInfo> order) {
		for (ExecNodeInfo node : order) {
			Set<MultipleInputNodeInfo> outputMinis = node.outputs.stream()
				.map(o -> o.mini)
				.collect(Collectors.toSet());
			if (outputMinis.size() == 1 && outputMinis.iterator().next() != null) {
				MultipleInputNodeInfo mini = outputMinis.iterator().next();
				if (canBeMerged(node.execNode)) {
					// this node can be merged
					mini.size++;
					node.mini = mini;
				}
			} else {
				// there are different multiple input nodes in the output or there is no multiple input node,
				// so this node cannot be merged into existing multiple input exec nodes
				if (canBeRoot(node.execNode)) {
					// we can start building a new multiple input exec node from this node
					node.mini = new MultipleInputNodeInfo(node.execNode);
				}
			}
		}
	}

	private List<ExecNode<?, ?>> createMultipleInputNode(List<ExecNodeInfo> order) {
		List<ExecNode<?, ?>> res = new ArrayList<>();

		for (int i = order.size() - 1; i >= 0; i--) {
			ExecNodeInfo node = order.get(i);

			if (node.mini != null && node.mini.size > 1 && node.mini.root == node.execNode) {
				RelNode rel = (RelNode) node.execNode;
				ExecNode<?, ?> multipleInputNode = buildMultipleInputNode(
					rel.getCluster(), rel.getTraitSet(), node.mini.inputs.toArray(new RelNode[0]), rel);
				if (node.outputs.isEmpty()) {
					res.add(multipleInputNode);
				} else {
					for (ExecNodeInfo output : node.outputs) {
						if (output.mini != null) {
							output.mini.inputs.add(multipleInputNode);
						} else {
							replaceInputNode(output.execNode, node.execNode, multipleInputNode);
						}
					}
				}
			} else if (node.mini == null || node.mini.size == 1) {
				if (node.outputs.isEmpty()) {
					res.add(node.execNode);
				} else {
					for (ExecNodeInfo output : node.outputs) {
						if (output.mini	!= null) {
							output.mini.inputs.add(node.execNode);
						}
					}
				}
			}
		}

		return res;
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

			ExecNodeInfo info = execNodeInfos.computeIfAbsent(node, ignore -> new ExecNodeInfo(node));
			node.getInputNodes().forEach(
				n -> execNodeInfos.computeIfAbsent(n, ignore -> new ExecNodeInfo(n)).outputs.add(info));
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
	private static class ExecNodeInfo {

		private ExecNode<?, ?> execNode;
		private List<ExecNodeInfo> outputs;
		private MultipleInputNodeInfo mini;

		private ExecNodeInfo(ExecNode<?, ?> execNode) {
			this.execNode = execNode;
			this.outputs = new ArrayList<>();
			this.mini = null;
		}
	}

	/**
	 * A helper class recording the information needed for creating a multiple input exec node.
	 */
	private static class MultipleInputNodeInfo {

		private ExecNode<?, ?> root;
		private int size;
		private List<ExecNode> inputs;

		private MultipleInputNodeInfo(ExecNode<?, ?> root) {
			this.root = root;
			this.size = 1;
			this.inputs = new ArrayList<>();
		}
	}
}
