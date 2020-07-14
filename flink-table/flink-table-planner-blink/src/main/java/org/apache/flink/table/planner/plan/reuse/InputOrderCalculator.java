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

package org.apache.flink.table.planner.plan.reuse;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecNestedLoopJoin;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class InputOrderCalculator {

	private final List<ExecNode<?, ?>> sinks;
	private final Set<ExecNode<?, ?>> boundaries;

	private final Function<ExecNode<?, ?>, Boolean> isSafeNode;
	private final Consumer<ExecNode<?, ?>> conflictResolver;

	private final TopologyGraph topologyGraph;

	public InputOrderCalculator(
			List<ExecNode<?, ?>> sinks,
			Set<ExecNode<?, ?>> boundaries,
			Function<ExecNode<?, ?>, Boolean> isSafeNode,
			Consumer<ExecNode<?, ?>> conflictResolver) {
		this.sinks = sinks;
		this.boundaries = boundaries;

		this.isSafeNode = isSafeNode;
		this.conflictResolver = conflictResolver;

		this.topologyGraph = new TopologyGraph();
	}

	public Map<ExecNode<?, ?>, Integer> calculate() {
		checkConflict();
		Map<ExecNode<?, ?>, Integer> orders = topologyGraph.calculateOrder();

		Set<Integer> boundaryOrderSet = new HashSet<>();
		for (ExecNode<?, ?> boundary : boundaries) {
			boundaryOrderSet.add(orders.getOrDefault(boundary, 0));
		}
		List<Integer> boundaryOrderList = new ArrayList<>(boundaryOrderSet);
		Collections.sort(boundaryOrderList);

		Map<ExecNode<?, ?>, Integer> results = new HashMap<>();
		for (ExecNode<?, ?> boundary : boundaries) {
			results.put(boundary, boundaryOrderList.indexOf(orders.get(boundary)));
		}
		return results;
	}

	public void checkConflict() {
		ExecNodeGraphVisitor visitor = new ExecNodeGraphVisitor(
			node -> {
				if (node instanceof BatchExecHashJoin || node instanceof BatchExecNestedLoopJoin) {
					processJoin(node);
				}
			},
			(output, input) -> !boundaries.contains(input)
		);
		for (ExecNode<?, ?> sink : sinks) {
			visitor.visit(sink);
		}
	}

	private void processJoin(ExecNode<?, ?> join) {
		Preconditions.checkArgument(
			join instanceof BatchExecHashJoin || join instanceof BatchExecNestedLoopJoin);

		ExecNode<?, ?> buildNode = getJoinChild(join, true);
		Preconditions.checkState(
			topologyGraph.link(buildNode, join),
			"Conflict occurs when specifying order between a join and its build side. This is a bug.");
		ExecNodeGraphVisitor buildVisitor = new ExecNodeGraphVisitor(
			node -> {}, // nothing to process
			(output, input) -> !boundaries.contains(output) // we can visit all children of the build side
		);
		List<ExecNode<?, ?>> buildInputs = buildVisitor.visit(buildNode);

		ExecNode<?, ?> probeNode = getJoinChild(join, false);
		Preconditions.checkState(
			topologyGraph.link(probeNode, join),
			"Conflict occurs when specifying order between a join and its probe side. This is a bug.");
		ExecNodeGraphVisitor probeVisitor = new ExecNodeGraphVisitor(
			node -> {}, // nothing to process
			(output, input) -> {
				if (boundaries.contains(output)) {
					return false;
				} else if (output instanceof BatchExecHashJoin || output instanceof BatchExecNestedLoopJoin) {
					// we only visit probe side for joins
					return input.equals(getJoinChild(output, false));
				} else {
					// we stop at safe nodes
					return !isSafeNode.apply(output);
				}
			}
		);
		List<ExecNode<?, ?>> probeInputs = probeVisitor.visit(probeNode);

		List<Tuple2<ExecNode<?, ?>, ExecNode<?, ?>>> linkedEdges = new ArrayList<>();
		boolean failed = false;
		OUTER_LOOP:
		for (ExecNode<?, ?> buildInput : buildInputs) {
			for (ExecNode<?, ?> probeInput : probeInputs) {
				if (topologyGraph.link(buildInput, probeInput)) {
					linkedEdges.add(Tuple2.of(buildInput, probeInput));
				} else {
					failed = true;
					break OUTER_LOOP;
				}
			}
		}
		if (failed) {
			for (Tuple2<ExecNode<?, ?>, ExecNode<?, ?>> edge : linkedEdges) {
				topologyGraph.unlink(edge.f0, edge.f1);
			}
			conflictResolver.accept(join);
		}
	}

	private ExecNode<?, ?> getJoinChild(ExecNode<?, ?> join, boolean getBuild) {
		Preconditions.checkArgument(
			join instanceof BatchExecHashJoin || join instanceof BatchExecNestedLoopJoin);

		int buildIndex;
		if (join instanceof BatchExecHashJoin) {
			buildIndex =  ((BatchExecHashJoin) join).leftIsBuild() ? 0 : 1;
		} else {
			buildIndex =  ((BatchExecNestedLoopJoin) join).leftIsBuild() ? 0 : 1;
		}

		return getBuild ? join.getInputNodes().get(buildIndex) : join.getInputNodes().get(1 - buildIndex);
	}

	private static class ExecNodeGraphVisitor {
		private final Consumer<ExecNode<?, ?>> processor;
		private final BiFunction<ExecNode<?, ?>, ExecNode<?, ?>, Boolean> shouldVisit;

		private final Set<ExecNode<?, ?>> visited;
		private final List<ExecNode<?, ?>> results;

		private ExecNodeGraphVisitor(
				Consumer<ExecNode<?, ?>> processor,
				BiFunction<ExecNode<?, ?>, ExecNode<?, ?>, Boolean> shouldVisit) {
			this.processor = processor;
			this.shouldVisit = shouldVisit;

			this.visited = new HashSet<>();
			this.results = new ArrayList<>();
		}

		private List<ExecNode<?, ?>> visit(ExecNode<?, ?> root) {
			dfs(root);
			return results;
		}

		private void dfs(ExecNode<?, ?> node) {
			if (visited.contains(node)) {
				return;
			}
			visited.add(node);

			processor.accept(node);
			results.add(node);

			for (ExecNode<?, ?> input : node.getInputNodes()) {
				if (shouldVisit.apply(node, input)) {
					dfs(input);
				}
			}
		}
	}

	private static class TopologyGraph {
		private final Map<ExecNode<?, ?>, TopologyNode> nodes;
		private int visitTag;

		private TopologyGraph() {
			this.nodes = new HashMap<>();
			this.visitTag = 0;
		}

		private boolean link(ExecNode<?, ?> from, ExecNode<?, ?> to) {
			TopologyNode fromNode = getNode(from);
			TopologyNode toNode = getNode(to);

			if (canReach(toNode, fromNode)) {
				// invalid edge, as `to` is the predecessor of `from`
				return false;
			} else {
				// link `from` and `to`
				fromNode.outputs.add(toNode);
				toNode.inputs.add(fromNode);
				return true;
			}
		}

		private void unlink(ExecNode<?, ?> from, ExecNode<?, ?> to) {
			TopologyNode fromNode = getNode(from);
			TopologyNode toNode = getNode(to);

			fromNode.outputs.remove(toNode);
			toNode.inputs.remove(fromNode);
		}

		private Map<ExecNode<?, ?>, Integer> calculateOrder() {
			Map<ExecNode<?, ?>, Integer> result = new HashMap<>();

			Queue<TopologyNode> queue = new LinkedList<>();
			for (TopologyNode node : nodes.values()) {
				node.tag = node.inputs.size();
				if (node.tag == 0) {
					queue.offer(node);
				}
			}

			while (!queue.isEmpty()) {
				TopologyNode node = queue.poll();
				int order = 0;
				for (TopologyNode input : node.inputs) {
					order = Math.max(order, input.order);
				}
				order++;
				node.order = order;
				result.put(node.execNode, order);

				for (TopologyNode output : node.outputs) {
					output.tag--;
					if (output.tag == 0) {
						queue.offer(output);
					}
				}
			}

			return result;
		}

		private boolean canReach(TopologyNode from, TopologyNode to) {
			Queue<TopologyNode> queue = new LinkedList<>();
			from.tag = --visitTag;
			queue.offer(from);

			while (!queue.isEmpty()) {
				TopologyNode node = queue.poll();
				if (to.equals(node)) {
					return true;
				}

				for (TopologyNode next : node.outputs) {
					if (next.tag == visitTag) {
						continue;
					}
					next.tag = visitTag;
					queue.offer(next);
				}
			}

			return false;
		}

		private TopologyNode getNode(ExecNode<?, ?> node) {
			// NOTE: We treat different `BatchExecBoundedStreamScan`s with same `DataStream` object as the same
			if (node instanceof BatchExecBoundedStreamScan) {
				for (Map.Entry<ExecNode<?, ?>, TopologyNode> entry : nodes.entrySet()) {
					ExecNode<?, ?> key = entry.getKey();
					if (key instanceof BatchExecBoundedStreamScan) {
						DataStream<?> existingStream =
							((BatchExecBoundedStreamScan) key).boundedStreamTable().dataStream();
						DataStream<?> currentStream =
							((BatchExecBoundedStreamScan) node).boundedStreamTable().dataStream();
						if (existingStream.equals(currentStream)) {
							return entry.getValue();
						}
					}
				}
				TopologyNode result = new TopologyNode(node);
				nodes.put(node, result);
				return result;
			} else {
				return nodes.compute(node, (k, v) -> v == null ? new TopologyNode(k) : v);
			}
		}
	}

	private static class TopologyNode {
		private final ExecNode<?, ?> execNode;

		private final List<TopologyNode> inputs;
		private final List<TopologyNode> outputs;

		private int tag;
		private int order;

		private TopologyNode(ExecNode<?, ?> execNode) {
			this.execNode = execNode;

			this.inputs = new ArrayList<>();
			this.outputs = new ArrayList<>();
		}
	}
}
