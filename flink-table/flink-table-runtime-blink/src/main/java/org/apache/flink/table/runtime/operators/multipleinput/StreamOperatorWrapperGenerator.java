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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link StreamOperatorWrapper} graph from a graph of {@link Transformation}s.
 */
public class StreamOperatorWrapperGenerator {

	/**
	 * Original input transformations for {@link MultipleInputStreamOperator}.
	 */
	private final List<Transformation<?>> inputTransforms;

	/**
	 * The tail (root) transformation of the transformation-graph in {@link MultipleInputStreamOperator}.
	 */
	private final Transformation<?> tailTransform;

	/**
	 * The read order corresponding to each transformation in {@link #inputTransforms}.
	 */
	private final int[] readOrders;

	/**
	 * Reordered input transformations which order corresponds to the order of {@link #inputSpecs}.
	 */
	private final List<Transformation<?>> orderedInputTransforms;

	/**
	 * The input specs which order corresponds to the order of {@link #orderedInputTransforms}.
	 */
	private final List<InputSpec> inputSpecs;

	/**
	 * The head (leaf) operator wrappers of the operator-graph in {@link MultipleInputStreamOperator}.
	 */
	private final List<StreamOperatorWrapper<?>> headOperatorWrappers;

	/**
	 * The tail (root) operator wrapper of the operator-graph in {@link MultipleInputStreamOperator}.
	 */
	private StreamOperatorWrapper<?> tailOperatorWrapper;

	private final Map<Transformation<?>, StreamOperatorWrapper<?>> visitedTransforms;

	private int parallelism;
	private int maxParallelism;
	private ResourceSpec minResources;
	private ResourceSpec preferredResources;
	private int managedMemoryWeight;

	public StreamOperatorWrapperGenerator(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform) {
		this(inputTransforms, tailTransform, new int[inputTransforms.size()]);
	}

	public StreamOperatorWrapperGenerator(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform,
			int[] readOrders) {
		this.inputTransforms = inputTransforms;
		this.tailTransform = tailTransform;
		this.readOrders = readOrders;
		this.inputSpecs = new ArrayList<>();
		this.headOperatorWrappers = new ArrayList<>();
		this.orderedInputTransforms = new ArrayList<>();
		this.visitedTransforms = new IdentityHashMap<>();
	}

	public void generate() {
		tailOperatorWrapper = visit(tailTransform, null);
		checkState(orderedInputTransforms.size() == inputTransforms.size());
		checkState(orderedInputTransforms.size() == inputSpecs.size());
	}

	public List<Transformation<?>> getOrderedInputTransforms() {
		return orderedInputTransforms;
	}

	public List<InputSpec> getInputSpecs() {
		return inputSpecs;
	}

	public List<StreamOperatorWrapper<?>> getHeadOperatorWrappers() {
		return headOperatorWrappers;
	}

	public StreamOperatorWrapper<?> getTailOperatorWrapper() {
		return tailOperatorWrapper;
	}

	public int getParallelism() {
		return parallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public ResourceSpec getMinResources() {
		return minResources;
	}

	public ResourceSpec getPreferredResources() {
		return preferredResources;
	}

	public int getManagedMemoryWeight() {
		return managedMemoryWeight;
	}

	@SuppressWarnings("unchecked")
	private StreamOperatorWrapper<?> visit(
			Transformation<?> transform,
			StreamOperatorWrapper<?> outputWrapper) {
		if (visitedTransforms.containsKey(transform)) {
			return visitedTransforms.get(transform);
		}

		if (minResources == null) {
			parallelism = transform.getParallelism();
			maxParallelism = transform.getMaxParallelism();
			minResources = transform.getMinResources();
			preferredResources = transform.getPreferredResources();
			managedMemoryWeight = transform.getManagedMemoryWeight();
		} else {
			// TODO
//			checkState(transform.getParallelism() == parallelism, "This should not happen.");
//			checkState(transform.getMaxParallelism() == maxParallelism, "This should not happen.");
			minResources = minResources.merge(transform.getMinResources());
			preferredResources = preferredResources.merge(transform.getPreferredResources());
			managedMemoryWeight += transform.getManagedMemoryWeight();
		}

		final StreamOperatorWrapper<?> wrapper;
		if (transform instanceof OneInputTransformation) {
			wrapper = visitOneInputTransformation((OneInputTransformation) transform, outputWrapper);
		} else if (transform instanceof TwoInputTransformation) {
			wrapper = visitTwoInputTransformation((TwoInputTransformation) transform, outputWrapper);
		} else {
			// TODO supports more transformation, e.g. UnionTransformation
			throw new RuntimeException("Unsupported Transformation: " + transform);
		}
		visitedTransforms.put(transform, wrapper);
		return wrapper;
	}

	private StreamOperatorWrapper<?> visitOneInputTransformation(
			OneInputTransformation<RowData, RowData> transform,
			StreamOperatorWrapper<?> outputWrapper) {
		Transformation<?> input = transform.getInput();

		StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper<>(
				transform.getOperatorFactory(),
				transform.getName(),
				Collections.singletonList(transform.getInputType()),
				transform.getOutputType()
		);

		if (outputWrapper != null) {
			wrapper.addOutput(outputWrapper);
		}

		int inputIdx = inputTransforms.indexOf(input);
		if (inputIdx >= 0) {
			orderedInputTransforms.add(input);
			inputSpecs.add(createInputSpec(readOrders[inputIdx], wrapper, 1));
			headOperatorWrappers.add(wrapper);
		} else {
			StreamOperatorWrapper<?> inputWrapper = visit(input, wrapper);
			wrapper.addInput(inputWrapper, 1);
		}
		return wrapper;
	}

	private StreamOperatorWrapper<?> visitTwoInputTransformation(
			TwoInputTransformation<RowData, RowData, RowData> transform,
			StreamOperatorWrapper<?> outputWrapper) {
		Transformation<?> input1 = transform.getInput1();
		Transformation<?> input2 = transform.getInput2();
		int inputIdx1 = inputTransforms.indexOf(input1);
		int inputIdx2 = inputTransforms.indexOf(input2);

		StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(
				transform.getOperatorFactory(),
				transform.getName(),
				Arrays.asList(transform.getInputType1(), transform.getInputType2()),
				transform.getOutputType());

		if (outputWrapper != null) {
			wrapper.addOutput(outputWrapper);
		}

		if (inputIdx1 >= 0 && inputIdx2 >= 0) {
			orderedInputTransforms.add(input1);
			inputSpecs.add(createInputSpec(readOrders[inputIdx1], wrapper, 1));
			orderedInputTransforms.add(input2);
			inputSpecs.add(createInputSpec(readOrders[inputIdx2], wrapper, 2));
			headOperatorWrappers.add(wrapper);
		} else if (inputIdx1 >= 0) {
			StreamOperatorWrapper<?> inputWrapper = visit(input2, wrapper);
			wrapper.addInput(inputWrapper, 2);
			orderedInputTransforms.add(input1);
			inputSpecs.add(createInputSpec(readOrders[inputIdx1], wrapper, 1));
			headOperatorWrappers.add(wrapper);
		} else if (inputIdx2 >= 0) {
			StreamOperatorWrapper<?> inputWrapper = visit(input1, wrapper);
			wrapper.addInput(inputWrapper, 1);
			orderedInputTransforms.add(input2);
			inputSpecs.add(createInputSpec(readOrders[inputIdx2], wrapper, 2));
			headOperatorWrappers.add(wrapper);
		} else {
			StreamOperatorWrapper<?> inputWrapper1 = visit(input1, wrapper);
			wrapper.addInput(inputWrapper1, 1);
			StreamOperatorWrapper<?> inputWrapper2 = visit(input2, wrapper);
			wrapper.addInput(inputWrapper2, 2);
		}
		return wrapper;
	}

	private InputSpec createInputSpec(int readOrder, StreamOperatorWrapper<?> outputWrapper, int inputIndex) {
		checkNotNull(outputWrapper);
		int inputId = inputSpecs.size() + 1;
		return new InputSpec(inputId, readOrder, outputWrapper, inputIndex);
	}
}
