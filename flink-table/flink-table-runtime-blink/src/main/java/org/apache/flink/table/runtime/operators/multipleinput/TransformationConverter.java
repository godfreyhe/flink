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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TransformationConverter {

	private final List<Transformation<?>> inputTransforms;
	private final Transformation<?> tailTransform;
	private final int[] readOrder;

	private ResourceSpec minResources;
	private ResourceSpec preferredResources;

	public TransformationConverter(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform) {
		this(inputTransforms, tailTransform, new int[inputTransforms.size()]);
	}

	public TransformationConverter(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform,
			int[] readOrder) {
		this.inputTransforms = inputTransforms;
		this.tailTransform = tailTransform;
		this.readOrder = readOrder;
	}

	public Tuple2<StreamOperatorWrapper<?>, List<StreamOperatorWrapper<?>>> convert() {
		List<StreamOperatorWrapper<?>> headOperatorWrappers = new ArrayList<>();
		StreamOperatorWrapper<?> tailOperatorWrapper = visit(tailTransform, headOperatorWrappers, null, -1);

		return new Tuple2<>(tailOperatorWrapper, headOperatorWrappers);
	}

	public ResourceSpec getMinResources() {
		return minResources;
	}

	public ResourceSpec getPreferredResources() {
		return preferredResources;
	}

	private StreamOperatorWrapper<?> visit(
			Transformation<?> transform,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> outputWrapper,
			int outputIdx) {

		if (minResources == null) {
			minResources = transform.getMinResources();
			preferredResources = transform.getPreferredResources();
		} else {
			minResources = minResources.merge(transform.getMinResources());
			preferredResources = preferredResources.merge(transform.getPreferredResources());
		}

		if (transform instanceof OneInputTransformation) {
			OneInputTransformation<RowData, RowData> oneInputTransform = (OneInputTransformation) transform;
			Transformation<?> input = oneInputTransform.getInput();

			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(
				oneInputTransform.getOperatorFactory(),
				oneInputTransform.getName(),
				Collections.singletonList(oneInputTransform.getInputType()),
				oneInputTransform.getOutputType());
			if (outputWrapper != null) {
				wrapper.addNext(outputWrapper, outputIdx);
			}

			int inputIdx = inputTransforms.indexOf(input);
			if (inputIdx >= 0) {
				wrapper.readOrder = Collections.singletonList(readOrder[inputIdx]);
				headOperatorWrappers.add(wrapper);
			} else {
				wrapper.addPrevious(visit(input, headOperatorWrappers, wrapper, 1), 1);
			}
			return wrapper;
		} else if (transform instanceof TwoInputTransformation) {
			TwoInputTransformation<RowData, RowData, RowData> twoInputTransform = (TwoInputTransformation) transform;
			Transformation<?> input1 = twoInputTransform.getInput1();
			Transformation<?> input2 = twoInputTransform.getInput2();
			int inputIdx1 = inputTransforms.indexOf(input1);
			int inputIdx2 = inputTransforms.indexOf(input2);

			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(
				twoInputTransform.getOperatorFactory(),
				twoInputTransform.getName(),
				Arrays.asList(twoInputTransform.getInputType1(), twoInputTransform.getInputType2()),
				twoInputTransform.getOutputType());
			if (outputWrapper != null) {
				wrapper.addNext(outputWrapper, outputIdx);
			}
			if (inputIdx1 >= 0 && inputIdx2 >= 0) {
				wrapper.readOrder = Arrays.asList(inputIdx1, inputIdx2);
				headOperatorWrappers.add(wrapper);
			} else if (inputIdx1 >= 0) {
				wrapper.readOrder = Arrays.asList(inputIdx1, -1);
				wrapper.addPrevious(visit(input2, headOperatorWrappers, wrapper, 2), 2);
				headOperatorWrappers.add(wrapper);
			} else if (inputIdx2 >= 0) {
				wrapper.readOrder = Arrays.asList(-1, inputIdx2);
				wrapper.addPrevious(visit(input1, headOperatorWrappers, wrapper, 1), 1);
				headOperatorWrappers.add(wrapper);
			} else {
				wrapper.addPrevious(visit(input1, headOperatorWrappers, wrapper, 1), 1);
				wrapper.addPrevious(visit(input2, headOperatorWrappers, wrapper, 2), 2);
			}
			return wrapper;
		} else {
			// TODO supports UnionTransformation
			throw new TableException("Unsupported Transformation: " + transform);
		}
	}
}
