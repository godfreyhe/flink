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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

public class TransformationConverter {

	private final List<Transformation<?>> inputTransforms;
	private final Transformation<?> tailTransform;

	public TransformationConverter(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform) {
		this.inputTransforms = inputTransforms;
		this.tailTransform = tailTransform;
	}

	public Tuple2<StreamOperatorWrapper<?>, List<StreamOperatorWrapper<?>>> convert() {
		List<StreamOperatorWrapper<?>> headOperatorWrappers = new ArrayList<>();
		StreamOperatorWrapper<?> tailOperatorWrapper = visit(tailTransform, headOperatorWrappers, null);

		return new Tuple2<>(tailOperatorWrapper, headOperatorWrappers);
	}

	private StreamOperatorWrapper<?> visit(
			Transformation<?> transform,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> outputWrapper) {

		if (transform instanceof OneInputTransformation) {
			OneInputTransformation<RowData, RowData> oneInputTransform = (OneInputTransformation) transform;
			Transformation<?> input = oneInputTransform.getInput();

			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(oneInputTransform.getOperatorFactory(), oneInputTransform.getName());
			if (outputWrapper != null) {
				wrapper.addNext(outputWrapper);
			}
			if (isMultipleOperatorInput(input)) {
				headOperatorWrappers.add(wrapper);
			} else {
				wrapper.addPrevious(visit(input, headOperatorWrappers, wrapper));
			}
			return wrapper;
		} else if (transform instanceof TwoInputTransformation) {
			TwoInputTransformation<RowData, RowData, RowData> twoInputTransform = (TwoInputTransformation) transform;
			Transformation<?> input1 = twoInputTransform.getInput1();
			Transformation<?> input2 = twoInputTransform.getInput2();
			boolean isMultipleOperatorInput1 = isMultipleOperatorInput(input1);
			boolean isMultipleOperatorInput2 = isMultipleOperatorInput(input2);

			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(twoInputTransform.getOperatorFactory(), twoInputTransform.getName());
			if (outputWrapper != null) {
				wrapper.addNext(outputWrapper);
			}
			if (isMultipleOperatorInput1 && isMultipleOperatorInput2) {
				headOperatorWrappers.add(wrapper);
			} else if (isMultipleOperatorInput1) {
				wrapper.addPrevious(visit(input2, headOperatorWrappers, wrapper));
				headOperatorWrappers.add(wrapper);
			} else if (isMultipleOperatorInput2) {
				wrapper.addPrevious(visit(input1, headOperatorWrappers, wrapper));
				headOperatorWrappers.add(wrapper);
			} else {
				wrapper.addPrevious(visit(input1, headOperatorWrappers, wrapper));
				wrapper.addPrevious(visit(input2, headOperatorWrappers, wrapper));
			}
			return wrapper;
		} else {
			// TODO supports UnionTransformation
			throw new TableException("Unsupported Transformation: " + transform);
		}
	}

	private boolean isMultipleOperatorInput(Transformation<?> transform) {
		return inputTransforms.contains(transform);
	}
}
