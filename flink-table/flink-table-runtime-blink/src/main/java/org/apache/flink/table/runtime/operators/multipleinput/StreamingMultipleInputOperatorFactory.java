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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

public class StreamingMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

	private final int numberOfInputs;
	private final List<Transformation<?>> inputTransforms; // TODO change to header transformation ?
	private final Transformation<?> tailTransform;

	public StreamingMultipleInputOperatorFactory(
			int numberOfInputs,
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform) {
		this.numberOfInputs = numberOfInputs;
		this.inputTransforms = inputTransforms;
		this.tailTransform = tailTransform;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends StreamOperator<RowData>> T createStreamOperator(StreamOperatorParameters<RowData> parameters) {
		List<StreamOperatorWrapper<?>> headOperatorWrappers = new ArrayList<>();
		StreamOperatorWrapper<?> tailOperatorWrapper = visit(tailTransform, headOperatorWrappers, null);

		return (T) new StreamingMultipleInputOperator(
				parameters,
				numberOfInputs,
				headOperatorWrappers,
				tailOperatorWrapper);
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return StreamingMultipleInputOperator.class;
	}

	private StreamOperatorWrapper<?> visit(
			Transformation<?> transform,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> outputWrapper) {

		if (transform instanceof OneInputTransformation) {
			OneInputTransformation<?, ?> oneInputTransform = (OneInputTransformation<?, ?>) transform;
			Transformation<?> input = oneInputTransform.getInput();
			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(oneInputTransform.getOperator());
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
			TwoInputTransformation<?, ?, ?> twoInputTransform = (TwoInputTransformation<?, ?, ?>) transform;
			Transformation<?> input1 = twoInputTransform.getInput1();
			Transformation<?> input2 = twoInputTransform.getInput2();
			boolean isMultipleOperatorInput1 = isMultipleOperatorInput(input1);
			boolean isMultipleOperatorInput2 = isMultipleOperatorInput(input2);

			StreamOperatorWrapper<?> wrapper = new StreamOperatorWrapper(twoInputTransform.getOperator());
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
