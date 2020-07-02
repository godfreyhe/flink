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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class BatchMultipleInputOperator
		extends AbstractStreamOperatorV2<RowData>
		implements MultipleInputStreamOperator<RowData>, BoundedMultiInput, InputSelectable {

	private final int numberOfInputs;
	private final List<StreamOperatorWrapper<?>> headOperatorWrappers;
	private final StreamOperatorWrapper<?> tailOperatorWrapper;

	private List<Integer> readOrders;
	private int latestOrder;
	private BitSet availableBits;
	private BitSet endedBits;
	private List<Tuple2<StreamOperatorWrapper, Integer>> wrappers;

	public BatchMultipleInputOperator(
			StreamOperatorParameters<RowData> parameters,
			int numberOfInputs,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> tailOperatorWrapper) {
		super(parameters, numberOfInputs);
		this.numberOfInputs = numberOfInputs;
		this.headOperatorWrappers = checkNotNull(headOperatorWrappers);
		this.tailOperatorWrapper = checkNotNull(tailOperatorWrapper);
		// init all operator
		createOperator(createParameters(parameters, parameters.getOutput(), tailOperatorWrapper.getInputTypes(), tailOperatorWrapper.getOutputType()), tailOperatorWrapper);
	}

	private void createOperator(StreamOperatorParameters<RowData> parameters, StreamOperatorWrapper<?> wrapper) {
		if (wrapper.factory instanceof ProcessingTimeServiceAware) {
			((ProcessingTimeServiceAware) wrapper.factory).setProcessingTimeService(parameters.getProcessingTimeService());
		}
		wrapper.createOperator(parameters);
		if (wrapper.getPrevious().size() == 1) {
			Output<StreamRecord<RowData>> output = new ChainingOutput(
				(OneInputStreamOperator<RowData, RowData>) wrapper.getStreamOperator());
			StreamOperatorWrapper<?> previous = wrapper.getPrevious().get(0).f0;
			createOperator(createParameters(parameters, output, previous.getInputTypes(), previous.getOutputType()), previous);
		} else if (wrapper.getPrevious().size() == 2) {
			Output<StreamRecord<RowData>> output1 = new ChainingOutput2(
				(TwoInputStreamOperator<RowData, RowData, RowData>) wrapper.getStreamOperator(), wrapper.getPrevious().get(0).f1);
			StreamOperatorWrapper<?> previous1 = wrapper.getPrevious().get(0).f0;
			createOperator(createParameters(parameters, output1, previous1.getInputTypes(), previous1.getOutputType()), previous1);

			Output<StreamRecord<RowData>> output2 = new ChainingOutput2(
				(TwoInputStreamOperator<RowData, RowData, RowData>) wrapper.getStreamOperator(), wrapper.getPrevious().get(1).f1);
			StreamOperatorWrapper<?> previous2 = wrapper.getPrevious().get(1).f0;
			createOperator(createParameters(parameters, output2, previous2.getInputTypes(), previous2.getOutputType()), previous2);
		} else if (wrapper.getPrevious().isEmpty()) {
			// do nothing
		} else {
			throw new TableException("Unsupported StreamOperatorWrapper: " + wrapper);
		}
	}

	private StreamOperatorParameters<RowData> createParameters(
		StreamOperatorParameters<RowData> parameters,
		Output<StreamRecord<RowData>> output,
		List<TypeInformation> inputTypes,
		TypeInformation outputType) {

		ExecutionConfig executionConfig = getExecutionConfig();

		StreamConfig streamConfig = new StreamConfig(parameters.getStreamConfig().getConfiguration().clone());
		streamConfig.setTypeSerializersIn(inputTypes.stream().map(t -> t.createSerializer(executionConfig)).toArray(TypeSerializer[]::new));
		streamConfig.setTypeSerializerOut(outputType.createSerializer(executionConfig));

		return new StreamOperatorParameters<>(
			parameters.getContainingTask(),
			streamConfig,
			output,
			parameters::getProcessingTimeService,
			parameters.getOperatorEventDispatcher()
		);
	}

	@Override
	public List<Input> getInputs() {
		List<Input> inputs = new ArrayList<>(numberOfInputs);
		readOrders = new ArrayList<>(numberOfInputs);
		latestOrder = -1;
		availableBits = new BitSet(numberOfInputs);
		endedBits = new BitSet(numberOfInputs);
		wrappers = new ArrayList<>();

		int inputId = 0;
		for (StreamOperatorWrapper<?> wrapper : headOperatorWrappers) {
			StreamOperator<RowData> op = wrapper.getStreamOperator();
			if (op instanceof OneInputStreamOperator) {
				inputs.add(new OneInput(this, ++inputId, (OneInputStreamOperator<RowData, RowData>) op));
				readOrders.add(wrapper.readOrder.get(0));
				wrappers.add(Tuple2.of(wrapper, 1));
			} else if (op instanceof TwoInputStreamOperator) {
				inputs.add(new TwoInput(this, ++inputId, (TwoInputStreamOperator<RowData, RowData, RowData>) op, 1));
				readOrders.add(wrapper.readOrder.get(0));
				wrappers.add(Tuple2.of(wrapper, 1));
				inputs.add(new TwoInput(this, ++inputId, (TwoInputStreamOperator<RowData, RowData, RowData>) op, 2));
				readOrders.add(wrapper.readOrder.get(1));
				wrappers.add(Tuple2.of(wrapper, 2));
			} else {
				throw new TableException("Unsupported StreamOperator: " + op);
			}
		}

		checkArgument(inputs.size() == numberOfInputs);
		return inputs;
	}

	@Override
	public InputSelection nextSelection() {
		BitSet bs = (BitSet) availableBits.clone();
		bs.andNot(endedBits);

		while (bs.isEmpty() && endedBits.cardinality() < numberOfInputs) {
			latestOrder++;
			availableBits.clear();
			for (int i = 0; i < readOrders.size(); i++) {
				if (readOrders.get(i) == latestOrder) {
					availableBits.set(i);
				}
			}

			bs = (BitSet) availableBits.clone();
			bs.andNot(endedBits);
		}

		InputSelection.Builder builder = new InputSelection.Builder();
		for (int i = 0; i < numberOfInputs; i++) {
			if (bs.get(i)) {
				builder.select(i + 1);
			}
		}
		return builder.build();
	}

	@Override
	public void endInput(int inputId) throws Exception {
		endedBits.set(inputId - 1);
		Tuple2<StreamOperatorWrapper, Integer> wrapper = wrappers.get(inputId - 1);
		wrapper.f0.endOperatorInput(wrapper.f1);
	}

	/**
	 * Open all operators in the chain from <b>tail to head</b>,
	 * contrary to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #close()}).
	 */
	@Override
	public void open() throws Exception {
		super.open();
		for (StreamOperatorWrapper<?> wrapper : getAllOperators(true)) {
			StreamOperator<?> operator = wrapper.getStreamOperator();
			operator.open();
		}
	}

	/**
	 * Closes all operators in a chain effect way. Closing happens from <b>head to tail</b> operator
	 * in the chain, contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #initializeState(StreamTaskStateInitializer)}).
	 */
	@Override
	public void close() throws Exception {
		super.close();
		for (StreamOperatorWrapper<?> wrapper : getAllOperators()) {
			wrapper.close();
		}
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		for (StreamOperatorWrapper<?> wrapper : getAllOperators()) {
			StreamOperator<?> operator = wrapper.getStreamOperator();
			operator.dispose();
		}
	}

	/**
	 * Returns an {@link Iterable} which traverses all operators in forward topological order.
	 */
	public Iterable<StreamOperatorWrapper<?>> getAllOperators() {
		return getAllOperators(false);
	}

	/**
	 * Returns an {@link Iterable} which traverses all operators in forward or reverse
	 * topological order.
	 */
	public Iterable<StreamOperatorWrapper<?>> getAllOperators(boolean reverse) {
		return reverse ?
			new StreamOperatorWrapper.ReadIterator(Collections.singletonList(tailOperatorWrapper), true) :
			new StreamOperatorWrapper.ReadIterator(headOperatorWrappers, false);
	}
}
