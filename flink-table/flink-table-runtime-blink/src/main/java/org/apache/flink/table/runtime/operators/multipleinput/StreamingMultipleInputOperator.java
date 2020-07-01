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

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
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
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class StreamingMultipleInputOperator
		extends AbstractStreamOperatorV2<RowData>
		implements MultipleInputStreamOperator<RowData> {

	private final int numberOfInputs;
	private final List<StreamOperatorWrapper<?>> headOperatorWrappers;
	private final StreamOperatorWrapper<?> tailOperatorWrapper;

	public StreamingMultipleInputOperator(
			StreamOperatorParameters<RowData> parameters,
			int numberOfInputs,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> tailOperatorWrapper) {
		super(parameters, numberOfInputs);
		this.numberOfInputs = numberOfInputs;
		this.headOperatorWrappers = checkNotNull(headOperatorWrappers);
		this.tailOperatorWrapper = checkNotNull(tailOperatorWrapper);
		// init all operator
		createOperator(parameters, tailOperatorWrapper);
	}

	private void createOperator(StreamOperatorParameters<RowData> parameters, StreamOperatorWrapper<?> wrapper) {
		if (wrapper.factory instanceof ProcessingTimeServiceAware) {
			((ProcessingTimeServiceAware) wrapper.factory).setProcessingTimeService(parameters.getProcessingTimeService());
		}
		wrapper.createOperator(parameters);
		if (wrapper.getPrevious().size() == 1) {
			Output<StreamRecord<RowData>> output = new ChainingOutput(
					(OneInputStreamOperator<RowData, RowData>) wrapper.getStreamOperator());
			createOperator(createParameters(parameters, output), wrapper.getPrevious().get(0).f0);
		} else if (wrapper.getPrevious().size() == 2) {
			Output<StreamRecord<RowData>> output1 = new ChainingOutput2(
					(TwoInputStreamOperator<RowData, RowData, RowData>) wrapper.getStreamOperator(), wrapper.getPrevious().get(0).f1);
			createOperator(createParameters(parameters, output1), wrapper.getPrevious().get(0).f0);

			Output<StreamRecord<RowData>> output2 = new ChainingOutput2(
					(TwoInputStreamOperator<RowData, RowData, RowData>) wrapper.getStreamOperator(), wrapper.getPrevious().get(1).f1);
			createOperator(createParameters(parameters, output2), wrapper.getPrevious().get(1).f0);
		} else if (wrapper.getPrevious().isEmpty()) {
			// do nothing
		} else {
			throw new TableException("Unsupported StreamOperatorWrapper: " + wrapper);
		}
	}

	private StreamOperatorParameters<RowData> createParameters(
			StreamOperatorParameters<RowData> parameters,
			Output<StreamRecord<RowData>> output) {
		return new StreamOperatorParameters<>(
				parameters.getContainingTask(),
				parameters.getStreamConfig(),
				output,
				parameters::getProcessingTimeService,
				parameters.getOperatorEventDispatcher()
		);
	}

	@Override
	public List<Input> getInputs() {
		List<Input> inputs = new ArrayList<>(numberOfInputs);
		int inputId = 1; // start from 1
		for (StreamOperatorWrapper<?> wrapper : headOperatorWrappers) {
			StreamOperator<RowData> op = wrapper.getStreamOperator();
			if (op instanceof OneInputStreamOperator) {
				inputs.add(new OneInput(this, inputId++, (OneInputStreamOperator<RowData, RowData>) op));
			} else if (op instanceof TwoInputStreamOperator) {
				inputs.add(new TwoInput(this, inputId++, (TwoInputStreamOperator<RowData, RowData, RowData>) op, 1));
				inputs.add(new TwoInput(this, inputId++, (TwoInputStreamOperator<RowData, RowData, RowData>) op, 2));
			} else {
				throw new TableException("Unsupported StreamOperator: " + op);
			}
		}
		checkArgument(inputs.size() == numberOfInputs);
		return inputs;
	}

	@Override
	public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
		super.initializeState(streamTaskStateManager);
		for (StreamOperatorWrapper<?> wrapper : getAllOperators(true)) {
			StreamOperator<?> operator = wrapper.getStreamOperator();
			operator.initializeState(streamTaskStateManager);
		}
	}

	/**
	 * Initialize state all operators in the chain from <b>tail to head</b>,
	 * contrary to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #close()}).
	 */
	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		for (StreamOperatorWrapper<?> wrapper : getAllOperators(true)) {
			StreamOperator<?> operator = wrapper.getStreamOperator();
			if (operator instanceof AbstractStreamOperator) {
				((AbstractStreamOperator<?>) operator).initializeState(context);
			} else if (operator instanceof AbstractStreamOperatorV2) {
				((AbstractStreamOperatorV2<?>) operator).initializeState(context);
			} else {
				throw new TableException("Unsupported StreamOperator: " + operator);
			}
		}
	}

	@Override
	public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
		super.initializeState(streamTaskStateManager);
		for (StreamOperatorWrapper<?> wrapper : getAllOperators(true)) {
			StreamOperator<?> operator = wrapper.getStreamOperator();
			operator.initializeState(streamTaskStateManager);
		}
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

	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		// go forward through the operator chain and tell each operator
		// to prepare the checkpoint
		for (StreamOperatorWrapper<?> wrapper : getAllOperators()) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				operator.prepareSnapshotPreBarrier(checkpointId);
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// go forward through the operator chain and tell each operator
		// to do snapshot
		for (StreamOperatorWrapper<?> wrapper : getAllOperators()) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				if (operator instanceof AbstractStreamOperator) {
					((AbstractStreamOperator<?>) operator).snapshotState(context);
				} else if (operator instanceof AbstractStreamOperatorV2) {
					((AbstractStreamOperatorV2<?>) operator).snapshotState(context);
				} else {
					throw new TableException("Unsupported StreamOperator: " + operator);
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		// go forward through the operator chain and tell each operator
		// to notify checkpoint complete
		for (StreamOperatorWrapper<?> wrapper : getAllOperators()) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				operator.notifyCheckpointComplete(checkpointId);
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		super.notifyCheckpointAborted(checkpointId);
		// go back through the operator chain and tell each operator
		// to notify checkpoint aborted
		for (StreamOperatorWrapper<?> wrapper : getAllOperators(true)) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				operator.notifyCheckpointAborted(checkpointId);
			}
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
