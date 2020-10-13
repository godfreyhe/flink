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

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.StateNameAware;
import org.apache.flink.table.runtime.operators.StateNameContext;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link MultipleInputStreamOperatorBase} to handle streaming operators.
 */
public class StreamMultipleInputStreamOperator extends MultipleInputStreamOperatorBase {

	public StreamMultipleInputStreamOperator(
			StreamOperatorParameters<RowData> parameters,
			List<InputSpec> inputSpecs,
			List<TableOperatorWrapper<?>> headWrappers,
			TableOperatorWrapper<?> tailWrapper) {
		super(parameters, inputSpecs, headWrappers, tailWrapper);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open() throws Exception {
		// initializeState for each operator first
		StateNameContext stateNameContext = new StateNameContext();
		final Iterator<TableOperatorWrapper<?>> it = topologicalOrderingOperators.descendingIterator();
		while (it.hasNext()) {
			StreamOperator<?> operator = it.next().getStreamOperator();
			if (operator instanceof StateNameAware) {
				((StateNameAware) operator).setStateNameContext(stateNameContext);
			} else if (operator instanceof AbstractUdfStreamOperator &&
					((AbstractUdfStreamOperator) operator).getUserFunction() instanceof StateNameAware) {
				((StateNameAware) ((AbstractUdfStreamOperator) operator).getUserFunction()).setStateNameContext(stateNameContext);
			}
			if (operator instanceof AbstractStreamOperator) {
				((AbstractStreamOperator<?>) operator).initializeState(stateHandler, timeServiceManager);
			} else {
				throw new TableException("Unsupported StreamOperator: " + operator);
			}
		}
		super.open();
	}

	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		// go forward through the operator chain and tell each operator
		// to prepare the checkpoint
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId);
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// go forward through the operator chain and tell each operator
		// to do snapshot
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				if (operator instanceof AbstractStreamOperator) {
					((AbstractStreamOperator<?>) operator).snapshotState(context);
				} else if (operator instanceof AbstractStreamOperatorV2) {
					((AbstractStreamOperatorV2<?>) operator).snapshotState(context);
				} else {
					throw new RuntimeException("Unsupported StreamOperator: " + operator);
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		// go forward through the operator chain and tell each operator
		// to notify checkpoint complete
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().notifyCheckpointComplete(checkpointId);
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		super.notifyCheckpointAborted(checkpointId);
		// go back through the operator chain and tell each operator
		// to notify checkpoint aborted
		Iterator<TableOperatorWrapper<?>> it = topologicalOrderingOperators.descendingIterator();
		while (it.hasNext()) {
			TableOperatorWrapper<?> wrapper = it.next();
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
			}
		}
	}
}
