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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class handles the close, endInput and other related logic of a {@link StreamOperator}.
 * It also automatically propagates the close operation to the next wrapper that the {@link #next}
 * points to, so we can use {@link #next} to link all operator wrappers in the operator chain and
 * close all operators only by calling the {@link #close()} method of the
 * header operator wrapper.
 */
@Internal
public class StreamOperatorWrapper<OP extends StreamOperator<RowData>> implements Serializable {

	private transient OP wrapped;

	public final StreamOperatorFactory<RowData> factory;
	private final String name;
	public List<Integer> readOrder = Collections.emptyList();

	private List<Tuple2<StreamOperatorWrapper<?>, Integer>> previous;
	private int endCount;
	private List<TypeInformation> inputTypes;
	private TypeInformation outputType;

	private List<StreamOperatorWrapper<?>> next;
	private List<Integer> nextIdx;

	private boolean closed;

	StreamOperatorWrapper(StreamOperatorFactory<RowData> factory, String name, List<TypeInformation> inputTypes, TypeInformation outputType) {
		this.factory = checkNotNull(factory);
		this.name = name;

		this.previous = new ArrayList<>();
		this.endCount = 0;
		this.inputTypes = inputTypes;
		this.outputType = outputType;

		this.next = new ArrayList<>();
		this.nextIdx = new ArrayList<>();
	}

	/**
	 * Closes the wrapped operator and propagates the close operation to the next wrapper that the
	 * {@link #next} points to.
	 *
	 * <p>Note that this method must be called in the task thread, because we need to call
	 * {@link MailboxExecutor#yield()} to take the mails of closing operator and running timers and
	 * run them.
	 */
	public void close() throws Exception {
		close(false);
	}

	/**
	 * Checks if the wrapped operator has been closed.
	 *
	 * <p>Note that this method must be called in the task thread.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Ends an input of the operator contained by this wrapper.
	 *
	 * @param inputId the input ID starts from 1 which indicates the first input.
	 */
	public void endOperatorInput(int inputId) throws Exception {
		endCount++;
		if (wrapped instanceof BoundedOneInput) {
			((BoundedOneInput) wrapped).endInput();
			endNexts();
		} else if (wrapped instanceof BoundedMultiInput) {
			((BoundedMultiInput) wrapped).endInput(inputId);
			if (endCount >= 2) {
				endNexts();
			}
		} else {
			endNexts();
		}
	}

	private void endNexts() throws Exception {
		for (int i = 0; i < next.size(); i++) {
			next.get(i).endOperatorInput(nextIdx.get(i));
		}
	}

	public void createOperator(StreamOperatorParameters<RowData> parameters) {
		checkArgument(wrapped == null);
		wrapped = factory.createStreamOperator(parameters);
		if (wrapped instanceof AbstractStreamOperator) {
			((AbstractStreamOperator) wrapped).setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		}
	}

	public OP getStreamOperator() {
		return checkNotNull(wrapped);
	}

	public List<TypeInformation> getInputTypes() {
		return inputTypes;
	}

	public TypeInformation getOutputType() {
		return outputType;
	}

	void addPrevious(StreamOperatorWrapper<?> previous, int inputIndex) {
		this.previous.add(new Tuple2<>(previous, inputIndex));
	}

	void addNext(StreamOperatorWrapper<?> next, int idx) {
		this.next.add(next);
		this.nextIdx.add(idx);
	}

	public List<Tuple2<StreamOperatorWrapper<?>, Integer>> getPrevious() {
		return previous;
	}

	public List<StreamOperatorWrapper<?>> getNext() {
		return next;
	}

	private void close(boolean invokingEndInput) throws Exception {
		if (isClosed()) {
			return;
		}
		// TODO
//		if (invokingEndInput) {
//			// NOTE: This only do for the case where the operator is one-input operator. At present,
//			// any non-head operator on the operator chain is one-input operator.
//			actionExecutor.runThrowing(() -> endOperatorInput(inputIndex));
//		}

		closed = true;
		wrapped.close();
	}

	/**
	 * TODO consider the following pattern:
	 * -- A --
	 *        \
	 *         J --
	 *        /
	 * ------
	 *
	 * J is not only the header node but also the tail node. How to ensure the visit order ?
	 */
	static class ReadIterator implements Iterator<StreamOperatorWrapper<?>>, Iterable<StreamOperatorWrapper<?>> {

		private final boolean reverse;
		// breadth first search
		private final Queue<StreamOperatorWrapper<?>> queue;
		private final Set<StreamOperatorWrapper<?>> visited;

		ReadIterator(List<StreamOperatorWrapper<?>> first, boolean reverse) {
			this.reverse = reverse;
			this.queue = new LinkedList<>();
			this.visited = Collections.newSetFromMap(new IdentityHashMap<>());
			enqueue1(first);
		}

		@Override
		public boolean hasNext() {
			return !queue.isEmpty();
		}

		@Override
		public StreamOperatorWrapper<?> next() {
			if (hasNext()) {
				StreamOperatorWrapper<?> next = queue.poll();
				if (reverse) {
					enqueue2(next.previous);
				} else {
					enqueue1(next.next);
				}
				return next;
			}

			throw new NoSuchElementException();
		}

		private void enqueue1(List<StreamOperatorWrapper<?>> wrappers) {
			for (StreamOperatorWrapper<?> wrapper : wrappers) {
				if (!visited.contains(wrapper)) {
					visited.add(wrapper);
					queue.add(wrapper);
				}
			}
		}

		private void enqueue2(List<Tuple2<StreamOperatorWrapper<?>, Integer>> wrappers) {
			for (Tuple2<StreamOperatorWrapper<?>, Integer> wrapper : wrappers) {
				if (!visited.contains(wrapper.f0)) {
					visited.add(wrapper.f0);
					queue.add(wrapper.f0);
				}
			}
		}

		@Nonnull
		@Override
		public Iterator<StreamOperatorWrapper<?>> iterator() {
			return this;
		}
	}
}
