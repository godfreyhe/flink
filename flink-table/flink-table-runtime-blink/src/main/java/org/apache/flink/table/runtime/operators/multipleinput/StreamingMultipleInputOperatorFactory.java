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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class StreamingMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

	private final int numberOfInputs;
	private final List<StreamOperatorWrapper<?>> headOperatorWrappers;
	private final StreamOperatorWrapper<?> tailOperatorWrapper;

	public StreamingMultipleInputOperatorFactory(
			int numberOfInputs,
			List<StreamOperatorWrapper<?>> headOperatorWrappers,
			StreamOperatorWrapper<?> tailOperatorWrapper) {
		this.numberOfInputs = numberOfInputs;
		this.headOperatorWrappers = headOperatorWrappers;
		this.tailOperatorWrapper = tailOperatorWrapper;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends StreamOperator<RowData>> T createStreamOperator(StreamOperatorParameters<RowData> parameters) {
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

}
