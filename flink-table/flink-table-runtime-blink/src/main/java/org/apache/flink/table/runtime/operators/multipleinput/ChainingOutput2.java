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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

public class ChainingOutput2 implements Output<StreamRecord<RowData>> {

	protected final TwoInputStreamOperator<RowData, RowData, RowData> operator;
	private final int inputIndex;

	public ChainingOutput2(TwoInputStreamOperator<RowData, RowData, RowData> operator, int inputIndex) {
		this.operator = operator;
		this.inputIndex = inputIndex;
	}

	@Override
	public void emitWatermark(Watermark mark) {
		try {
			switch (inputIndex) {
				case 1:
					operator.processWatermark1(mark);
					break;
				case 2:
					operator.processWatermark2(mark);
					break;
			}
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		try {
			switch (inputIndex) {
				case 1:
					operator.processLatencyMarker1(latencyMarker);
					break;
				case 2:
					operator.processLatencyMarker2(latencyMarker);
					break;
			}
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void collect(StreamRecord<RowData> record) {
		pushToOperator(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		pushToOperator(record);
	}

	@Override
	public void close() {
		try {
			operator.close();
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	protected <X> void pushToOperator(StreamRecord<X> record) {
		try {
			// we know that the given outputTag matches our OutputTag so the record
			// must be of the type that our operator expects.
			@SuppressWarnings("unchecked")
			StreamRecord<RowData> castRecord = (StreamRecord<RowData>) record;

			switch (inputIndex) {
				case 1:
					operator.setKeyContextElement1(castRecord);
					operator.processElement1(castRecord);
					break;
				case 2:
					operator.setKeyContextElement2(castRecord);
					operator.processElement2(castRecord);
					break;
			}
		} catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}
}
