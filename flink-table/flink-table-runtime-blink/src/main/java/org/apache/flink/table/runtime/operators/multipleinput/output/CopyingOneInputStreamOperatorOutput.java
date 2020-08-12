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

package org.apache.flink.table.runtime.operators.multipleinput.output;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * An {@link Output} that can be used to emit copying elements and other messages for {@link OneInputStreamOperator}.
 */
public class CopyingOneInputStreamOperatorOutput extends OutputBase {

	private final OneInputStreamOperator<RowData, RowData> operator;
	private final TypeSerializer<RowData> serializer;
	@Nullable
	private final KeySelector<RowData, ?> keySelector;

	public CopyingOneInputStreamOperatorOutput(
			OneInputStreamOperator<RowData, RowData> operator,
			TypeSerializer<RowData> serializer,
			@Nullable KeySelector<RowData, ?> keySelector) {
		super(operator);
		this.operator = operator;
		this.serializer = serializer;
		this.keySelector = keySelector;
	}

	@Override
	public void emitWatermark(Watermark mark) {
		try {
			operator.processWatermark(mark);
		} catch (Exception e) {
			throw new ExceptionInMultipleInputOperatorException(e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		try {
			operator.processLatencyMarker(latencyMarker);
		} catch (Exception e) {
			throw new ExceptionInMultipleInputOperatorException(e);
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

	protected <X> void pushToOperator(StreamRecord<X> record) {
		try {
			// we know that the given outputTag matches our OutputTag so the record
			// must be of the type that our operator expects.
			@SuppressWarnings("unchecked")
			StreamRecord<RowData> castRecord = (StreamRecord<RowData>) record;
			StreamRecord<RowData> copy = castRecord.copy(serializer.copy(castRecord.getValue()));

			if (keySelector != null) {
				Object key = keySelector.getKey(copy.getValue());
				operator.setCurrentKey(key);
			}
			operator.processElement(copy);
		} catch (Exception e) {
			throw new ExceptionInMultipleInputOperatorException(e);
		}
	}
}
