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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

public class TwoInput extends InputBase {

	private final TwoInputStreamOperator<RowData, RowData, RowData> op;
	private final int inputIndex;

	public TwoInput(
			AbstractStreamOperatorV2<RowData> owner,
			int inputId,
			TwoInputStreamOperator<RowData, RowData, RowData> op,
			int inputIndex) {
		super(owner, inputId, op);
		this.op = op;
		this.inputIndex = inputIndex;
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		switch (inputIndex) {
			case 1:
				op.processElement1(element);
				break;
			case 2:
				op.processElement2(element);
				break;
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		switch (inputIndex) {
			case 1:
				op.processWatermark1(mark);
				break;
			case 2:
				op.processWatermark2(mark);
				break;
		}
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		switch (inputIndex) {
			case 1:
				op.processLatencyMarker1(latencyMarker);
				break;
			case 2:
				op.processLatencyMarker2(latencyMarker);
				break;
		}
	}
}
