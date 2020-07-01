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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

public abstract class InputBase implements Input<RowData> {

	private final KeySelector<?, ?> stateKeySelector;
	private final StreamOperator<RowData> op;

	public InputBase(
			AbstractStreamOperatorV2<RowData> owner,
			int inputId,
			StreamOperator<RowData> op) {
		this.op = op;
		// TODO ???
		this.stateKeySelector = owner.getOperatorConfig().getStatePartitioner(inputId - 1, owner.getUserCodeClassloader());
	}

	@Override
	public void setKeyContextElement(StreamRecord record) throws Exception {
		internalSetKeyContextElement(record, stateKeySelector);
	}

	protected <T> void internalSetKeyContextElement(
			StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
		if (selector != null) {
			Object key = selector.getKey(record.getValue());
			op.setCurrentKey(key);
		}
	}
}
