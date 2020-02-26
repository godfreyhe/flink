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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.executor.StreamExecutor;
import org.apache.flink.table.planner.delegation.ExecutorBase;

import java.util.List;

public class ExecutorWrapper implements Executor {

	private final Executor realExecutor;

	private StreamGraph streamGraph;

	public ExecutorWrapper(Executor realExecutor) {
		this.realExecutor = realExecutor;
	}

	@Override
	public JobExecutionResult execute(
		List<Transformation<?>> transformations, String jobName) throws Exception {
		if (isBlinkPlanner(realExecutor.getClass())) {
			streamGraph = ((ExecutorBase) realExecutor).getStreamGraph(transformations, jobName);
		} else if (realExecutor instanceof StreamExecutor) {
			streamGraph = ((StreamExecutor) realExecutor).getStreamGraph(transformations, jobName);
		} else {
			throw new SqlClientException("unsupported executor:" + realExecutor);
		}
		return null;
	}

	private boolean isBlinkPlanner(Class<? extends Executor> executorClass) {
		try {
			return ExecutorBase.class.isAssignableFrom(executorClass);
		} catch (NoClassDefFoundError ignore) {
			// blink planner might not be on the class path
			return false;
		}
	}

	public StreamGraph getStreamGraph() {
		return streamGraph;
	}
}
