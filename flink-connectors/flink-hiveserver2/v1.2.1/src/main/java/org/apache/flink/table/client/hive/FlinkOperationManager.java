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

package org.apache.flink.table.client.hive;

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.hive.operations.FlinkExecuteStatementOperation;
import org.apache.flink.table.client.hive.operations.FlinkGetCatalogsOperation;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Flink OperationManager.
 */
public class FlinkOperationManager extends OperationManager {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkOperationManager.class);

	final Map<OperationHandle, Operation> handleToOperation = ReflectionUtils.getSupperField(this, "handleToOperation");
	final Executor executor;

	public FlinkOperationManager(Executor executor) {
		LOG.info("Initializing FlinkOperationManager");
		this.executor = executor;
	}

	@Override
	public ExecuteStatementOperation newExecuteStatementOperation(
			HiveSession parentSession,
			String statement,
			Map<String, String> confOverlay,
			boolean runAsync) throws HiveSQLException {
		LOG.info("Receive new execution statement: " + statement);

		final ExecuteStatementOperation operation = new FlinkExecuteStatementOperation(
				parentSession, statement, confOverlay, runAsync, executor);
		handleToOperation.put(operation.getHandle(), operation);
		return operation;
	}

	@Override
	public GetCatalogsOperation newGetCatalogsOperation(HiveSession parentSession) {
		LOG.info("Receive new getCatalogs operation");
		FlinkGetCatalogsOperation operation = new FlinkGetCatalogsOperation(parentSession, executor);
		handleToOperation.put(operation.getHandle(), operation);
		return operation;
	}

	@Override
	public GetSchemasOperation newGetSchemasOperation(HiveSession parentSession, String catalogName, String schemaName) {
		LOG.info("Receive new getSchemas operation");
		return super.newGetSchemasOperation(parentSession, catalogName, schemaName);
	}

	@Override
	public MetadataOperation newGetTablesOperation(HiveSession parentSession, String catalogName, String schemaName, String tableName, List<String> tableTypes) {
		LOG.info("Receive new getTables operation: " + catalogName + "." + schemaName + "." + tableName);
		return super.newGetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes);
	}
}
