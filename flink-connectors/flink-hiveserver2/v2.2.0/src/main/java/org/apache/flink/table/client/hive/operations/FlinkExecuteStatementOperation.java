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

package org.apache.flink.table.client.hive.operations;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.session.HiveSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Flink ExecuteStatementOperation for DQLs, DMLs and DDLs.
 */
public class FlinkExecuteStatementOperation extends ExecuteStatementOperation {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkExecuteStatementOperation.class);

	private final Executor executor;

	private String statementId;

	private Table table;
	private TableSchema tableSchema;
	private List<Row> resultList;
	private Iterator<Row> iter;

	public FlinkExecuteStatementOperation(
			HiveSession parentSession,
			String statement,
			Map<String, String> confOverlay,
			Boolean runInBackground,
			Executor executor) {
		super(parentSession, statement, confOverlay, runInBackground);
		this.executor = executor;
	}

	@Override
	protected void runInternal() throws HiveSQLException {
		setState(OperationState.PENDING);
		setHasResultSet(true);

		if (!shouldRunAsync()) {
			execute();
		} else {
			final UserGroupInformation currentUGI = getCurrentUGI();

			// Runnable impl to call runInternal asynchronously
			// from a different thread
			Runnable backgroundOperation = new Runnable() {
				@Override
				public void run() {
					PrivilegedExceptionAction<Object> doAsAction = new PrivilegedExceptionAction<Object>() {
						@Override
						public Object run() throws HiveSQLException {
							registerCurrentOperationLog();

							try {
								execute();
							} catch (HiveSQLException e) {
								setOperationException(e);
								LOG.error("Error running SQL query: ", e);
							} finally {
								// FIXME
//								unregisterOperationLog();
							}
							return null;
						}
					};

					try {
						currentUGI.doAs(doAsAction);
					} catch (Exception e) {
						setOperationException(new HiveSQLException(e));
						LOG.error("Error running SQL query as user : " + currentUGI.getShortUserName(), e);
					}
				}
			};

			try {
				// This submit blocks if no background threads are available to run this operation
				Future<?> backgroundHandle =
					getParentSession().getSessionManager().submitBackgroundOperation(backgroundOperation);
				setBackgroundHandle(backgroundHandle);
			} catch (RejectedExecutionException rejected) {
				setState(OperationState.ERROR);
				throw new HiveSQLException("The background threadpool connot accept" +
					" new task for execution, please retry the operation", rejected);
			}
		}
	}

	@Override
	public void cancel(OperationState operationState) throws HiveSQLException {

	}

	@Override
	public org.apache.hive.service.cli.TableSchema getResultSetSchema() throws HiveSQLException {
		if (table == null || tableSchema == null || tableSchema.getFieldCount() == 0) {
			return new org.apache.hive.service.cli.TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")));
		} else {
			final String[] fieldNames = tableSchema.getFieldNames();
			final DataType[] fieldTypes = tableSchema.getFieldDataTypes();
			final List<FieldSchema> schema =
				IntStream
				.range(0, tableSchema.getFieldCount())
				.mapToObj((i) -> {
					final String hiveTypeString = fieldTypes[i].toString();
					return new FieldSchema(fieldNames[i], hiveTypeString, "");
				}).collect(Collectors.toList());
			return new org.apache.hive.service.cli.TableSchema(schema);
		}
	}

	@Override
	public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
		validateDefaultFetchOrientation(orientation);
		assertState(Arrays.asList(OperationState.FINISHED));
		setHasResultSet(true);

		RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion(), false);

		if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
			iter = resultList.iterator();
		}

		if (!iter.hasNext()) {
			return rowSet;
		} else {
			long curRow = 0;
			while (curRow < maxRows && iter.hasNext()) {
				final Row row = iter.next();
				final List<Object> fields =
					IntStream
						.range(0, row.getArity())
						.mapToObj(i -> row.getField(i))
						.collect(Collectors.toList());
				rowSet.addRow(fields.toArray());
				curRow += 1;
			}
			return rowSet;
		}
	}

	@Override
	public void close() throws HiveSQLException {
		LOG.info("CLOSING " + statementId);
		cleanup(OperationState.CLOSED);
		cleanupOperationLog();
	}

	private void execute() throws HiveSQLException {
		statementId = UUID.randomUUID().toString();
		final String statement = getStatement();
		LOG.info("execute statement " + statement + " with " + statementId);
		setState(OperationState.RUNNING);

		try {
			String sessionId = parentSession.getSessionHandle().getSessionId().toString();
			ResultDescriptor result = executor.executeQuery(sessionId, statement);

			// TODO ???
			TypedResult<Integer> typedResult =
					executor.snapshotResult(sessionId, result.getResultId(), Integer.MAX_VALUE);

			// stop retrieval if job is done
			if (typedResult.getType() == TypedResult.ResultType.EOS) {
				resultList = new ArrayList<>();
			}
			// update page
			else if (typedResult.getType() == TypedResult.ResultType.PAYLOAD) {
				int newPageCount = typedResult.getPayload();
				resultList = executor.retrieveResultPage(result.getResultId(), newPageCount);
			}

			iter = resultList.iterator();
			setState(OperationState.FINISHED);
		} catch (SqlExecutionException e) {
			if (getStatus().getState() == OperationState.CANCELED) {
				return;
			} else {
				setState(OperationState.ERROR);
				throw e;
			}
		} catch (Exception e) {
			LOG.error("Error executing query: ", e);
			setState(OperationState.ERROR);
			throw new HiveSQLException(e);
		}
	}

	private UserGroupInformation getCurrentUGI() throws HiveSQLException {
		try {
			return Utils.getUGI();
		} catch (Exception e) {
			throw new HiveSQLException("Unable to get current user", e);
		}
	}

	private void registerCurrentOperationLog() {
		if (isOperationLogEnabled) {
			if (operationLog == null) {
				LOG.warn("Failed to get current OperationLog object of Operation: " +
					getHandle().getHandleIdentifier());
				isOperationLogEnabled = false;
				return;
			}
			// FIXME
//			OperationLog.setCurrentOperationLog(operationLog);
		}
	}

	private void cleanup(OperationState state) throws HiveSQLException {
		setState(state);
		if (shouldRunAsync()) {
			Future<?> backgroundHandle = getBackgroundHandle();
			if (backgroundHandle != null) {
				backgroundHandle.cancel(true);
			}
		}

		// TODO: should cleanup running job
	}
}
