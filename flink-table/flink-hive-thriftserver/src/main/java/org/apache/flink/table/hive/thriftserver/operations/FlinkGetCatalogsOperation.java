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

package org.apache.flink.table.hive.thriftserver.operations;

import org.apache.flink.table.client.gateway.Executor;

import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.session.HiveSession;

import java.util.List;

/**
 * FlinkGetCatalogsOperation.
 */
public class FlinkGetCatalogsOperation extends GetCatalogsOperation {
	private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
			.addStringColumn("TABLE_CAT", "Catalog name. NULL if not applicable.");
	private final Executor executor;
	private final RowSet rowSet;

	public FlinkGetCatalogsOperation(HiveSession parentSession, Executor executor) {
		super(parentSession);
		this.executor = executor;
		rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion());
	}

	@Override
	public void runInternal() throws HiveSQLException {
		setState(OperationState.RUNNING);
		try {
			String sessionId = getParentSession().getSessionHandle().getSessionId().toString();
			List<String> catalogs = executor.listCatalogs(sessionId);
			rowSet.addRow(catalogs.toArray());
			setState(OperationState.FINISHED);
		} catch (HiveSQLException e) {
			setState(OperationState.ERROR);
			throw e;
		}
	}

	@Override
	public TableSchema getResultSetSchema() throws HiveSQLException {
		return RESULT_SET_SCHEMA;
	}

	@Override
	public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
		assertState(OperationState.FINISHED);
		validateDefaultFetchOrientation(orientation);
		if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
			rowSet.setStartOffset(0);
		}
		return rowSet.extractSubset((int) maxRows);
	}
}
