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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.DmlBatch;
import org.apache.flink.table.api.ResultTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DmlBatchImpl implements DmlBatch {

	private List<ModifyOperation> operations = new ArrayList<>();

	private TableEnvironmentImpl tableEnv;

	DmlBatchImpl(TableEnvironmentImpl tableEnv) {
		this.tableEnv = tableEnv;
	}

	@Override
	public void addInsert(String stmt) {
		List<Operation> operations = tableEnv.parser.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException("only single insert statement is supported");
		}

		Operation operation = operations.get(0);
		if (operation instanceof ModifyOperation) {
			this.operations.add((ModifyOperation) operation);
		} else {
			throw new TableException("only insert statement is supported");
		}
	}

	@Override
	public void addInsert(String targetPath, Table table) {
		UnresolvedIdentifier unresolvedIdentifier = tableEnv.parser.parseIdentifier(targetPath);
		ObjectIdentifier objectIdentifier = tableEnv.catalogManager.qualifyIdentifier(unresolvedIdentifier);

		operations.add(new CatalogSinkModifyOperation(
			objectIdentifier,
			table.getQueryOperation()));
	}

	@Override
	public ResultTable execute() throws Exception {
		try {
			// TODO gen job name
			return tableEnv.executeInsertInternal(operations, "sink");
		} finally {
			operations.clear();
		}
	}

	@Override
	public String explain(boolean extended) {
		return tableEnv.planner
			.explain(operations.stream().map(o -> (Operation) o).collect(Collectors.toList()), extended);
	}

}
