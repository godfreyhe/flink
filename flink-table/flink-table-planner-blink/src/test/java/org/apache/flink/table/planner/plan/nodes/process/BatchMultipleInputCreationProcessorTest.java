/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.process;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link BatchMultipleInputCreationProcessor}.
 */
public class BatchMultipleInputCreationProcessorTest extends TableTestBase {

	private BatchTableTestUtil util = batchTestUtil(new TableConfig());

	@Before
	public void before() {
		util.addTableSource(
			"x",
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[]{"a", "b", "c"});
		util.addTableSource(
			"y",
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[]{"d", "e", "f"});
		util.addTableSource(
			"z",
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[]{"g", "h", "i"});

		util.addTableSource(
			"T1",
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[]{"a", "b", "c"});
		util.addTableSource(
			"T2",
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[]{"a", "b", "c"});
	}

	@Test
	public void testStarSchemaJoin() {
		util.tableEnv().getConfig().getConfiguration().setBoolean(
			OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true);
		util.tableEnv().getConfig().getConfiguration().setString(
			ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin");

		String sqlQuery = "SELECT * FROM (SELECT * FROM x, y WHERE x.a = y.d) T1, (SELECT * FROM x, z WHERE x.a = z.g) T2 WHERE T1.a = T2.a AND T1.b > T2.b";
		util.verifyPlan(sqlQuery);
	}

	@Test
	public void myTest() {
		String query1 = "SELECT SUM(a) AS s1, b AS b1 FROM T1 group by b";
		String query2 = "SELECT SUM(a) AS s2, b AS b2 FROM T2 group by b";
		String query = "SELECT s1, s2, b1 FROM (" + query1 + ") RIGHT JOIN (" + query2 + ") ON b1 = b2";
		util.verifyPlan(query);
	}
}
