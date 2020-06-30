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
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link StreamMultipleInputCreationProcessor}.
 */
public class StreamMultipleInputCreationProcessorTest extends TableTestBase {

	private StreamTableTestUtil util = streamTestUtil(new TableConfig());

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
			new String[]{"a", "b", "c"});
	}

	@Test
	public void testStarSchemaJoin() {
		String sqlQuery = "SELECT * FROM (SELECT x.a, x.b FROM x, y WHERE x.a = y.d UNION ALL (SELECT x.a, x.b FROM x, z WHERE x.a = z.a))";
		util.verifyPlan(sqlQuery);
	}
}
