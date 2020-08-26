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

package org.apache.flink.table.planner.plan.multipleinput;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.plan.multipleinput.schema.TpcdsSchema;
import org.apache.flink.table.planner.plan.multipleinput.schema.TpcdsSchemaProvider;
import org.apache.flink.table.planner.plan.multipleinput.stats.TpcdsStatsProvider;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.FileUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TpcdsPlanTest extends TableTestBase {

	private static final List<String> TPCDS_TABLES = Arrays.asList(
		"catalog_sales", "catalog_returns", "inventory", "store_sales",
		"store_returns", "web_sales", "web_returns", "call_center", "catalog_page",
		"customer", "customer_address", "customer_demographics", "date_dim",
		"household_demographics", "income_band", "item", "promotion", "reason",
		"ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site"
	);

	private static final String DATA_SUFFIX = ".dat";
	private static final String COL_DELIMITER = "|";
	private static final String FILE_SEPARATOR = "/";

	private final BatchTableTestUtil util = batchTestUtil(new TableConfig());

	@Parameterized.Parameter
	public String caseName;

	@Before
	public void before() {
		TableEnvironment tEnv = util.getTableEnv();

		//config Optimizer parameters
		tEnv.getConfig().getConfiguration()
			.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
		tEnv.getConfig().getConfiguration()
			.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
		tEnv.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
		tEnv.getConfig().getConfiguration()
			.set(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "ALL_EDGES_BLOCKING");
		tEnv.getConfig().getConfiguration()
			.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, true);

		//register TPC-DS tables
		/*TPCDS_TABLES.forEach(table -> {
			TpcdsSchema schema = TpcdsSchemaProvider.getTableSchema(table);
			CsvTableSource.Builder builder = CsvTableSource.builder();
			builder.path("/tmp" + FILE_SEPARATOR + table + DATA_SUFFIX);
			for (int i = 0; i < schema.getFieldNames().size(); i++) {
				builder.field(
					schema.getFieldNames().get(i),
					TypeConversions.fromDataTypeToLegacyInfo(schema.getFieldTypes().get(i)));
			}
			builder.fieldDelimiter(COL_DELIMITER);
			builder.emptyColumnAsNull();
			builder.lineDelimiter("\n");
			CsvTableSource tableSource = builder.build();
			ConnectorCatalogTable catalogTable = ConnectorCatalogTable.source(tableSource, true);
			tEnv.getCatalog(tEnv.getCurrentCatalog()).ifPresent(catalog -> {
				try {
					catalog.createTable(new ObjectPath(tEnv.getCurrentDatabase(), table), catalogTable, false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		});

		TpcdsStatsProvider.registerTpcdsStats(tEnv);*/

		HiveCatalog hive = new HiveCatalog("myhive", "tpcds_bin_orc_10000", "/Users/tsreaper/opt/test-data/tpcds-hive-conf", "2.3.4");
		tEnv.registerCatalog("myhive", hive);
		tEnv.useCatalog("myhive");
	}

	@Test
	public void testPlan() throws Exception {
		File sqlFile = new File(TpcdsPlanTest.class.getClassLoader().getResource("org/apache/flink/table/planner/plan/multipleinput/query/query" + caseName + ".sql").getFile());
		String sql = FileUtils.readFileUtf8(sqlFile);
		util.verifyPlan(sql);

		/*TableEnvironment tEnv = util.getTableEnv();
		tEnv.executeSql(sql);*/
	}

	@Parameterized.Parameters(name = "q{0}")
	public static Collection<String> parameters() {
		return Arrays.asList(
			/*"1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
			"11", "12", "13", "14a", "14b", "15", "16", "17", "18", "19", "20",
			"21", "22", "23a", "23b", "24a", "24b", "25", "26", "27", "28", "29", "30",
			"31", "32", "33", "34", "35", "36", "37", "38", "39a", "39b", "40",
			"41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
			"51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
			"61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
			"71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
			"81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
			"91", "92", "93", "94", "95", "96", "97", "98", "99"*/"5");
	}
}
