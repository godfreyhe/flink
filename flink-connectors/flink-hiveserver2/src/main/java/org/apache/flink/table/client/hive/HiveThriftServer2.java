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

import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Flink SQL port of HiveServer2.
 */
public class HiveThriftServer2 extends HiveServer2 implements ReflectedCompositeService {

	private static final Logger LOG = LoggerFactory.getLogger(HiveThriftServer2.class);
	private final CliOptions cliOptions;

	public HiveThriftServer2(CliOptions cliOptions) {
		super();
		this.cliOptions = cliOptions;
	}

	public CliOptions getCliOptions() {
		return cliOptions;
	}

	@Override
	public synchronized void init(HiveConf hiveConf) {
		FlinkCLIService cliService = new FlinkCLIService(this);
		ReflectionUtils.setSuperField(this, "cliService", cliService);
		addService(cliService);
		ThriftCLIService thriftCLIService =
				HiveServer2Shim.createThriftCLIService(cliService, HiveServer2.isHTTPTransportMode(hiveConf));
		ReflectionUtils.setSuperField(this, "thriftCLIService", thriftCLIService);
		addService(thriftCLIService);

		initCompositeService(hiveConf);
	}

	public static void main(String[] args) {
		LOG.info("Start Flink ThriftServer!");

		CliOptions cliOptions = CliOptionsParser.parseGatewayModeGateway(args);

		if (cliOptions.isPrintHelp()) {
			HiveServer2.main(args);
			return;
		}

		// TODO: parse args and do something
		HiveConf hiveConf = new HiveConf();
		hiveConf.setBoolean("datanucleus.schema.autoCreateTables", true);
		hiveConf.setBoolean("hive.metastore.schema.verification.record.version", false);
		HiveThriftServer2 server = new HiveThriftServer2(cliOptions);
		server.init(hiveConf);
		server.start();
	}
}
