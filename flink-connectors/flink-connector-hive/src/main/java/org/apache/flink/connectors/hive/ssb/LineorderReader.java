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

package org.apache.flink.connectors.hive.ssb;

import org.apache.flink.connectors.hive.read.SplitReader;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Random;

/**
 * Lineorder reader.
 */
public class LineorderReader implements SplitReader {
	private static final Logger LOG = LoggerFactory.getLogger(LineorderReader.class);

	private static final int TOTAL_NUMBER = 600000000;
	private static final int TOTAL_NUMBER_PER_SPLIT = TOTAL_NUMBER / 256;
	private final int[] selectedFields;
	private int startNumber;
	private final int stopNumber;
	private Random random = new Random();

	private static final String[] ORDER_PRIORITY = { "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW" };
	private static final String[] SHIP_MODE = { "AIR", "FOB", "REG AIR", "MAIL", "TRUCK", "SHIP", "RAIL" };

	public LineorderReader(int splitNumber, int[] selectedFields) {
		this.selectedFields = selectedFields;
		startNumber = splitNumber * TOTAL_NUMBER_PER_SPLIT;
		stopNumber = startNumber + TOTAL_NUMBER_PER_SPLIT;
	}

	public static void main(String[] args) throws IOException {
		int[] selectedFields = new int[] { 2, 4, 5, 12 };
		LogicalType[] types = new LogicalType[]{ new IntType(), new IntType(), new DateType(), new IntType() };
		LineorderReader reader = new LineorderReader(1, selectedFields);
		BaseRow reuse = new BinaryRow(selectedFields.length);
		BaseRow row = null;
		long startTime = System.currentTimeMillis();
		while (!reader.reachedEnd()) {
			row = reader.nextRecord(reuse);
		}

		System.out.println(((BinaryRow) row).toOriginString(types));
		long endTime = System.currentTimeMillis();
		System.out.println("cost: " + (endTime - startTime));
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return startNumber >= stopNumber;
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) throws IOException {
		if (reachedEnd()) {
			return null;
		}
		final BinaryRow row = reuse instanceof BinaryRow ?
				(BinaryRow) reuse : new BinaryRow(selectedFields.length);

		BinaryRowWriter writer = new BinaryRowWriter(row, 20);

		startNumber += 1;

		for (int i = 0; i < selectedFields.length; ++i) {
			switch (selectedFields[i]) {
				case 0:
					// lo_orderkey
					writer.writeInt(i, startNumber);
					break;
				case 1:
					//lo_linenumber
					writer.writeByte(i, (byte) random.nextInt());
					break;
				case 2:
					//lo_custkey
					writer.writeInt(i, random.nextInt());
					break;
				case 3:
					//lo_partkey
					writer.writeInt(i, random.nextInt());
					break;
				case 4:
					//lo_suppkey
					writer.writeInt(i, random.nextInt());
					break;
				case 5:
					//lo_orderdate
					writer.writeInt(i, SqlDateTimeUtils.localDateToUnixDate(LocalDate.of(2020, 3, 25)));
					break;
				case 6:
					//lo_orderpriority
					writer.writeBinary(i, ORDER_PRIORITY[random.nextInt(ORDER_PRIORITY.length)].getBytes());
					break;
				case 7:
					//lo_shippriority
					writer.writeByte(i, (byte) 0);
					break;
				case 8:
					//lo_quantity
					writer.writeByte(i, (byte) (1 + random.nextInt(50)));
					break;
				case 9:
					//lo_extendedprice
					writer.writeInt(i, 1 + random.nextInt(55450));
					break;
				case 10:
					//lo_ordtotalprice
					writer.writeInt(i, 1 + random.nextInt(388000));
					break;
				case 11:
					//lo_discount
					writer.writeByte(i, (byte) (random.nextInt(11)));
					break;
				case 12:
					//lo_revenue ?
					writer.writeInt(i, random.nextInt());
					break;
				case 13:
					//lo_supplycost
					writer.writeInt(i, random.nextInt());
					break;
				case 14:
					//lo_tax ?
					writer.writeByte(i, (byte) (random.nextInt()));
					break;
				case 15:
					//lo_commitdate
					writer.writeInt(i, SqlDateTimeUtils.localDateToUnixDate(LocalDate.of(2020, 3, 25)));
					break;
				case 16:
					//lo_shipmode
					writer.writeBinary(i, SHIP_MODE[random.nextInt(SHIP_MODE.length)].getBytes());
					break;
			}
		}
		writer.complete();
		return row;
	}

	@Override
	public void close() throws IOException {

	}

}
