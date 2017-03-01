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

package org.apache.flink.table.api.scala.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.{CommonTestData, TestPartitionableTableSource}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableSourceITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testCsvTableSourceSQL(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    tEnv.registerTableSource("persons", csvTable)

    tEnv.sql(
      "SELECT id, first, last, score FROM persons WHERE id < 4 ")
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSourceTableAPI(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource
    StreamITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.scan("csvTable")
      .where('id > 4)
      .select('last, 'score * 2)
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "Williams,69.0",
      "Miller,13.56",
      "Smith,180.2",
      "Williams,4.68")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSourceWithFilterable(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource(tableName, CommonTestData.getFilterableTableSource)
    tEnv.scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPartitionableTableSource(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)

    tEnv.scan("csv_table")
      .where('part === "2" || 'part === "1" && 'id > 2)
      .addSink(new StreamITCase.StringSink)

    env.execute()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=1", prunedPartitions.head.getEntireValue)
    assertEquals("part=2", prunedPartitions(1).getEntireValue)

    val expected = mutable.MutableList("3,John,2", "4,nosharp,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPartitionPruningRuleNotApplied(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)

    tEnv.scan("csv_table")
      .where('name === "Lucy")
      .addSink(new StreamITCase.StringSink)

    env.execute()

    assertTrue(table.getPrunedPartitions.isEmpty)

    val expected = mutable.MutableList("6,Lucy,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // FIXME can't find function in Catalog when converting expression to RexNode in RexCallWrapper
  @Test(expected = classOf[ValidationException])
  def testPartitionableTableSourceAPIWithUDF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)
    val strToInt = new StrToInt
    tEnv.registerFunction("str_to_int", strToInt)

    tEnv.scan("csv_table")
      .where(strToInt('part) >= 2 && 'id < 5)
      .addSink(new StreamITCase.StringSink)

    env.execute()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=2", prunedPartitions.head.getEntireValue)
    assertEquals("part=3", prunedPartitions(1).getEntireValue)

    val expected = mutable.MutableList("3,John,2", "4,nosharp,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // FIXME can't find function in Catalog when converting expression to RexNode in RexCallWrapper
  @Test(expected = classOf[ValidationException])
  def testPartitionableTableSourceSQLWithUDF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)
    tEnv.registerFunction("str_to_int", new StrToInt)

    tEnv.sql("select * from csv_table where str_to_int(part) >= 2 and id < 5")
      .addSink(new StreamITCase.StringSink)

    env.execute()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=2", prunedPartitions.head.getEntireValue)
    assertEquals("part=3", prunedPartitions(1).getEntireValue)

    val expected = mutable.MutableList("3,John,2", "4,nosharp,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

class StrToInt extends ScalarFunction {
  def eval(str: String): Int = {
    str.toInt
  }
}
