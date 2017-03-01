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

package org.apache.flink.table.api.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.{CommonTestData, TestPartitionableTableSource}
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableSourceITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testCsvTableSource(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerTableSource("csvTable", csvTable)
    val results = tEnv.sql(
      "SELECT id, first, last, score FROM csvTable").collect()

    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCsvTableSourceWithProjection(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .where('score < 20)
      .select('last, 'id.floor(), 'score * 2)
      .collect()

    val expected = Seq(
      "Smith,1,24.6",
      "Miller,3,15.78",
      "Smith,4,0.24",
      "Miller,6,13.56",
      "Williams,8,4.68").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNestedBatchTableSourceSQL(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val nestedTable = CommonTestData.getNestedTableSource

    tableEnv.registerTableSource("NestedPersons", nestedTable)

    val result = tableEnv.sql("SELECT NestedPersons.firstName, NestedPersons.lastName," +
        "NestedPersons.address.street, NestedPersons.address.city AS city " +
        "FROM NestedPersons " +
        "WHERE NestedPersons.address.city LIKE 'Dublin'").collect()

    val expected = "Bob,Taylor,Pearse Street,Dublin"

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testPartitionableTableSource(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)

    val results = tEnv.scan("csv_table")
      .where('part === "2" || 'part === "1" && 'id > 2)
      .collect()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=1", prunedPartitions.head.getEntireValue)
    assertEquals("part=2", prunedPartitions(1).getEntireValue)

    val expected = Seq("3,John,2", "4,nosharp,2").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPartitionableTableSourceWithoutPartitionFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)

    val results = tEnv.scan("csv_table")
      .where('name === "Lucy")
      .collect()

    assertTrue(table.getPrunedPartitions.isEmpty)

    val expected = Seq("6,Lucy,3").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // FIXME can't find function in Catalog when converting expression to RexNode in RexCallWrapper
  @Test(expected = classOf[ValidationException])
  def testPartitionableTableSourceAPIWithUDF(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)
    val strToInt = new StrToInt
    tEnv.registerFunction("str_to_int", strToInt)

    val results = tEnv.scan("csv_table")
      .where(strToInt('part) >= 2 && 'id < 5)
      .collect()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=2", prunedPartitions.head.getEntireValue)
    assertEquals("part=3", prunedPartitions(1).getEntireValue)

    val expected = Seq("3,John,2", "4,nosharp,2").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // FIXME can't find function in Catalog when converting expression to RexNode in RexCallWrapper
  @Test(expected = classOf[ValidationException])
  def testPartitionableTableSourceSQLWithUDF(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = new TestPartitionableTableSource
    tEnv.registerTableSource("csv_table", table)
    tEnv.registerFunction("str_to_int", new StrToInt)

    val results = tEnv.sql("select * from csv_table where str_to_int(part) >= 2 and id < 5")
      .collect()

    assertTrue(table.getPrunedPartitions.isDefined)
    val prunedPartitions = table.getPrunedPartitions.get
    assertEquals(2, prunedPartitions.size)
    assertEquals("part=2", prunedPartitions.head.getEntireValue)
    assertEquals("part=3", prunedPartitions(1).getEntireValue)

    val expected = Seq("3,John,2", "4,nosharp,2").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}

class StrToInt extends ScalarFunction {
  def eval(str: String): Int = {
    str.toInt
  }
}
