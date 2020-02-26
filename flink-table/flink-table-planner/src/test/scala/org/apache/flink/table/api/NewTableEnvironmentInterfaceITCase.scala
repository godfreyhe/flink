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

package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types.{DOUBLE, INT, STRING}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.NewTableEnvironmentInterfaceITCase.{getPersonCsvTableSource, readFromResource, replaceStageId}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import _root_.java.io.{File, FileOutputStream, OutputStreamWriter}
import _root_.java.util

import _root_.scala.io.Source

@RunWith(classOf[Parameterized])
class NewTableEnvironmentInterfaceITCase(settings: EnvironmentSettings, mode: String) {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  var tEnv: TableEnvironment = _

  @Before
  def setup(): Unit = {
    mode match {
      case "TableEnvironment" =>
        tEnv = TableEnvironmentImpl.create(settings)
      case "StreamTableEnvironment" =>
        tEnv = StreamTableEnvironment.create(
          StreamExecutionEnvironment.getExecutionEnvironment, settings)
      case _ => throw new UnsupportedOperationException("unsupported mode: " + mode)
    }
  }

  @Test
  def testExecuteStatement(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    val sink3Path = registerCsvTableSink(tEnv, Array("id"), Array(INT), "MySink3")
    val sink4Path = registerCsvTableSink(tEnv, Array("score"), Array(DOUBLE), "MySink4")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyFile(sink3Path)
    checkEmptyFile(sink4Path)

    val result1 = tEnv.executeStatement("insert into MySink1 select first from MyTable")
    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    assertFirstValues(sink1Path)
    checkEmptyFile(sink2Path)

    val row1 = result1.getResultRows.iterator().next()
    assertEquals(-1L, row1.getField(0))

    // delete first csv file
    new File(sink1Path).delete()
    assertFalse(new File(sink1Path).exists())

    val result2 = tEnv.executeStatement("insert into MySink2 select last from MyTable")
    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    assertFalse(new File(sink1Path).exists())
    assertLastValues(sink2Path)

    val row2 = result2.getResultRows.iterator().next()
    assertEquals(-1L, row2.getField(0))
  }

  @Test
  def testDmlBatchExecute(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val result = batch.execute()
    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    checkEmptyOperationInTableEnvironment(tEnv)

    val row1 = result.getResultRows.iterator().next()
    assertEquals(-1L, row1.getField(0))
    assertEquals(-1L, row1.getField(1))

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testDmlBatchExplain(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val explain = batch.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testDmlBatchExplain.out")),
      replaceStageId(explain))

    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    checkEmptyOperationInTableEnvironment(tEnv)
  }

  @Test
  def testExplainTwice(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val explain1 = batch.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testDmlBatchExplain.out")),
      replaceStageId(explain1))
    val explain2 = batch.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testDmlBatchExplain.out")),
      replaceStageId(explain2))

    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    checkEmptyOperationInTableEnvironment(tEnv)
  }

  @Test
  def testNewExecuteAndOldExecute(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    val sink3Path = registerCsvTableSink(tEnv, Array("id"), Array(INT), "MySink3")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyFile(sink3Path)

    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val result = batch.execute()
    val row1 = result.getResultRows.iterator().next()
    assertEquals(-1L, row1.getField(0))
    assertEquals(-1L, row1.getField(1))

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)

    deleteFile(sink1Path)
    deleteFile(sink2Path)
    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)

    val table3 = tEnv.sqlQuery("select id from MyTable")
    tEnv.insertInto(table3, "MySink3")
    tEnv.execute("test3")

    assertFileNotExist(sink1Path)
    assertFileNotExist(sink2Path)
  }

  @Test
  def testOldExecuteAndNewExecute(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    val sink3Path = registerCsvTableSink(tEnv, Array("id"), Array(INT), "MySink3")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyFile(sink3Path)

    val table3 = tEnv.sqlQuery("select id from MyTable")
    tEnv.insertInto(table3, "MySink3")
    tEnv.execute("test3")

    assertIdValues(sink3Path)
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    deleteFile(sink3Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val result = batch.execute()
    val row1 = result.getResultRows.iterator().next()
    assertEquals(-1L, row1.getField(0))
    assertEquals(-1L, row1.getField(1))

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
    assertFileNotExist(sink3Path)
  }

  @Test
  def testInsertIntoAndNewExecute(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    val sink3Path = registerCsvTableSink(tEnv, Array("id"), Array(INT), "MySink3")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyFile(sink3Path)

    val table3 = tEnv.sqlQuery("select id from MyTable")
    tEnv.insertInto(table3, "MySink3")
    val explain = tEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testInsertIntoAndNewExecute.out")),
      replaceStageId(explain))

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val result = batch.execute()
    val row1 = result.getResultRows.iterator().next()
    assertEquals(-1L, row1.getField(0))
    assertEquals(-1L, row1.getField(1))

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
    checkEmptyFile(sink3Path)

    deleteFile(sink1Path)
    deleteFile(sink2Path)

    tEnv.execute("test3")
    assertFileNotExist(sink1Path)
    assertFileNotExist(sink2Path)
    assertIdValues(sink3Path)
  }

  @Test
  def testNewExplainAndOldExplain(): Unit = {
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    val sink3Path = registerCsvTableSink(tEnv, Array("id"), Array(INT), "MySink3")
    val sink4Path = registerCsvTableSink(tEnv, Array("score"), Array(DOUBLE), "MySink4")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)
    checkEmptyFile(sink3Path)
    checkEmptyFile(sink4Path)

    val batch = tEnv.createDmlBatch()
    batch.addInsert("insert into MySink1 select first from MyTable")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    batch.addInsert("MySink2", table2)

    val explain1 = batch.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testDmlBatchExplain.out")),
      replaceStageId(explain1))
    val explain2 = batch.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testDmlBatchExplain.out")),
      replaceStageId(explain2))

    checkEmptyTransformationsInStreamExecutionEnvironment(tEnv)
    checkEmptyOperationInTableEnvironment(tEnv)

    val table3 = tEnv.sqlQuery("select id from MyTable")
    tEnv.insertInto(table3, "MySink3")
    val table4 = tEnv.sqlQuery("select score from MyTable")
    tEnv.insertInto(table4, "MySink4")
    val explain = tEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testNewExplainAndOldExplain.out")),
      replaceStageId(explain))
  }

  @Test
  def testSqlUpdateAndToDataStream(): Unit = {
    if (!mode.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamTableEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    resultSet.addSink(new StreamITCase.StringSink[Row])

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    assertTrue(StreamITCase.testResults.isEmpty)

    deleteFile(sink1Path)

    streamEnv.execute("test2")
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testToDataStreamAndSqlUpdate(): Unit = {
    if (!mode.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamTableEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    resultSet.addSink(new StreamITCase.StringSink[Row])

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamEnv.execute("test2")
    checkEmptyFile(sink1Path)
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
    StreamITCase.testResults.clear()

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    assertTrue(StreamITCase.testResults.isEmpty)
  }

  private def checkEmptyTransformationsInStreamExecutionEnvironment(
      tEnv: TableEnvironment): Unit = {
    try {
      // getStreamGraph will check buffered transformation first
      tEnv.asInstanceOf[TableEnvironmentImpl]
        .getPlanner.asInstanceOf[StreamPlanner]
        .getExecutionEnvironment
        .getStreamGraph
      assertTrue(false)
    } catch {
      case e: IllegalStateException =>
        assertEquals("No operators defined in streaming topology. Cannot execute.", e.getMessage)
      case _ => assertTrue(false)
    }
  }

  private def checkEmptyOperationInTableEnvironment(tEnv: TableEnvironment): Unit = {
    if (!tEnv.isInstanceOf[StreamTableEnvironment]) {
      try {
        // explain will check buffered operation first
        tEnv.explain(false)
        assertTrue(false)
      } catch {
        case e: IllegalArgumentException =>
          assertTrue(e.getMessage.contains("operations should not be empty"))
        case _ => assertTrue(false)
      }
    }
  }

  private def registerCsvTableSink(
      tEnv: TableEnvironment,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableName: String): String = {
    val resultFile = _tempFolder.newFile()
    val path = resultFile.getAbsolutePath

    val configuredSink = new CsvTableSink(path, ",", 1, WriteMode.OVERWRITE)
      .configure(fieldNames, fieldTypes)
    tEnv.registerTableSink(tableName, configuredSink)

    path
  }

  private def assertFirstValues(csvFilePath: String): Unit = {
    val expected = List("Mike", "Bob", "Sam", "Peter", "Liz", "Sally", "Alice", "Kelly")
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(expected.sorted, actual.sorted)
  }

  private def assertLastValues(csvFilePath: String): Unit = {
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(getExpectedLastValues.sorted, actual.sorted)
  }

  private def getExpectedLastValues: List[String] = {
    List("Smith", "Taylor", "Miller", "Smith", "Williams", "Miller", "Smith", "Williams")
  }

  private def assertIdValues(csvFilePath: String): Unit = {
    val expected = List("1", "2", "3", "4", "5", "6", "7", "8")
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(expected.sorted, actual.sorted)
  }

  private def checkEmptyFile(csvFilePath: String): Unit = {
    assertTrue(FileUtils.readFileUtf8(new File(csvFilePath)).isEmpty)
  }

  private def deleteFile(path: String): Unit = {
    new File(path).delete()
    assertFalse(new File(path).exists())
  }

  private def assertFileNotExist(path: String): Unit = {
    assertFalse(new File(path).exists())
  }

}

object NewTableEnvironmentInterfaceITCase {
  @Parameterized.Parameters(name = "{1}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array(EnvironmentSettings.newInstance().useOldPlanner().build(), "TableEnvironment"),
      Array(EnvironmentSettings.newInstance().useOldPlanner().build(), "StreamTableEnvironment")
    )
  }

  def readFromResource(path: String): String = {
    val inputStream = getClass.getResource(path).getFile
    Source.fromFile(inputStream).mkString
  }

  def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }

  def getPersonCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"),
      "csv-test",
      "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("first", Types.STRING)
      .field("id", Types.INT)
      .field("score", Types.DOUBLE)
      .field("last", Types.STRING)
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .ignoreFirstLine()
      .commentPrefix("%")
      .build()
  }

  private def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}
