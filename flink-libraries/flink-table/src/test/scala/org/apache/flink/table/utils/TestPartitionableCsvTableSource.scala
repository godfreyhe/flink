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

package org.apache.flink.table.utils

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.plan.rules.util.PartitionPruner
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.mutable.{ListBuffer, Map}

class TestPartitionableCsvTableSource
  extends BatchTableSource[Row]
    with StreamTableSource[Row]
    with PartitionableTableSource {

  private val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)
  private val fieldNames = Array("id", "name", "part")
  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  private val allPartitions = Seq("part=1", "part=2", "part=3")
  private var prunedPartitions: Option[Seq[String]] = None

  private val partition2FileMap = Map[String, String](
    "part=1" -> writeRecords(Seq("1,Anna,1", "2,Jack,1"), "1"),
    "part=2" -> writeRecords(Seq("3,John,2", "4,nosharp,2"), "2"),
    "part=3" -> writeRecords(Seq("5,Peter,3", "6,Lucy,3"), "3")
  )

  private def getPartitionFiles: Seq[String] = {
    val files = ListBuffer[String]()
    partition2FileMap.filterKeys {
      k => prunedPartitions.isEmpty || prunedPartitions.get.contains(k)
    }.values.foreach(files += _)
    files.toList
  }

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    val files = getPartitionFiles.toArray
    execEnv.createInput(new TestPartitionRowCsvInputFormat(files, fieldTypes), returnType)
      .setParallelism(1)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val files = getPartitionFiles.toArray
    execEnv.createInput(new TestPartitionRowCsvInputFormat(files, fieldTypes), returnType)
      .setParallelism(1)
  }

  override def getReturnType: TypeInformation[Row] = returnType

  override def getAllPartitions: Seq[Partition] = {
    allPartitions.map(p => new TestPartition(p))
  }

  override def getPartitionPruner: Option[PartitionPruner] = Some(PartitionPruner.DEFAULT)

  override def getPartitionFieldNames: Array[String] = Array("part")

  override def setPrunedPartitions(partitions: Seq[Partition]): Unit = {
    prunedPartitions = Some(partitions.map(p => p.getEntireValue).asInstanceOf[Seq[String]])
  }

  def getPrunedPartitions: Option[Seq[String]] = prunedPartitions

  private def writeRecords(records: Seq[String], postfix: String): String = {
    val tempFile = File.createTempFile(s"csv-test-$postfix-", ".tmp")
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8")
    tmpWriter.write(records.mkString("\n"))
    tmpWriter.close()
    tempFile.getCanonicalPath
  }
}

class TestPartition(partition: String) extends Partition {

  private val kv = partition.split("=")
  private val map = Map[String, Any](kv(0) -> kv(1))

  override def getFieldValue(fieldName: String): Any = map.getOrElse(fieldName, null)

  override def getEntireValue: Any = partition
}

class TestPartitionRowCsvInputFormat(
  files: Array[String],
  fieldTypes: Array[TypeInformation[_]]) extends RowCsvInputFormat(new Path("/tmp"), fieldTypes) {

  override def createInputSplits(minNumSplits: Int): Array[FileInputSplit] = {
    files.zipWithIndex.map {
      case (file, index) => new FileInputSplit(index, new Path(file), 0, -1, null)
    }
  }
}

