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

import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.common.io.{DefaultInputSplitAssigner, InputFormat}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplit, InputSplitAssigner}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.mutable.Map

class TestPartitionableTableSource
  extends PartitionableTableSource
    with StreamTableSource[Row]
    with BatchTableSource[Row] {

  private val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)
  private val fieldNames = Array("id", "name", "part")
  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  private val allPartitions = Seq("part=1", "part=2", "part=3")
  private val data = Map[String, Seq[Row]](
    "part=1" -> Seq(createRow(1, "Anna", "1"), createRow(2, "Jack", "1")),
    "part=2" -> Seq(createRow(3, "John", "2"), createRow(4, "nosharp", "2")),
    "part=3" -> Seq(createRow(5, "Peter", "3"), createRow(6, "Lucy", "3"))
  )

  private def createRow(id: Int, name: String, part: String): Row = {
    val row = new Row(3)
    row.setField(0, id)
    row.setField(1, name)
    row.setField(2, part)
    row
  }

  private def getPartitionData: Array[Seq[Row]] = {
    val prunedPartitions = getPrunedPartitions
    data.filterKeys {
      key => prunedPartitions.isEmpty || prunedPartitions.get.map(_.getEntireValue).contains(key)
    }.values.toArray
  }

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.createInput(new TestPartitionInputFormat(getPartitionData), returnType)
      .setParallelism(1)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.createInput(new TestPartitionInputFormat(getPartitionData), returnType)
      .setParallelism(1)
  }

  override def getReturnType: TypeInformation[Row] = returnType

  override def getAllPartitions: Seq[Partition] = allPartitions.map(p => new TestPartition(p))

  override def getPartitionFieldNames: Array[String] = Array("part")

  override def getPartitionFieldTypes: Array[TypeInformation[_]] = {
    Array(BasicTypeInfo.STRING_TYPE_INFO)
  }

  override def supportDropPartitionPredicate: Boolean = true
}

class TestPartition(partition: String) extends Partition {

  private val kv = partition.split("=")
  private val map = Map[String, Any](kv(0) -> kv(1))

  override def getFieldValue(fieldName: String): Any = map.getOrElse(fieldName, null)

  override def getEntireValue: Any = partition
}

class TestPartitionInputFormat(data: Array[Seq[Row]]) extends InputFormat[Row, GenericInputSplit] {

  var currentSplitNumber = 0
  var currentSplitIndex = 0

  override def configure(parameters: Configuration): Unit = {}

  override def nextRecord(reuse: Row): Row = {
    val row = data(currentSplitNumber)(currentSplitIndex)
    currentSplitIndex += 1
    row
  }

  override def getInputSplitAssigner(inputSplits: Array[GenericInputSplit]): InputSplitAssigner = {
    new DefaultInputSplitAssigner(inputSplits.asInstanceOf[Array[InputSplit]])
  }

  override def reachedEnd(): Boolean = currentSplitIndex >= data(currentSplitNumber).size

  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = null

  override def close(): Unit = {}

  override def createInputSplits(minNumSplits: Int): Array[GenericInputSplit] = {
    data.zipWithIndex.map {
      case (_, index) => new GenericInputSplit(index, data.length)
    }
  }

  override def open(split: GenericInputSplit): Unit = {
    currentSplitNumber = split.getSplitNumber
    currentSplitIndex = 0
  }
}
