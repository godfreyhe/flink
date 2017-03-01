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

package org.apache.flink.table.plan.util

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.sources.Partition
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.Map

class PartitionPrunerTest {

  val partitionFieldNames = Array("part1", "part2")
  val partitionFieldTypes: Array[TypeInformation[_]] =
    Array(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
  val allPartitions = Seq(
    new TestPartition("part1=p1,part2=p2"),
    new TestPartition("part1=p3,part2=p4"),
    new TestPartition("part1=p5,part2=p6"),
    new TestPartition("part1=p7,part2=p8"))
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)
  val relBuilder = tEnv.getRelBuilder

  def getPrunedPartitions(
    allPartitions: Seq[Partition],
    partitionPredicate: Array[Expression]): Seq[Partition] = {
    PartitionPruner.DEFAULT.getPrunedPartitions(
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      partitionPredicate,
      relBuilder)
  }

  @Test
  def testEmptyPartitions(): Unit = {
    val allPartitions = Seq()
    val predicate = Array[Expression]('part1 === "p1")
    val prunedPartitions = getPrunedPartitions(allPartitions, predicate)
    assertTrue(prunedPartitions.isEmpty)
  }

  @Test
  def testOnePartition(): Unit = {
    val predicate1 = Array[Expression]('part1 === "p1")
    val prunedPartitions1 = getPrunedPartitions(allPartitions, predicate1)
    assertEquals(1, prunedPartitions1.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions1.head.getEntireValue)

    val predicate2 = Array[Expression]('part2 === "p4")
    val prunedPartitions2 = getPrunedPartitions(allPartitions, predicate2)
    assertEquals(1, prunedPartitions2.length)
    assertEquals("part1=p3,part2=p4", prunedPartitions2.head.getEntireValue)
  }

  @Test
  def testTwoPartitionAnd(): Unit = {
    val predicate = Array[Expression]('part1 === "p3", 'part2 === "p4")
    val prunedPartitions = getPrunedPartitions(allPartitions, predicate)
    assertEquals(1, prunedPartitions.length)
    assertEquals("part1=p3,part2=p4", prunedPartitions.head.getEntireValue)
  }

  @Test
  def testTwoPartitionOr(): Unit = {
    val predicate = Array[Expression]('part1 === "p1" || 'part2 === "p4")
    val prunedPartitions = getPrunedPartitions(allPartitions, predicate)
    assertEquals(2, prunedPartitions.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions.head.getEntireValue)
    assertEquals("part1=p3,part2=p4", prunedPartitions.apply(1).getEntireValue)
  }

  @Test
  def testUDF(): Unit = {
    val predicate = Array[Expression](UpperUDF('part1) === "P1" || 'part2 === "p4")
    val prunedPartitions = getPrunedPartitions(allPartitions, predicate)
    assertEquals(2, prunedPartitions.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions.head.getEntireValue)
    assertEquals("part1=p3,part2=p4", prunedPartitions.apply(1).getEntireValue)
  }

  @Test(expected = classOf[TableException])
  def testFilterFunctionWithOpen(): Unit = {
    // upper_udf_with_open(part1) = 'P1'
    val predicate = Array[Expression](UpperUDFWithOpen('part1) === "P1")
    getPrunedPartitions(allPartitions, predicate)
  }

  @Test(expected = classOf[TableException])
  def testFilterFunctionWithClose(): Unit = {
    // upper_udf_with_close(part1) = 'P1'
    val predicate = Array[Expression](UpperUDFWithClose('part1) === "P1")
    getPrunedPartitions(allPartitions, predicate)
  }

}

class TestPartition(partition: String) extends Partition {

  private val map = Map[String, Any]()
  partition.split(",").foreach {
    case p =>
      val kv = p.split("=")
      map.put(kv(0), kv(1))
  }

  override def getFieldValue(fieldName: String): Any = map.getOrElse(fieldName, null)

  override def getEntireValue: Any = partition
}

object UpperUDFWithOpen extends ScalarFunction {

  override def open(context: FunctionContext): Unit = {
    // no nothing
  }

  def eval(str: String): String = {
    str.toUpperCase()
  }
}

object UpperUDFWithClose extends ScalarFunction {

  def eval(str: String): String = {
    str.toUpperCase()
  }

  override def close(): Unit = {
    // no nothing
  }
}

object UpperUDF extends ScalarFunction {

  def eval(str: String): String = {
    str.toUpperCase()
  }
}
