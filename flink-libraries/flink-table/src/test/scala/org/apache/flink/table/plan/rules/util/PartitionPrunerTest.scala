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

package org.apache.flink.table.plan.rules.util

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, VARCHAR}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.sources.Partition
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.Map

class PartitionPrunerTest {

  private val allFieldNames = List("id", "name", "part1", "part2")
  private val allFieldTypes = List(BIGINT, VARCHAR, VARCHAR, VARCHAR)
  private val partitionFieldNames = Array("part1", "part2")
  val allPartitions = Seq(
    new TestPartition("part1=p1,part2=p2"),
    new TestPartition("part1=p3,part2=p4"),
    new TestPartition("part1=p5,part2=p6"),
    new TestPartition("part1=p7,part2=p8"))

  val util = new RexNodeTestUtil(allFieldNames, allFieldTypes)

  def getPrunedPartitions(
    allPartitions: Seq[Partition],
    partitionFilter: RexNode): Seq[Partition] = {
    PartitionPruner.DEFAULT.getPrunedPartitions(
      partitionFieldNames,
      util.rowType,
      allPartitions,
      partitionFilter)
  }

  @Test
  def testEmptyPartitions(): Unit = {
    val allPartitions = Seq()
    val n = util.eq(util.strRef("part1"), util.strNode("p1"))
    val prunedPartitions = getPrunedPartitions(allPartitions, n)
    assertTrue(prunedPartitions.isEmpty)
  }

  @Test
  def testOnePartition(): Unit = {
    val n1 = util.eq(util.strRef("part1"), util.strNode("p1"))
    val prunedPartitions1 = getPrunedPartitions(allPartitions, n1)
    assertEquals(1, prunedPartitions1.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions1.head.getEntireValue)

    val n2 = util.eq(util.strRef("part2"), util.strNode("p4"))
    val prunedPartitions2 = getPrunedPartitions(allPartitions, n2)
    assertEquals(1, prunedPartitions2.length)
    assertEquals("part1=p3,part2=p4", prunedPartitions2.head.getEntireValue)
  }

  @Test
  def testTwoPartitionAnd(): Unit = {
    val n = util.and(
      util.eq(util.strRef("part1"), util.strNode("p3")),
      util.eq(util.strRef("part2"), util.strNode("p4"))
    )
    val prunedPartitions = getPrunedPartitions(allPartitions, n)
    assertEquals(1, prunedPartitions.length)
    assertEquals("part1=p3,part2=p4", prunedPartitions.head.getEntireValue)
  }

  @Test
  def testTwoPartitionOr(): Unit = {
    val n = util.or(
      util.eq(util.strRef("part1"), util.strNode("p1")),
      util.eq(util.strRef("part2"), util.strNode("p4"))
    )
    val prunedPartitions = getPrunedPartitions(allPartitions, n)
    assertEquals(2, prunedPartitions.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions.head.getEntireValue)
    assertEquals("part1=p3,part2=p4", prunedPartitions.apply(1).getEntireValue)
  }

  @Test
  def testUDF(): Unit = {
    val n = util.or(
      util.eq(util.udfNode("upper_udf", new UpperUDF, util.strRef("part1")), util.strNode("P1")),
      util.eq(util.strRef("part2"), util.strNode("p4"))
    )
    val prunedPartitions = getPrunedPartitions(allPartitions, n)
    assertEquals(2, prunedPartitions.length)
    assertEquals("part1=p1,part2=p2", prunedPartitions.head.getEntireValue)
    assertEquals("part1=p3,part2=p4", prunedPartitions.apply(1).getEntireValue)
  }

  @Test(expected = classOf[TableException])
  def testFilterFunctionWithOpen(): Unit = {
    // upper_udf_with_open(part1) = 'P1'
    val n = util.eq(
      util.udfNode("upper_udf_with_open", new UpperUDFWithOpen, util.strRef("part1")),
      util.strNode("P1")
    )
    n.accept(new ScalarFunctionValidator())
  }

  @Test(expected = classOf[TableException])
  def testFilterFunctionWithClose(): Unit = {
    // upper_udf_with_close(part1) = 'P1'
    val n = util.eq(
      util.udfNode("upper_udf_with_close", new UpperUDFWithClose, util.strRef("part1")),
      util.strNode("P1")
    )
    n.accept(new ScalarFunctionValidator())
  }

  @Test
  def testFilterFunctionWithoutOpenClose(): Unit = {
    // upper_udf(part1) = 'P1'
    val n = util.eq(
      util.udfNode("upper_udf", new UpperUDF, util.strRef("part1")),
      util.strNode("P1")
    )
    n.accept(new ScalarFunctionValidator())
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

class UpperUDFWithOpen extends ScalarFunction {

  override def open(context: FunctionContext): Unit = {
    // no nothing
  }

  def eval(str: String): String = {
    str.toUpperCase()
  }
}

class UpperUDFWithClose extends ScalarFunction {

  def eval(str: String): String = {
    str.toUpperCase()
  }

  override def close(): Unit = {
    // no nothing
  }
}

class UpperUDF extends ScalarFunction {

  def eval(str: String): String = {
    str.toUpperCase()
  }
}
