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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.plan.util.PartitionPredicateExtractor.extractPartitionPredicate
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class PartitionPredicateExtractorTest {

  @Test
  def testNonePartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('name === "abc")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testOnePartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part === "test")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testOnePartitionFieldWithOr(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part === "test1" || 'part === "test2")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testOnePartitionOrNonPartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('name === "abc" || 'part === "test")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testTwoPartitionFieldsWithOr(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = Array[Expression]('part1 === "test1" || 'part2 === "test2")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testTwoPartitionFieldsOrNonPartitionField(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = Array[Expression]('part1 === "test1" && 'part2 === "test2" || 'name === "abc")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testLike(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part.like("%est"))
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIsNull(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part.isNull)
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIsNotNull(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part.isNotNull)
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testCast(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part.cast(BasicTypeInfo.INT_TYPE_INFO))
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIf(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression]('part.isNull.?("test1", "test2"))
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testDeterministicUDF(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression](UpperUDF('part) === "TEST")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testNonDeterministicUDF(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = Array[Expression](RandomUDF('part) === "test1")
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testMultipleCompositeExpressions(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = Array[Expression](
      'part1 > "test1",
      'name >= "abc",
      'name === "def" || 'part1 === "test",
      'part1 >= "test1" || 'part2 < "test2",
      'part1 === "test1" && 'part2 === "test2" || 'name === "abc",
      'part1.isNotNull || 'part2.like("test") || 'part2.cast(BasicTypeInfo.INT_TYPE_INFO) === 1,
      UpperUDF('part1) === "TEST" || 'part2.toTimestamp === 123456789,
      'part1.isNull.?("test1", "test2") || 'part2.isNotNull.upperCase === "TEST"
    )
    val (partitionPredicate, remaining) = extractPartitionPredicate(predicate, partitionFieldNames)
    assertEquals(5, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertEquals(predicate(3), partitionPredicate(1))
    assertEquals(predicate(5), partitionPredicate(2))
    assertEquals(predicate(6), partitionPredicate(3))
    assertEquals(predicate(7), partitionPredicate(4))

    assertEquals(3, remaining.length)
    assertEquals(predicate(1), remaining.head)
    assertEquals(predicate(2), remaining(1))
    assertEquals(predicate(4), remaining(2))
  }

  object UpperUDF extends ScalarFunction {
    def eval(str: String): String = {
      str.toUpperCase
    }

    override def isDeterministic: Boolean = true
  }

  object RandomUDF extends ScalarFunction {
    def eval(str: String): String = {
      val r = new Random().nextInt()
      str + r
    }

    override def isDeterministic: Boolean = false
  }

}
