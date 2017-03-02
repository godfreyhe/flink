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
import org.apache.flink.table.functions.ScalarFunction
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class PartitionConditionExtractorTest {
  private val allFieldNames = List("id", "name", "part1", "part2")
  private val allFieldTypes = List(BIGINT, VARCHAR, VARCHAR, VARCHAR)
  private val partitionFieldNames = Array("part1", "part2")

  val util = new RexNodeTestUtil(allFieldNames, allFieldTypes)

  def extractPartitionCondition(partitionCondition: RexNode): RexNode = {
    PartitionConditionExtractor.extract(
      partitionCondition, partitionFieldNames, util.rowType, util.rexBuilder)
  }

  @Test
  def testNoPartitionFields(): Unit = {
    // id < 10 and (name = 'abc' or name= 'test')
    val n1 = util.and(
      util.lt(util.intRef("id"), util.intNode(10)),
      util.or(
        util.eq(util.strRef("name"), util.strNode("abc")),
        util.eq(util.strRef("name"), util.strNode("test"))
      )
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("AND(<($0, 10), OR(=($1, 'abc'), =($1, 'test')))", n1.toString)
    assertNull(partNode1)

    // id < 10 or name = 'abc' or name= 'test'
    val n2 = util.or(
      util.lt(util.intRef("id"), util.intNode(10)),
      util.or(
        util.eq(util.strRef("name"), util.strNode("abc")),
        util.eq(util.strRef("name"), util.strNode("test"))
      )
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("OR(<($0, 10), OR(=($1, 'abc'), =($1, 'test')))", n2.toString)
    assertNull(partNode2)
  }

  @Test
  def testOnePartition(): Unit = {
    // part1 = 'p1'
    val n = util.eq(util.strRef("part1"), util.strNode("p1"))
    val partNode = extractPartitionCondition(n)
    assertEquals("=($2, 'p1')", n.toString)
    assertEquals("=($2, 'p1')", partNode.toString)
  }

  @Test
  def testOnePartitionAnd(): Unit = {
    // id > 10 and part1 > 'p1'
    val n1 = util.and(
      util.gt(util.intRef("id"), util.intNode(10)),
      util.gt(util.strRef("part1"), util.strNode("p1"))
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("AND(>($0, 10), >($2, 'p1'))", n1.toString)
    assertEquals(">($2, 'p1')", partNode1.toString)

    // (id > 10 or id < 5) and part1 > 'p1'
    val n2 = util.and(
      util.or(
        util.gt(util.intRef("id"), util.intNode(10)),
        util.lt(util.intRef("id"), util.intNode(5))
      ),
      util.gt(util.strRef("part1"), util.strNode("p1"))
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(OR(>($0, 10), <($0, 5)), >($2, 'p1'))", n2.toString)
    assertEquals(">($2, 'p1')", partNode2.toString)

    // (id > 10 or id < 5) and (part1 = 'p1' or part1 = 'p3')
    val n3 = util.and(
      util.or(
        util.gt(util.intRef("id"), util.intNode(10)),
        util.lt(util.intRef("id"), util.intNode(5))
      ),
      util.or(
        util.eq(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part1"), util.strNode("p3"))
      )
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("AND(OR(>($0, 10), <($0, 5)), OR(=($2, 'p1'), =($2, 'p3')))", n3.toString)
    assertEquals("OR(=($2, 'p1'), =($2, 'p3'))", partNode3.toString)
  }

  @Test
  def testOnePartitionOr(): Unit = {
    // id < 10 or part1 = 'p1'
    val n1 = util.or(
      util.lt(util.intRef("id"), util.intNode(10)),
      util.eq(util.strRef("part1"), util.strNode("p1"))
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("OR(<($0, 10), =($2, 'p1'))", n1.toString)
    assertNull(partNode1)

    // id < 10 or (part1 = 'p1' or part1 = 'p2')
    val n2 = util.or(
      util.lt(util.intRef("id"), util.intNode(10)),
      util.or(
        util.eq(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part1"), util.strNode("p2"))
      )
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("OR(<($0, 10), OR(=($2, 'p1'), =($2, 'p2')))", n2.toString)
    assertNull(partNode2)
  }

  @Test
  def testOnePartitionNot(): Unit = {
    // not(part1 = 'p1')
    val n1 = util.not(util.eq(util.strRef("part1"), util.strNode("p1")))
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("NOT(=($2, 'p1'))", n1.toString)
    assertEquals("NOT(=($2, 'p1'))", partNode1.toString)

    // not(part1 = 'p1') and id = 10
    val n2 = util.and(
      util.not(
        util.eq(util.strRef("part1"), util.strNode("p1"))
      ),
      util.eq(util.intRef("id"), util.intNode(10))
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(NOT(=($2, 'p1')), =($0, 10))", n2.toString)
    assertEquals("NOT(=($2, 'p1'))", partNode2.toString)

    // not(part1 = 'p1' or part1 = 'p2') or id = 10
    val n3 = util.or(
      util.not(
        util.or(
          util.eq(util.strRef("part1"), util.strNode("p1")),
          util.eq(util.strRef("part1"), util.strNode("p2"))
        )
      ),
      util.eq(util.intRef("id"), util.intNode(10))
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("OR(NOT(OR(=($2, 'p1'), =($2, 'p2'))), =($0, 10))", n3.toString)
    assertNull(partNode3)
  }

  @Test
  def testTwoPartitionAnd(): Unit = {
    // part1 > 'p1' and part2 = 'p2'
    val n1 = util.and(
      util.gt(util.strRef("part1"), util.strNode("p1")),
      util.eq(util.strRef("part2"), util.strNode("p2"))
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("AND(>($2, 'p1'), =($3, 'p2'))", n1.toString)
    assertEquals("AND(>($2, 'p1'), =($3, 'p2'))", partNode1.toString)

    // part1 > 'p1' and (id = 10 or id = 20) and part2 = 'p2'
    val n2 = util.and(
      util.gt(util.strRef("part1"), util.strNode("p1")),
      util.or(
        util.eq(util.intRef("id"), util.intNode(10)),
        util.eq(util.intRef("id"), util.intNode(20))
      ),
      util.eq(util.strRef("part2"), util.strNode("p2"))
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(>($2, 'p1'), OR(=($0, 10), =($0, 20)), =($3, 'p2'))", n2.toString)
    assertEquals("AND(>($2, 'p1'), =($3, 'p2'))", partNode2.toString)

    // (part1 > 'p1' or part2 < 'p4') and (part1 < 'p3' or part2 > 'p2') and (id = 10 or id = 20)
    val n3 = util.and(
      util.and(
        util.or(
          util.gt(util.strRef("part1"), util.strNode("p1")),
          util.lt(util.strRef("part2"), util.strNode("p4"))
        ),
        util.or(
          util.lt(util.strRef("part1"), util.strNode("p3")),
          util.gt(util.strRef("part2"), util.strNode("p2"))
        )
      ),
      util.or(
        util.eq(util.intRef("id"), util.intNode(10)),
        util.eq(util.intRef("id"), util.intNode(20))
      )
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("AND(AND(OR(>($2, 'p1'), <($3, 'p4')), OR(<($2, 'p3'), >($3, 'p2'))), " +
      "OR(=($0, 10), =($0, 20)))", n3.toString)
    assertEquals(
      "AND(OR(>($2, 'p1'), <($3, 'p4')), OR(<($2, 'p3'), >($3, 'p2')))", partNode3.toString)
  }

  @Test
  def testTwoPartitionOr(): Unit = {
    // part1 > 'p1' or part2 = 'p2'
    val n1 = util.or(
      util.gt(util.strRef("part1"), util.strNode("p1")),
      util.eq(util.strRef("part2"), util.strNode("p2"))
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("OR(>($2, 'p1'), =($3, 'p2'))", n1.toString)
    assertEquals("OR(>($2, 'p1'), =($3, 'p2'))", partNode1.toString)

    // (part1 > 'p1' or part2 = 'p2') and id = 10
    val n2 = util.and(
      util.or(
        util.gt(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part2"), util.strNode("p2"))
      ),
      util.eq(util.intRef("id"), util.intNode(10))
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(OR(>($2, 'p1'), =($3, 'p2')), =($0, 10))", n2.toString)
    assertEquals("OR(>($2, 'p1'), =($3, 'p2'))", partNode2.toString)

    // (part1 > 'p1' or part2 = 'p2') or id = 10
    val n3 = util.or(
      util.or(
        util.gt(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part2"), util.strNode("p2"))
      ),
      util.eq(util.intRef("id"), util.intNode(10))
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("OR(OR(>($2, 'p1'), =($3, 'p2')), =($0, 10))", n3.toString)
    assertNull(partNode3)

    // ((part1 = 'p1' and part2 = 'p4') or (part1 = 'p3' and part2 = 'p2')) and (id = 10 or id = 20)
    val n4 = util.and(
      util.or(
        util.and(
          util.eq(util.strRef("part1"), util.strNode("p1")),
          util.eq(util.strRef("part2"), util.strNode("p4"))
        ),
        util.and(
          util.eq(util.strRef("part1"), util.strNode("p3")),
          util.eq(util.strRef("part2"), util.strNode("p2"))
        )
      ),
      util.or(
        util.eq(util.intRef("id"), util.intNode(10)),
        util.eq(util.intRef("id"), util.intNode(20))
      )
    )
    val partNode4 = extractPartitionCondition(n4)
    assertEquals("AND(OR(AND(=($2, 'p1'), =($3, 'p4')), AND(=($2, 'p3'), =($3, 'p2'))), " +
      "OR(=($0, 10), =($0, 20)))", n4.toString)
    assertEquals(
      "OR(AND(=($2, 'p1'), =($3, 'p4')), AND(=($2, 'p3'), =($3, 'p2')))", partNode4.toString)
  }

  @Test
  def testTwoPartitionNot(): Unit = {
    // not(part1 = 'p1' and part2 = 'p2')
    val n1 = util.not(
      util.and(
        util.eq(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part2"), util.strNode("p2"))
      )
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("NOT(AND(=($2, 'p1'), =($3, 'p2')))", n1.toString)
    assertEquals("NOT(AND(=($2, 'p1'), =($3, 'p2')))", partNode1.toString)

    // not(part1 = 'p1' or part2 = 'p2')
    val n2 = util.not(
      util.or(
        util.eq(util.strRef("part1"), util.strNode("p1")),
        util.eq(util.strRef("part2"), util.strNode("p2"))
      )
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("NOT(OR(=($2, 'p1'), =($3, 'p2')))", n2.toString)
    assertEquals("NOT(OR(=($2, 'p1'), =($3, 'p2')))", partNode2.toString)


    // not(part1 = 'p1' or not(part2 = 'p2')) and id = 10
    val n3 = util.and(
      util.not(
        util.or(
          util.eq(util.strRef("part1"), util.strNode("p1")),
          util.not(
            util.eq(util.strRef("part2"), util.strNode("p2"))
          )
        )
      ),
      util.eq(util.intRef("id"), util.intNode(10))
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("AND(NOT(OR(=($2, 'p1'), NOT(=($3, 'p2')))), =($0, 10))", n3.toString)
    assertEquals("NOT(OR(=($2, 'p1'), NOT(=($3, 'p2'))))", partNode3.toString)
  }

  @Test
  def testUDF(): Unit = {
    // upper_udf(part1) = 'p1'
    val n1 = util.eq(
      util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
      util.strNode("p1")
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("=(upper_udf($2), 'p1')", n1.toString)
    assertEquals("=(upper_udf($2), 'p1')", partNode1.toString)

    // (upper_udf(part1) = 'p1' or split_udf(part2, '#') = 'p2') and upper_udf(name) = 'abc'
    val n2 = util.and(
      util.or(
        util.eq(
          util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
          util.strNode("p1")
        ),
        util.eq(
          util.udfNode("split_udf", new SplitUDF(), util.strRef("part2"), util.strNode("#")),
          util.strNode("p2")
        )
      ),
      util.eq(
        util.udfNode("upper_udf", new UpperUDF(), util.strRef("name")),
        util.strNode("abc")
      )
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals(
      "AND(OR(=(upper_udf($2), 'p1'), =(split_udf($3, '#'), 'p2')), =(upper_udf($1), 'abc'))",
      n2.toString)
    assertEquals("OR(=(upper_udf($2), 'p1'), =(split_udf($3, '#'), 'p2'))", partNode2.toString)

    // upper_udf(part1) = 'p1' and concat_udf(part2, name, '#') = 'p2#abc'
    val n3 = util.and(
      util.eq(
        util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
        util.strNode("p1")
      ),
      util.eq(
        util.udfNode("concat_udf", new ConcatUDF(),
          util.strRef("part2"), util.strRef("name"), util.strNode("#")),
        util.strNode("p2")
      )
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("AND(=(upper_udf($2), 'p1'), =(concat_udf($3, $1, '#'), 'p2'))", n3.toString)
    assertEquals("=(upper_udf($2), 'p1')", partNode3.toString)

    // upper_udf(part1) = 'p1' or concat_udf(part2, name, '#') = 'p2#abc'
    val n4 = util.or(
      util.eq(
        util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
        util.strNode("p1")
      ),
      util.eq(
        util.udfNode("concat_udf", new ConcatUDF(),
          util.strRef("part2"), util.strRef("name"), util.strNode("#")),
        util.strNode("p2")
      )
    )
    val partNode4 = extractPartitionCondition(n4)
    assertEquals("OR(=(upper_udf($2), 'p1'), =(concat_udf($3, $1, '#'), 'p2'))", n4.toString)
    assertNull(partNode4)
  }

  @Test
  def testNondeterministicUDF(): Unit = {
    // random_udf(part1) = 'p1'
    val n1 = util.eq(
      util.nondeterministicUdfNode("random_udf", new RandomUDF(), util.strRef("part1")),
      util.strNode("p1")
    )
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("=(random_udf($2), 'p1')", n1.toString)
    assertNull(partNode1)

    // upper_udf(part1) = 'p1' and random_udf(part2) = 'p2'
    val n2 = util.and(
      util.eq(
        util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
        util.strNode("p1")
      ),
      util.eq(
        util.nondeterministicUdfNode("random_udf", new RandomUDF(), util.strRef("part2")),
        util.strNode("p2")
      )
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(=(upper_udf($2), 'p1'), =(random_udf($3), 'p2'))", n2.toString)
    assertEquals("=(upper_udf($2), 'p1')", partNode2.toString)

    // upper_udf(part1) = 'p1' or random_udf(part2) = 'p2'
    val n3 = util.or(
      util.eq(
        util.udfNode("upper_udf", new UpperUDF(), util.strRef("part1")),
        util.strNode("p1")
      ),
      util.eq(
        util.nondeterministicUdfNode("random_udf", new RandomUDF(), util.strRef("part2")),
        util.strNode("p2")
      )
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("OR(=(upper_udf($2), 'p1'), =(random_udf($3), 'p2'))", n3.toString)
    assertNull(partNode3)
  }

  @Test
  def testSpecialOperatorLike(): Unit = {
    // name like 'abc'
    val n1 = util.likeNode(util.strRef("name"), util.strNode("p1"))
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("LIKE($1, 'p1')", n1.toString)
    assertNull(partNode1)

    // part1 like 'p1'
    val n2 = util.likeNode(util.strRef("part1"), util.strNode("p1"))
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("LIKE($2, 'p1')", n2.toString)
    assertEquals("LIKE($2, 'p1')", partNode2.toString)
  }

  @Test
  def testSpecialOperatorCaseWhen(): Unit = {
    // id = (case name when 'abc' then 1 else 2 end)
    val n1 = util.eq(
      util.intRef("id"),
      util.caseNode(
        util.eq(util.strRef("name"), util.strNode("abc")),
        util.intNode(1),
        util.intNode(2)
      ))
    val partNode1 = extractPartitionCondition(n1)
    assertEquals("=($0, CASE(=($1, 'abc'), 1, 2))", n1.toString)
    assertNull(partNode1)

    // part1 = (case name when 'abc' then 'p1' else 'p2' end) and name = 'abc'
    val n2 = util.and(
      util.eq(
        util.intRef("part1"),
        util.caseNode(
          util.eq(util.strRef("name"), util.strNode("abc")),
          util.strNode("p1"),
          util.strNode("p2")
        )),
      util.eq(util.strRef("name"), util.strNode("abc"))
    )
    val partNode2 = extractPartitionCondition(n2)
    assertEquals("AND(=($2, CASE(=($1, 'abc'), 'p1', 'p2')), =($1, 'abc'))", n2.toString)
    assertNull(partNode2)

    // part1 = (case part2 when 'p3' then 'p1' else 'p2' end) and name = 'abc'
    val n3 = util.and(
      util.eq(
        util.intRef("part1"),
        util.caseNode(
          util.eq(util.strRef("part2"), util.strNode("p3")),
          util.strNode("p1"),
          util.strNode("p2")
        )),
      util.eq(util.strRef("name"), util.strNode("abc"))
    )
    val partNode3 = extractPartitionCondition(n3)
    assertEquals("AND(=($2, CASE(=($3, 'p3'), 'p1', 'p2')), =($1, 'abc'))", n3.toString)
    assertEquals("=($2, CASE(=($3, 'p3'), 'p1', 'p2'))", partNode3.toString)
  }

  class UpperUDF extends ScalarFunction {
    def eval(str: String): String = {
      str.toUpperCase
    }
  }

  class SplitUDF extends ScalarFunction {
    def eval(str: String, split: String): String = {
      str.split(split).head
    }
  }

  class ConcatUDF extends ScalarFunction {
    def eval(str1: String, str2: String, connector: String): String = {
      str1 + connector + str2
    }
  }

  class RandomUDF extends ScalarFunction {
    def eval(str: String): String = {
      val r = new Random().nextInt()
      str + r
    }
  }

}
