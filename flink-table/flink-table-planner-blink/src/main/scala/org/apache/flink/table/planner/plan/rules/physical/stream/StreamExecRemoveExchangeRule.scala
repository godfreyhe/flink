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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecExchange, StreamExecGroupAggregate, StreamExecJoin, StreamPhysicalRel}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelDistribution

import java.util

import scala.collection.JavaConversions._

class StreamExecRemoveExchangeRule1
  extends RelOptRule(
    operand(classOf[StreamExecJoin],
      operand(classOf[StreamExecExchange],
        operand(classOf[StreamExecGroupAggregate], any())),
      operand(classOf[StreamPhysicalRel], any())),
    "StreamExecRemoveExchangeRule1") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val exchange: StreamExecExchange = call.rel(1)
    val agg: StreamExecGroupAggregate = call.rel(2)

    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false
    }

    util.Arrays.equals(agg.grouping, exchange.getDistribution.getKeys.map(_.toInt).toArray)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: StreamExecJoin = call.rel(0)
    val agg: StreamExecGroupAggregate = call.rel(2)
    val newJoin = join.copy(join.getTraitSet, util.Arrays.asList(agg, join.getRight))

    call.transformTo(newJoin)
  }
}

class StreamExecRemoveExchangeRule2
  extends RelOptRule(
    operand(classOf[StreamExecJoin],
      operand(classOf[StreamPhysicalRel], any()),
      operand(classOf[StreamExecExchange],
        operand(classOf[StreamExecGroupAggregate], any()))),
    "StreamExecRemoveExchangeRule2") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val exchange: StreamExecExchange = call.rel(2)
    val agg: StreamExecGroupAggregate = call.rel(3)

    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false
    }

    util.Arrays.equals(agg.grouping, exchange.getDistribution.getKeys.map(_.toInt).toArray)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: StreamExecJoin = call.rel(0)
    val agg: StreamExecGroupAggregate = call.rel(3)
    val newJoin = join.copy(join.getTraitSet, util.Arrays.asList(join.getLeft, agg))

    call.transformTo(newJoin)
  }
}

class StreamExecRemoveExchangeRule3
  extends RelOptRule(
    operand(classOf[StreamExecGroupAggregate],
      operand(classOf[StreamExecExchange],
        operand(classOf[StreamExecJoin], any()))),
    "StreamExecRemoveExchangeRule3") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val exchange: StreamExecExchange = call.rel(1)
    val join: StreamExecJoin = call.rel(2)

    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false
    }

    util.Arrays.equals(join.getJoinInfo.leftKeys.toIntArray,
      exchange.getDistribution.getKeys.map(_.toInt).toArray) ||
      util.Arrays.equals(join.getJoinInfo.rightKeys.toIntArray,
        exchange.getDistribution.getKeys.map(_.toInt).toArray)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: StreamExecGroupAggregate = call.rel(0)
    val join: StreamExecJoin = call.rel(2)

    val newAgg = agg.copy(agg.getTraitSet, util.Arrays.asList(join))

    call.transformTo(newAgg)
  }
}

class StreamExecRemoveExchangeRule4
  extends RelOptRule(
    operand(classOf[StreamExecJoin],
      operand(classOf[StreamExecExchange],
        operand(classOf[StreamExecJoin], any())),
      operand(classOf[StreamPhysicalRel], any())),
    "StreamExecRemoveExchangeRule4") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val exchange: StreamExecExchange = call.rel(1)
    val subJoin: StreamExecJoin = call.rel(2)

    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false
    }

    util.Arrays.equals(subJoin.getJoinInfo.leftKeys.toIntArray,
      exchange.getDistribution.getKeys.map(_.toInt).toArray) ||
      util.Arrays.equals(subJoin.getJoinInfo.rightKeys.toIntArray,
        exchange.getDistribution.getKeys.map(_.toInt).toArray)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: StreamExecJoin = call.rel(0)
    val subJoin: StreamExecJoin = call.rel(2)
    val newJoin = join.copy(join.getTraitSet, util.Arrays.asList(subJoin, join.getRight))

    call.transformTo(newJoin)
  }
}

class StreamExecRemoveExchangeRule5
  extends RelOptRule(
    operand(classOf[StreamExecJoin],
      operand(classOf[StreamPhysicalRel], any()),
      operand(classOf[StreamExecExchange],
        operand(classOf[StreamExecJoin], any()))),
    "StreamExecRemoveExchangeRule5") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val exchange: StreamExecExchange = call.rel(2)
    val subJoin: StreamExecJoin = call.rel(3)

    if (exchange.getDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false
    }

    util.Arrays.equals(subJoin.getJoinInfo.leftKeys.toIntArray,
      exchange.getDistribution.getKeys.map(_.toInt).toArray) ||
      util.Arrays.equals(subJoin.getJoinInfo.rightKeys.toIntArray,
        exchange.getDistribution.getKeys.map(_.toInt).toArray)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: StreamExecJoin = call.rel(0)
    val subJoin: StreamExecJoin = call.rel(3)
    val newJoin = join.copy(join.getTraitSet, util.Arrays.asList(join.getLeft, subJoin))

    call.transformTo(newJoin)
  }
}

object StreamExecRemoveExchangeRule {
  val INSTANCE1 = new StreamExecRemoveExchangeRule1
  val INSTANCE2 = new StreamExecRemoveExchangeRule2
  val INSTANCE3 = new StreamExecRemoveExchangeRule3
  val INSTANCE4 = new StreamExecRemoveExchangeRule4
  val INSTANCE5 = new StreamExecRemoveExchangeRule5
}
