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

package org.apache.flink.table.plan.rules

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexProgram}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.dataset.{BatchTableSourceScan, DataSetCalc}
import org.apache.flink.table.plan.nodes.datastream.{DataStreamCalc, StreamTableSourceScan}
import org.apache.flink.table.plan.rules.util.PartitionConditionExtractor
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.{PartitionableTableSource, TableSource}
import org.slf4j.LoggerFactory

/**
  * This rule tries to extract partition-pruning predicate, get the relevant partitions from
  * all partitions by the predicate.
  */
abstract class PartitionPruningRule(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description) {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private[rules] def doOnMatch(
    tableSource: PartitionableTableSource,
    calcProgram: RexProgram,
    rowType: RelDataType,
    builder: RexBuilder
  ): Unit = {

    if (calcProgram.getCondition == null) {
      return
    }

    val partitionFieldNames = tableSource.getPartitionFieldNames

    val errorFieldNames = partitionFieldNames.filterNot(rowType.getFieldNames.contains)
    if (errorFieldNames.nonEmpty) {
      throw new TableException(s"Error partition field names: ${errorFieldNames.mkString(",")}")
    }

    val partitionCondition = PartitionConditionExtractor.extract(
      calcProgram.expandLocalRef(calcProgram.getCondition),
      partitionFieldNames,
      rowType,
      builder)

    if (partitionCondition == null) {
      return
    }

    if (LOG.isDebugEnabled) {
      LOG.debug(s"partition condition: ${partitionCondition.toString}")
    }

    val allPartitions = tableSource.getAllPartitions
    val partitionPruner = tableSource.getPartitionPruner.get
    val prunedPartitions = partitionPruner.getPrunedPartitions(
      partitionFieldNames,
      rowType,
      allPartitions,
      partitionCondition
    )

    LOG.info(s"number of all partitions is ${allPartitions.size}, " +
      s"number of pruned partitions is ${prunedPartitions.size}")
    if (LOG.isDebugEnabled) {
      LOG.debug(s"pruned partitions: ${prunedPartitions.map(f => f.getEntireValue).mkString(",")}")
    }

    tableSource.setPrunedPartitions(prunedPartitions)
  }

  def doMatches(tableSource: TableSource[_]): Boolean = {
    tableSource match {
      case t: PartitionableTableSource => t.getPartitionPruner.isDefined &&
        t.getPartitionFieldNames != null && t.getPartitionFieldNames.nonEmpty
      case _ => false
    }
  }
}

class DataSetPartitionPruningRule extends PartitionPruningRule(
  RelOptRule.operand(classOf[DataSetCalc],
    RelOptRule.operand(classOf[BatchTableSourceScan], RelOptRule.any())),
  "DataSetPartitionPruningRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: BatchTableSourceScan = call.rel(1)
    val table: TableSourceTable[_] = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    doMatches(table.tableSource)
  }

  override def onMatch(call: RelOptRuleCall) = {
    val filter: DataSetCalc = call.rel(0)
    val scan: BatchTableSourceScan = call.rel(1)
    val table = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val tableSource = table.tableSource.asInstanceOf[PartitionableTableSource]
    doOnMatch(tableSource, filter.calcProgram, scan.getRowType, scan.getCluster.getRexBuilder)
  }
}

class DataStreamPartitionPruningRule extends PartitionPruningRule(
  RelOptRule.operand(classOf[DataStreamCalc],
    RelOptRule.operand(classOf[StreamTableSourceScan], RelOptRule.any())),
  "DataStreamPartitionPruningRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: StreamTableSourceScan = call.rel(1)
    val table: TableSourceTable[_] = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    doMatches(table.tableSource)
  }

  override def onMatch(call: RelOptRuleCall) = {
    val filter: DataStreamCalc = call.rel(0)
    val scan: StreamTableSourceScan = call.rel(1)
    val table = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val tableSource = table.tableSource.asInstanceOf[PartitionableTableSource]
    doOnMatch(tableSource, filter.calcProgram, scan.getRowType, scan.getCluster.getRexBuilder)
  }
}

object DataSetPartitionPruningRule {
  val INSTANCE: RelOptRule = new DataSetPartitionPruningRule
}

object DataStreamPartitionPruningRule {
  val INSTANCE: RelOptRule = new DataStreamPartitionPruningRule
}
