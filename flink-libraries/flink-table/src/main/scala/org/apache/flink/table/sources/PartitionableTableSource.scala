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

package org.apache.flink.table.sources

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.util.{PartitionPredicateExtractor, PartitionPruner}

/**
  * A [[TableSource]] extending this class is a partition table,
  * and will get the relevant partitions about the query.
  */
abstract class PartitionableTableSource extends FilterableTableSource {

  private var prunedPartitions: Option[Seq[Partition]] = None
  private var relBuilder: Option[RelBuilder] = None

  /**
    * Get all partitions belong to this table
    *
    * @return All partitions belong to this table
    */
  def getAllPartitions: Seq[Partition]

  /**
    * Get partition field names.
    *
    * @return Partition field names.
    */
  def getPartitionFieldNames: Array[String]

  /**
    * Get partition field types.
    *
    * @return Partition field types.
    */
  def getPartitionFieldTypes: Array[TypeInformation[_]]

  /**
    * Whether drop partition predicate after apply partition pruning.
    *
    * @return true only if the result is correct without partition predicate
    */
  def supportDropPartitionPredicate: Boolean = false

  /**
    * If the table source should also apply filters it needs to override this method.
    * by default returns all predicates.
    *
    * @param predicate a filter expression that will be applied to fields to return.
    * @return an unsupported predicate expression.
    */
  def applyPredicate(predicate: Array[Expression]): Array[Expression] = {
    predicate
  }

  /** return an predicate expression that was set. */
  override def getPredicate: Array[Expression] = {
    Array.empty
  }

  /**
    * @param relBuilder Builder for relational expressions.
    */
  def setRelBuilder(relBuilder: RelBuilder): Unit = {
    this.relBuilder = Some(relBuilder)
  }

  /**
    * @param predicate a filter expression that will be applied to fields to return.
    * @return an unsupported predicate expression.
    */
  override def setPredicate(predicate: Array[Expression]): Array[Expression] = {
    val partitionFieldNames = getPartitionFieldNames
    val nonPartitionPredicate = if (partitionFieldNames.nonEmpty) {
      // extract partition predicate
      val (partitionPredicate, otherPredicate) =
        PartitionPredicateExtractor.extractPartitionPredicate(predicate, partitionFieldNames)
      if (partitionPredicate.nonEmpty) {
        // do partition pruning
        prunedPartitions = Some(applyPartitionPruning(partitionPredicate))
      }
      otherPredicate
    } else {
      predicate
    }

    val finalRemaining = if (supportDropPartitionPredicate) {
      nonPartitionPredicate
    } else {
      predicate
    }

    applyPredicate(finalRemaining)
  }

  /**
    * Default implementation for partition pruning.
    *
    * @param partitionPredicate a filter expression that will be applied to partition values.
    * @return the pruned partitions
    */
  def applyPartitionPruning(partitionPredicate: Array[Expression]): Seq[Partition] = {
    val partitionPruner = getPartitionPruner.getOrElse(
      throw new TableException("PartitionPruner is null"))
    val builder = relBuilder.getOrElse(throw new TableException("relBuilder is null"))
    partitionPruner.getPrunedPartitions(
      getPartitionFieldNames,
      getPartitionFieldTypes,
      getAllPartitions,
      partitionPredicate,
      builder)
  }

  /**
    * Get custom [[PartitionPruner]] or default [[PartitionPruner.DEFAULT]]
    *
    * @return Custom PartitionPruner or DefaultPartitionPrunerImpl
    */
  def getPartitionPruner: Option[PartitionPruner] = Some(PartitionPruner.DEFAULT)

  /**
    * @return pruned partitions
    */
  def getPrunedPartitions: Option[Seq[Partition]] = prunedPartitions
}

/**
  * The base class of partition value
  */
trait Partition {

  def getFieldValue(fieldName: String): Any

  def getEntireValue: Any
}
