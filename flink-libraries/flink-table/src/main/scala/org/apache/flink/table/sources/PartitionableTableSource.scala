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

import org.apache.flink.table.plan.rules.PartitionPruningRule
import org.apache.flink.table.plan.rules.util.PartitionPruner

/**
  * A [[TableSource]] extending this interface is a partition table,
  * and will get the relevant partitions about the query after [[PartitionPruningRule]] applied.
  */
trait PartitionableTableSource {

  /**
    * Get all partitions belong to this table
    *
    * @return All partitions belong to this table
    */
  def getAllPartitions: Seq[Partition]

  /**
    * Set pruned partitions when [[PartitionPruningRule]] applied
    *
    * @param partitions The pruned partitions
    */
  def setPrunedPartitions(partitions: Seq[Partition])

  /**
    * Get partition field names.
    *
    * @return Partition field names.
    */
  def getPartitionFieldNames: Array[String]

  /**
    * Get custom [[PartitionPruner]] or default [[PartitionPruner.DEFAULT]]
    *
    * @return Custom PartitionPruner or DefaultPartitionPrunerImpl
    */
  def getPartitionPruner: Option[PartitionPruner]

}

