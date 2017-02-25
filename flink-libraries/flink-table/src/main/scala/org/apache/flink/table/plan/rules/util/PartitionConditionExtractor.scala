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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object PartitionConditionExtractor {

  /**
    * Extracts the partition condition from original condition
    *
    * @param condition           original condition
    * @param partitionFieldNames The names of partition field
    * @param rowType             input row type
    * @param builder             builder for Rex expressions
    * @return partition condition
    */
  def extract(
    condition: RexNode,
    partitionFieldNames: Array[String],
    rowType: RelDataType,
    builder: RexBuilder): RexNode = {
    val extractor = new PartitionConditionExtractor(partitionFieldNames, rowType, builder)
    condition.accept(extractor)
  }
}

class PartitionConditionExtractor(
  partitionFieldNames: Array[String],
  rowType: RelDataType,
  builder: RexBuilder)
  extends RexVisitorImpl[RexNode](true) {

  override def visitInputRef(inputRef: RexInputRef): RexNode = {

    val f = rowType.getFieldList.get(inputRef.getIndex)
    if (partitionFieldNames.contains(f.getName)) {
      inputRef
    } else {
      null
    }
  }

  override def visitCall(call: RexCall): RexNode = {

    val isDeterministic = call.getOperator match {
      case e: SqlFunction if e.getFunctionType == SqlFunctionCategory.USER_DEFINED_FUNCTION =>
        e.isDeterministic
      case _ => true
    }
    // drop non-deterministic udf
    if (!isDeterministic) {
      return null
    }

    val newOperands = ArrayBuffer[RexNode]()
    var operandsPruned: Boolean = false
    call.operands.asScala.foreach {
      case n =>
        val r = n.accept(this)
        if (r != null) {
          newOperands += r
        } else {
          operandsPruned = true
        }
    }

    call.getOperator match {
      case SqlStdOperatorTable.AND =>
        newOperands.size match {
          case 0 => null
          case 1 => newOperands.head
          case _ => builder.makeCall(call.getOperator, newOperands.toList.asJava)
        }
      case _ if !operandsPruned => call
      case _ => null
    }
  }

  override def visitLiteral(literal: RexLiteral): RexNode = literal
}
