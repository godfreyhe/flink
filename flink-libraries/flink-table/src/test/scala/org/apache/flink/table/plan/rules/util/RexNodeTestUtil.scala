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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction

import scala.collection.JavaConverters._

class RexNodeTestUtil(fieldNames: Seq[String], fieldTypes: Seq[SqlTypeName]) {

  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(RelDataTypeSystem.DEFAULT)
  private val allFieldTypes: Seq[RelDataType] = fieldTypes.map(typeFactory.createSqlType)
  private val strType = typeFactory.createSqlType(SqlTypeName.VARCHAR)
  private val intType = typeFactory.createSqlType(SqlTypeName.INTEGER)
  val rowType = typeFactory.createStructType(allFieldTypes.asJava, fieldNames.asJava)
  var rexBuilder = new RexBuilder(typeFactory)

  def and(nodes: RexNode*): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes.asJava)
  }

  def or(nodes: RexNode*): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.OR, nodes.asJava)
  }

  def not(node: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.NOT, node)
  }

  def gt(left: RexNode, right: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, left, right)
  }

  def lt(left: RexNode, right: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, left, right)
  }

  def eq(left: RexNode, right: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right)
  }

  def intRef(fieldName: String): RexNode = {
    rexBuilder.makeInputRef(intType, getFieldIndex(fieldName))
  }

  def strRef(fieldName: String): RexNode = {
    rexBuilder.makeInputRef(strType, getFieldIndex(fieldName))
  }

  def intNode(value: Int): RexNode = {
    rexBuilder.makeLiteral(value, intType, true)
  }

  def strNode(s: String): RexNode = {
    rexBuilder.makeLiteral(s)
  }

  def udfNode(name: String, udf: ScalarFunction, nodes: RexNode*): RexNode = {
    rexBuilder.makeCall(new ScalarSqlFunction(name, udf, typeFactory), nodes.asJava)
  }

  def nondeterministicUdfNode(name: String, udf: ScalarFunction, nodes: RexNode*): RexNode = {
    rexBuilder.makeCall(new NondeterministicScalarSqlFunction(name, udf, typeFactory), nodes.asJava)
  }

  def likeNode(refNode: RexNode, valNode: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.LIKE, Seq(refNode, valNode).asJava)
  }

  def caseNode(conditionNode: RexNode, leftNode: RexNode, rightNode: RexNode): RexNode = {
    rexBuilder.makeCall(SqlStdOperatorTable.CASE, Seq(conditionNode, leftNode, rightNode).asJava)
  }

  private def getFieldIndex(fieldName: String): Int = {
    if (fieldNames.contains(fieldName)) {
      fieldNames.indexOf(fieldName)
    } else {
      throw new IllegalArgumentException("can not find " + fieldName)
    }
  }

  class NondeterministicScalarSqlFunction(
    name: String,
    scalarFunction: ScalarFunction,
    typeFactory: FlinkTypeFactory)
    extends ScalarSqlFunction(name, scalarFunction, typeFactory) {

    override def isDeterministic: Boolean = false
  }

}
