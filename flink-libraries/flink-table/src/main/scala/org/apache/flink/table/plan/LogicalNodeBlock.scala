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

package org.apache.flink.table.plan

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.logical._
import org.apache.flink.util.Preconditions

import scala.collection.mutable

object LogicalNodeBlockTreeBuilder {

  /**
    * Decompose the [[LogicalNode]] plan into many [[LogicalNodeBlock]]s,
    * and rebuild the [[LogicalNodeBlock]] tree.
    *
    * @param sinkNodes SinkNodes belongs to a some LogicalNode plan
    * @return Sink-LogicalNodeBlocks, each LogicalNodeBlock include a SinkNode
    */
  def buildLogicalNodeBlockTree(sinkNodes: Seq[LogicalNode]): Seq[LogicalNodeBlock] = {
    val builder = new LogicalPlanNodeTreeBuilder
    sinkNodes.foreach {
      case _: SinkNode =>
      case o => throw TableException(s"Error node: $o, Only SinkNode is supported.")
    }
    builder.buildLogicalNodeBlockTree(sinkNodes)
  }

  private class LogicalPlanNodeTreeBuilder {

    val node2Wrapper = mutable.Map[LogicalNode, LogicalNodeWrapper]()
    val node2Block = mutable.Map[LogicalNode, LogicalNodeBlock]()

    def buildLogicalNodeBlockTree(sinkNodes: Seq[LogicalNode]): Seq[LogicalNodeBlock] = {
      sinkNodes.foreach {
        s => buildNodeWrappers(s, None)
      }

      sinkNodes.map {
        s => buildBlockTree(s, None)
      }
    }

    private def buildNodeWrappers(node: LogicalNode, parent: Option[LogicalNode]): Unit = {
      node2Wrapper.getOrElseUpdate(node, new LogicalNodeWrapper(node)).addParentNode(parent)
      node.children.foreach {
        c => buildNodeWrappers(c, Some(node))
      }
    }

    private def buildBlockTree(
      node: LogicalNode,
      block: Option[LogicalNodeBlock]): LogicalNodeBlock = {

      val currentBlock = block match {
        case Some(b) => b
        case None => node2Block.getOrElseUpdate(node, new LogicalNodeBlock(node))
      }

      node.children.foreach {
        child => if (node2Wrapper.get(child).get.hasMultipleParents) {
          val childBlock = node2Block.getOrElseUpdate(child, new LogicalNodeBlock(child))
          currentBlock.addChild(childBlock)
          buildBlockTree(child, Some(childBlock))
        } else {
          buildBlockTree(child, Some(currentBlock))
        }
      }

      currentBlock
    }
  }

  class LogicalNodeWrapper(logicalNode: LogicalNode) {
    val parentNodes = mutable.Set[LogicalNode]()

    def addParentNode(parent: Option[LogicalNode]): Unit = {
      parent match {
        case Some(p) => parentNodes += p
        case None => // do nothing
      }
    }

    def hasMultipleParents: Boolean = parentNodes.size > 1
  }

}

/**
  * A [[LogicalNodeBlock]] is a sub-tree in the [[LogicalNode]] plan, and each block does not
  * include a [[LogicalNode]] which has two parent nodes.
  * The nodes in each block will be optimized independence.
  *
  * For example: (Table API)
  * val table = tEnv.scan("test_table").select('a, 'b, 'c)
  * table.where('a >= 70).select('a, 'b).writeToSink(sink1)
  * table.where('a < 70 ).select('a, 'c).writeToSink(sink2)
  *
  * the LogicalNode tree is:
  *
  *        CatalogNode
  *            |
  *       Project('a,'b,'c)
  *        /          \
  * Filter('a>=70)  Filter('a<70)
  *     |              |
  * Project('a,'b)  Project('a,'c)
  *     |              |
  * SinkNode(sink1) SinkNode(sink2)
  *
  * This [[LogicalNode]] tree will be decomposed into 3 [[LogicalNodeBlock]]s, the break-point
  * is a LogicalNode which has two parent nodes.
  * the first LogicalNodeBlock include CatalogNode and Project('a,'b,'c)
  * the second one include Filter('a>=70), Project('a,'b) and SinkNode(sink1
  * the third one include Filter('a<70), Project('a,'c), SinkNode(sink2)
  * And the first LogicalNodeBlock is the child of another two.
  * So the LogicalNodeBlock tree is:
  *
  *         LogicalNodeBlock1
  *          /            \
  * LogicalNodeBlock2  LogicalNodeBlock3
  *
  * The optimizing order is from child block to parent. The optimized result (DataSet or DataStream)
  * will be registered into tables first, and then be converted to a new CatalogNode which is the
  * new output node of current block and is also the input of its parent blocks.
  *
  * @param outputNode A LogicalNode of the output in the block, which could be a SinkNode or
  *                   a LogicalNode which has two parent nodes.
  */
class LogicalNodeBlock(val outputNode: LogicalNode) {
  private val childBlocks = mutable.Set[LogicalNodeBlock]()
  // After this block has been optimized, the result will be converted to a new CatalogNode as
  // new output node
  private var newOutputNode: Option[LogicalNode] = None

  def addChild(block: LogicalNodeBlock): Unit = {
    childBlocks += block
  }

  def children: Seq[LogicalNodeBlock] = childBlocks.toSeq

  def setNewOutputNode(newNode: LogicalNode): Unit = {
    newOutputNode = Some(newNode)
  }

  def getNewOutputNode: Option[LogicalNode] = newOutputNode

  /**
    * Get new logical node plan of this block. The child blocks (inputs) will be replace with
    * new CatalogNodes (the optimized result of child block).
    *
    * @return New LogicalNode plan of this block
    */
  def getLogicalPlan: LogicalNode = {

    def isChildBlockOutputNode(node: LogicalNode): Option[LogicalNodeBlock] = {
      val find = childBlocks.filter(_.outputNode eq node)
      if (find.isEmpty) {
        None
      } else {
        Preconditions.checkArgument(find.size == 1)
        Some(find.head)
      }
    }

    def createNewLogicalPlan(node: LogicalNode): LogicalNode = {
      val newChildren = node.children.map {
        n =>
          val block = isChildBlockOutputNode(n)
          if (block.isDefined) {
            block.get.getNewOutputNode.get
          } else {
            createNewLogicalPlan(n)
          }
      }

      cloneLogicalNode(node, newChildren)
    }

    createNewLogicalPlan(outputNode)
  }

  private def cloneLogicalNode(node: LogicalNode, children: Seq[LogicalNode]): LogicalNode = {
    node match {
      case CatalogNode(tableName, rowType) => CatalogNode(tableName, rowType)
      case LogicalRelNode(relNode) => LogicalRelNode(relNode)
      case u: UnaryNode =>
        Preconditions.checkArgument(children.length == 1)
        val child = children.head
        u match {
          case Filter(condition, _) => Filter(condition, child)
          case LogicalTableFunctionCall(funcName, tableFunc, params, resultType, fieldNames, _) =>
            LogicalTableFunctionCall(funcName, tableFunc, params, resultType, fieldNames, child)
          case Aggregate(grouping, aggregate, _) => Aggregate(grouping, aggregate, child)
          case Limit(offset, fetch, _) => Limit(offset, fetch, child)
          case WindowAggregate(grouping, window, property, aggregate, _) =>
            WindowAggregate(grouping, window, property, aggregate, child)
          case Distinct(_) => Distinct(child)
          case SinkNode(_, sink) => SinkNode(child, sink)
          case AliasNode(aliasList, _) => AliasNode(aliasList, child)
          case Project(projectList, _) => Project(projectList, child)
          case Sort(order, _) => Sort(order, child)
          case _ => throw TableException(s"Unsupported UnaryNode node: $node")
        }
      case b: BinaryNode =>
        Preconditions.checkArgument(children.length == 2)
        val left = children.head
        val right = children(1)
        b match {
          case Join(_, _, joinType, condition, correlated) =>
            Join(left, right, joinType, condition, correlated)
          case Union(_, _, all) => Union(left, right, all)
          case Intersect(_, _, all) => Intersect(left, right, all)
          case Minus(_, _, all) => Minus(left, right, all)
          case _ => throw TableException(s"Unsupported BinaryNode node: $node")
        }
      case _ => throw TableException(s"Unsupported LogicalNode node: $node")
    }
  }

}

