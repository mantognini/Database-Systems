package relation
package compiler

import scala.collection.mutable

import ch.epfl.data.sc.pardis
import pardis.optimization.RecursiveRuleBasedTransformer
import pardis.quasi.TypeParameters._
import pardis.types._
import PardisTypeImplicits._
import pardis.ir._

import relation.deep.RelationDSLOpsPackaged
import relation.shallow._

class ColumnStoreLowering(override val IR: RelationDSLOpsPackaged, override val schemaAnalysis: SchemaAnalysis) extends RelationLowering(IR, schemaAnalysis) {
  import IR.Predef._

  // Like a Structure of Array, but using a dictionary to map schema field to column store
  // Additionally, we keep track of the cardinality of the relation
  type LoweredRelation = (Rep[Int], Map[String, Rep[Array[String]]])

  def relationScan(scanner: Rep[RelationScanner], schema: Schema, size: Rep[Int], resultSchema: Schema): LoweredRelation = {
    // Create the main storage
    val storage = (for (column <- schema.columns) yield {
      val colStorage = dsl"new Array[String]($size)"

      column -> colStorage
    }).toMap


    // Load the records using the scanner, unrolling the inner loop (which is defined as a function)
    def loadRecord(index: Var[Int]) = {
      for (column <- schema.columns) {
        dsl"${storage(column)}($index) = $scanner.next_string()"
      }
    }
    val i = newVar(dsl"0")
    dsl"""
      while ($scanner.hasNext) {
        ${loadRecord(i)}
        $i = $i + 1
      }
    """

    (size, storage)
  }

  def relationProject(relation: Rep[Relation], schema: Schema, resultSchema: Schema): LoweredRelation = {
    ??? // TODO
  }

  def relationSelect(relation: Rep[Relation], field: String, value: Rep[String], resultSchema: Schema): LoweredRelation = {
    ??? // TODO
  }

  def relationJoin(leftRelation: Rep[Relation], rightRelation: Rep[Relation], leftKey: String, rightKey: String, resultSchema: Schema): LoweredRelation = {
    ??? // TODO
  }

  def relationPrint(relation: Rep[Relation]): Unit = {
    val (size, storage) = getRelationLowered(relation)
    val schema = getRelationSchema(relation)

    // Valid even if schema is empty
    val getRecordString = (index: Rep[Int]) =>
      (dsl""" "" """ /: schema.columns.zipWithIndex) {
        case (acc, (column, 0)) => dsl"""$acc +       ${storage(column)}($index)"""
        case (acc, (column, _)) => dsl"""$acc + "|" + ${storage(column)}($index)"""
      }

    dsl"""
      for (i <- 0 until $size)
        println($getRecordString(i))
    """
  }

}