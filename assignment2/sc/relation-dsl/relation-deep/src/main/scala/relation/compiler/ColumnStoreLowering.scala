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
    val storage = createStorage(schema.columns, size)


    // Load the records using the scanner, unrolling the inner loop (which is defined as a function)
    def loadRecord(index: Var[Int]) =
      for (column <- schema.columns) {
        dsl"${storage(column)}($index) = $scanner.next_string()"
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
    val (size, storage) = getRelationLowered(relation)

    val newStorage = storage filterKeys { resultSchema.columns contains _ }

    (size, newStorage)
  }

  def relationSelect(relation: Rep[Relation], field: String, value: Rep[String], resultSchema: Schema): LoweredRelation = {
    val (size, storage) = getRelationLowered(relation)
    val schema = getRelationSchema(relation) // Should be the same as resultSchema...

    val predicateStorage = storage(field)

    // Count how many tuples we should keep
    val newSize = dsl"""
      var newSize_ = 0
      for (i <- 0 until $size) {
        if (${predicateStorage}(i) == $value)
          newSize_ = newSize_ + 1
      }
      newSize_
    """

    // Create a new storage for the filtered relation
    val newStorage = createStorage(schema.columns, newSize)

    // Load the data into the new storage, with unrolling of inner loop
    def copyRecord(source: Var[Int], dest: Var[Int]) =
      for (column <- schema.columns) {
        dsl"${newStorage(column)}($dest) = ${storage(column)}($source)"
      }

    val source = newVar(dsl"0")
    val dest = newVar(dsl"0")
    dsl"""
      while ($source < $size) {
        if (${predicateStorage}($source) == $value) {
          ${copyRecord(source, dest)}
          $dest = $dest + 1
        }
        $source = $source + 1
      }
    """

    (newSize, newStorage)
  }

  def relationJoin(leftRelation: Rep[Relation], rightRelation: Rep[Relation],
                   leftKey: String, rightKey: String, resultSchema: Schema): LoweredRelation = {
    val (sizeL, storageL) = getRelationLowered(leftRelation)
    val (sizeR, storageR) = getRelationLowered(rightRelation)

    val schemaL = getRelationSchema(leftRelation)
    val schemaR = getRelationSchema(rightRelation)

    val columnsL = schemaL.columns
    val columnsR = schemaR.columns filter { _ != rightKey }

    // Helper function to iterate on valid pairs of tuples only
    def iterateOnProduct(f: (Rep[Int], Rep[Int]) => Rep[Unit]) =
      dsl"""
        for {
          i <- 0 until $sizeL
          j <- 0 until $sizeR
        } {
          if (${storageL(leftKey)}(i) == ${storageR(rightKey)}(j))
            ${f}(i, j)
        }
      """

    // Count the matching pairs
    val newSize_ = newVar(dsl"0")
    iterateOnProduct { (_, _) =>
      dsl"$newSize_ = $newSize_ + 1"
    }
    val newSize = dsl"$newSize_"

    // Create the output storage
    val newStorage = createStorage(columnsL ++ columnsR, newSize)

    // Copy the tuples
    val dest = newVar(dsl"0")
    iterateOnProduct { (i, j) =>
      for (column <- columnsL) {
        dsl"${newStorage(column)}($dest) = ${storageL(column)}($i)"
      }
      for (column <- columnsR) {
        dsl"${newStorage(column)}($dest) = ${storageR(column)}($j)"
      }
      dsl"$dest = $dest + 1"
    }

    (newSize, newStorage)
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

  private def createStorage(columns: Seq[String], size: Rep[Int]) =
    (for (column <- columns) yield {
      val colStorage = dsl"new Array[String]($size)"

      column -> colStorage
    }).toMap

}

