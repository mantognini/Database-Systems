package relation
package compiler

import shallow._
import deep._

object Main extends App {

  implicit object Context extends RelationDSLOpsPackaged

  // prints "one"
  def pgrmA = dsl"""
    val schema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", schema, "|")
    val selEn = En.select(x => x.getField(schema, "number") == "one")
    val projEn = selEn.project(Schema("number"))
    projEn.print
  """

  // prints "English|number|French" for each number
  def pgrmB = dsl"""
    val EnSchema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", EnSchema, "|")
    val FrSchema = Schema("digit", "nombre")
    val Fr = Relation.scan("data/Fr.csv", FrSchema, "|")
    val EnFr = En.join(Fr, "digit", "digit")
    EnFr.print
  """

  def pgrmC = dsl"""
    val EnSchema = Schema("number", "digit")
    val En = Relation.scan("data/En.csv", EnSchema, "|").select(x =>
      x.getField(EnSchema, "number") == "one"
    )
    val projEn = En.project(Schema("number"))
    projEn.print
  """

  // def pgrm = pgrmA
  def pgrm = pgrmB
  // def pgrm = pgrmC

  val compiler = new RelationCompiler(Context)

  compiler.compile(pgrm)
}
