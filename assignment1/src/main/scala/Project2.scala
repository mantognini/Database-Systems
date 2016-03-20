package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast

import java.util.regex.{ Pattern => JPattern }

// See Project1 for query & schemata details
object Project2 {
  private type Path = String

  private type Pattern = Broadcast[JPattern]

  private case class Settings(
    input: Path,
    output: Path,
    task: Int
  ) {
    require(task == 1 || task == 2)

    def getCustomersPath = input + "/customer.tbl"
    def getOrdersPath    = input + "/orders.tbl"
    def getOutputPath    = output + "/out_" + task
  }

  // Read settings from command line arguments
  private def extractSettings(args: Array[String]): Settings = {
    if (args.length != 3)
      throw new IllegalArgumentException("Incorrect number of parameters")

    Settings(args(0), args(1), args(2).toInt)
  }

  // Extract the customer's key
  private def extractCustomer(record: String): Int = {
    val custkey = record takeWhile { _ != '|' }
    custkey.toInt
  }

  // Determine whether an order's comment is refering to a special request
  // (useful for pattern matching)
  private object IsNormal {
    def unapply(comment: String)(implicit pattern: Pattern): Option[String] = test(comment) match {
      case true  => None
      case false => Some(comment)
    }

    def test(comment: String)(implicit pattern: Pattern) = pattern.value.matcher(comment).matches
  }

  // Extract normal order and start counting
  private def extractNormalOrder(record: String)(implicit pattern: Pattern): Option[(Int, Int)] = {
    val fields  = record split '|'
    val custkey = fields(1).toInt
    val comment = fields(8)

    comment match {
      case IsNormal(_) => Some((custkey, 1))
      case _           => None
    }
  }

  // Main program procedure:
  // Read the data from file and save the result to file in the same CSV format
  def main(args: Array[String]): Unit = {
    // Create a Spark context using default settings
    val s    = extractSettings(args)
    val conf = new SparkConf().setAppName(s"mantogni.Project2 <${s.task}>")
    val sc   = new SparkContext(conf)

    // Broadcast pattern
    val p = """.*special.*requests.*""".r.pattern
    implicit val pattern = sc broadcast p

    val histogram =
      if (s.task == 1) runTask1(sc, s)
      else             runTask2(sc, s)

    histogram map { case (k, v) => s"$k|$v" } coalesce 1 saveAsTextFile s.getOutputPath
  }

  def runTask1(sc: SparkContext, s: Settings)(implicit pattern: Pattern) = {
    // Load customers' key
    val customers = {
      val source = sc textFile s.getCustomersPath
      val keys   = source map extractCustomer

      // NOTE: we add an extra column to the data
      //       to have a (K, V) RDD for PairRDD features
      keys map { key => (key, 0) } // this value is actually not used
    }

    // Load normal orders and start counting
    val orders = sc textFile s.getOrdersPath flatMap extractNormalOrder

    // Count the number of orders for each customer having at least one order
    val orderCount = orders reduceByKey { _ + _ }

    // Apply the left outer join then map the data to start counting,
    // dropping the extra 0 along the way and resulting in (count, 1)
    val join = customers leftOuterJoin orderCount map {
      case (_, (0, Some(count))) => (count, 1)
      case (_, (0, None))        => (0,     1)

      case (_, (e, _))           => sys.error("unexpected value " + e)
    }

    // Finilise the histogram and save the result in the proper format
    join reduceByKey { _ + _ }
  }

  def runTask2(sc: SparkContext, s: Settings)(implicit pattern: Pattern) = {
    // Optimised version with inner join instead of left outer join.
    // Hence, no seed of customers anymore!

    // Load normal orders and start counting
    val orders = sc textFile s.getOrdersPath flatMap extractNormalOrder

    // Count the number of orders for each customer having at least one order
    val orderCount = orders reduceByKey { _ + _ }

    // Finilise the histogram and save the result in the proper format
    orderCount map { case (_, count) => (count, 1) } reduceByKey { _ + _ }
  }
}
