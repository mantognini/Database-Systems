package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// The query to handle is:
//
//  SELECT O_COUNT, COUNT(*) AS CUSTDIST
//  FROM (
//    SELECT C_CUSTKEY, COUNT(O_ORDERKEY)
//    FROM CUSTOMER left outer join ORDERS on C_CUSTKEY = O_CUSTKEY
//         AND O_COMMENT not like '%special%requests%'
//    GROUP BY C_CUSTKEY
//  ) AS C_ORDERS (C_CUSTKEY, O_COUNT)
//  GROUP BY O_COUNT
//
// Or, in plain English:
//
//  For each customer we count the number of "normal" orders he made,
//  and then produce a histogram of the number of customers (y axis)
//  that ordered a given number of products (x axis).
//
// NOTE: The left outer join keeps track of unmatched data on the left.
// NOTE: `%` match any number of characters
// NOTE: SQL LIKE statement is not case sensitive
//
// The schemata are:                             NEEDED?
//
// CUSTOMER(C_CUSTKEY     INTEGER NOT NULL,         √
//          C_NAME        VARCHAR(25) NOT NULL,
//          C_ADDRESS     VARCHAR(40) NOT NULL,
//          C_NATIONKEY   INTEGER NOT NULL,
//          C_PHONE       CHAR(15) NOT NULL,
//          C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
//          C_MKTSEGMENT  CHAR(10) NOT NULL,
//          C_COMMENT     VARCHAR(117) NOT NULL)
//
// ORDERS(O_ORDERKEY       INTEGER NOT NULL,
//        O_CUSTKEY        INTEGER NOT NULL,        √
//        O_ORDERSTATUS    CHAR(1) NOT NULL,
//        O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
//        O_ORDERDATE      DATE NOT NULL,
//        O_ORDERPRIORITY  CHAR(15) NOT NULL,
//        O_CLERK          CHAR(15) NOT NULL,
//        O_SHIPPRIORITY   INTEGER NOT NULL,
//        O_COMMENT        VARCHAR(79) NOT NULL)    √
//
object Project1 {
  private type Path = String

  private case class Settings(
    input: Path,
    output: Path
  ) {
    def getCustomersPath = input + "/customer.tbl"
    def getOrdersPath    = input + "/orders.tbl"

    // TODO extend for Project2
    def getOutputPath = output + "/out"
  }

  // Read settings from command line arguments
  private def extractSettings(args: Array[String]): Settings = {
    if (args.length != 2)
      throw new IllegalArgumentException("Incorrect number of parameters")

    Settings(args(0), args(1))
  }

  // Extract the customer's key
  private def extractCustomer(record: String): Int = {
    val custkey = record takeWhile { _ != '|' }
    custkey.toInt
  }

  // Determine whether an order's comment is refering to a special request
  // (useful for pattern matching)
  private object IsNormal {
    def unapply(comment: String): Option[String] = test(comment) match {
      case true  => None
      case false => Some(comment)
    }

    val pattern = """.*special.*requests.*""".r.pattern
    def test(comment: String) = pattern.matcher(comment.toLowerCase).matches
  }

  // Extract orders's client id and its comment
  private def extractOrders(record: String): (Int, String) = {
    val fields  = record split '|'
    val custkey = fields(1).toInt
    val comment = fields(8)

    (custkey, comment)
  }

  // Main program procedure:
  // Read the data from file and save the result to file in the same CSV format
  def main(args: Array[String]): Unit = {
    // Create a Spark context using default settings
    val s    = extractSettings(args)
    val conf = new SparkConf().setAppName("mantogni.Project1")
    val sc   = new SparkContext(conf)

    // Load customers' key
    val customers = {
      val source = sc textFile s.getCustomersPath
      val keys   = source map extractCustomer

      // NOTE: we add an extra column to the data
      //       to have a (K, V) RDD for PairRDD features
      keys map { key => (key, 0) } // this value is actually not used
    }

    // Load normal orders and start counting
    val orders = {
      val source  = sc textFile s.getOrdersPath
      val records = source map extractOrders

      records collect { case (key, IsNormal(comment)) =>
        (key, 1) // initial count is one
      }
    }

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
    val histogram = join reduceByKey { _ + _ }
    histogram map { case (k, v) => s"$k|$v" } saveAsTextFile s.getOutputPath
  }
}
