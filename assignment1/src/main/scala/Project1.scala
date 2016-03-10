package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Project1 {
  type Path = String

  case class Settings(
    input: Path,
    output: Path
  ) {
    def getCustomersPath = input + "/customer.tbl"
    def getOrdersPath    = input + "/orders.tbl"
  }

  def extractSettings(args: Array[String]): Settings = {
    if (args.length != 2)
      throw new IllegalArgumentException("Incorrect number of parameters")

    Settings(args(0), args(1))
  }

  def main(args: Array[String]): Unit = {
    val s = extractSettings(args)

    val conf = new SparkConf().setAppName("mantogni.Project1")
    val sc   = new SparkContext(conf)

    // Query is:
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
    // In plain English:
    //
    //  For each customer we count the number of "normal" orders he made,
    //  and then produce a histogram of the number of customers (y axis)
    //  that ordered a given number of products (x axis).
    //
    // NOTE: The left outer join keeps track of unmatched data on the left.
    // NOTE: `%` match any number of characters
    // NOTE: SQL LIKE statement is not case sensitive

    // FIXME: can we assume that the custkey is a primary key?

    // Load orders + customers
    val customers = sc.textFile(s.getCustomersPath)
    val orders    = sc.textFile(s.getOrdersPath)

    // println("# orders: " + orders.count())
    // println("# customers: " + customers.count())

    // Extract the required fields from both schemas,
    // which are:                                       NEEDED?
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

    // Extract the minimal schema for customers in the query: (key)
    val custkeys = customers map { record =>
      val fields  = record split '|'
      val custkey = fields(0).toInt
      // TODO this might be optimised into `record takeWhile { _ != '|' }`

      custkey
    }

    // Extract the minimal schema for orders in the query: (key, comment)
    val ordersMini = orders map { record =>
      val fields  = record split '|'
      val custkey = fields(1).toInt
      val comment = fields(8)

      (custkey, comment)
    }

    // Filter based on comment before doing the actual join
    val ordersMiniFiltered = ordersMini filter { case (_, comment) =>
      val regex = """.*special.*requests.*""".r
      val pattern = regex.pattern
      // TODO it might be better to build this pattern once per node

      pattern.matcher(comment.toLowerCase).matches
    }
    // TODO it might be better to merge this filtering into the previous
    // mapping by using `collect` with a partial function, or use
    // flatMap with Option.

/* ERROR!

   RDD transformations and actions can only be invoked by the driver,
   not inside of other transformations; for example,
      rdd1.map(x => rdd2.values.count() * x)
   is invalid because the values transformation and count action cannot
   be performed inside of the rdd1.map transformation. For more
   information, see SPARK-5063.

    // Apply outer join: for each record on the left, we filter the ones
    // on the right that match the predicate on `custkey`. Here we directly
    // extract the COUNT instead of doing it later.
    val records = custkeys map { custkey =>
      val orders = ordersMiniFiltered filter { case (key, _) => key == custkey }

      (custkey, orders.count)
    }
*/

    // Create a view (key, key) to use Spark's join
    val custkeys2 = custkeys keyBy identity

    // Count the # of orders per customer, dropping comments
    val ordersCount = ordersMiniFiltered.aggregateByKey(0)((acc, _) => acc + 1, _ + _)

    // Apply the left outer join then mapping values (to drop the extra key),
    // resulting in (key, count)
    val join = custkeys2 leftOuterJoin ordersCount mapValues {
      case (_, Some(count)) => count
      case (_, None)        => 0
    }

    // Swap key <-> count schema to group by count
    val records = join map { case (key, count) => (count, key) }

    // Count the number of customers for a given order count
    val answer = records.aggregateByKey(0)((acc, _) => acc + 1, _ + _)

    println(answer.collectAsMap mkString "\n")
  }
}
