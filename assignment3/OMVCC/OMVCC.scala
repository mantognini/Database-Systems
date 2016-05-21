// IMPORTANT -- THIS IS INDIVIDUAL WORK. ABSOLUTELY NO COLLABORATION!!!


// - implement a (main-memory) data store with OMVCC.
// - objects are <int, int> key-value pairs.
// - if an operation is to be refused by the OMVCC protocol,
//   undo its xact (what work does this take?) and throw an exception.
// - garbage collection of versions is optional.
// - throw exceptions when necessary, such as when we try to:
//   + execute an operation in a transaction that is not running
//   + read a nonexisting key
//   + write into a key where it already has an uncommitted version
// - you may but do not need to create different exceptions for operations that
//   are refused and for operations that are refused and cause the Xact to be
//   aborted. Keep it simple!
// - keep the interface, we want to test automatically!
object OMVCC {
  import scala.collection.mutable.{ Map => MutableMap, Set => MutableSet }
  import scala.annotation.tailrec

  private case class Transaction(startTimestamp: Long) {
    val undoBuffer = MutableSet[Int]() // set of keys updated by this xact
    val readPreds  = MutableSet[Int]() // set of key
    // TODO add predicates for modquery

    var commitTimestamp: Long = -1 // unset

    def isReadOnly = undoBuffer.isEmpty
  }


  sealed abstract class Version
  case class CommittedVersion(value: Int, timestamp: Long) extends Version
  case class TemporaryVersion(value: Int, xactOwner: Long) extends Version


  case class NoSuchKeyException(xact: Long, key: Int) extends Exception
  case class BadWriteException(xact: Long, key: Int, value: Int) extends Exception
  case class BadCommitException(xact: Long) extends Exception
  case class NoSuchXactException(xact: Long) extends Exception


  // list of active xacts
  private val xacts = MutableMap[Long, Transaction]()
  private val commits = MutableSet[Transaction]()

  // key-(value+version) storage
  private val storage = MutableMap[Int, List[Version]]().withDefaultValue(Nil)

  private var startAndCommitTimestampGen: Long = 0
  private var transactionIdGen: Long = 1L << 62


  // returns transaction id == logical start timestamp
  def begin: Long = {
    startAndCommitTimestampGen += 1   // SHOULD BE USED
    transactionIdGen += 1             // SHOULD BE USED

    val xact = transactionIdGen

    xacts += xact -> Transaction(startAndCommitTimestampGen)

    xact
  }

  // return value of object key in transaction xact;
  // if xact is invalid, a NoSuchXactException is thrown;
  // if the key doesn't exists, the xact is aborted and
  // a NoSuchKeyException is thrown;
  @throws(classOf[Exception])
  def read(xact: Long, key: Int): Int = {
    val t = getTransaction(xact)

    // (1) either the key was written at least once by the given transaction,
    // (2) or it was committed by another transaction before this one began,
    // (3) or the key was never committed or written by this transaction.

    t.readPreds += key

    if (t.undoBuffer contains key) {
      // (1)
      getTemporaryVersion(t, xact, key)
    } else {
      getMostRecentReadableVersion(t, key) match {
        case None =>
          // (3)
          rollback(xact)
          throw NoSuchKeyException(xact, key)

        case Some(value) =>
          // (2)
          value
      }
    }
  }

  // return the list of values that are congruent modulo k with zero.
  // this is our only kind of query / bulk read.
  // if xact is invalid, a NoSuchXactException is thrown
  @throws(classOf[Exception])
  def modquery(xact: Long, k: Int): java.util.List[Integer] = {
    val l = new java.util.ArrayList[Integer]
    /* TODO */
    l
  }

  // update the value of an existing object identified by key
  // or insert <key,value> for a non-existing key in transaction xact;
  // if xact is invalid, a NoSuchXactException is thrown;
  // writing fails when:
  //  - attempting to update a value with an uncommitted version
  //    from another transaction, or
  //  - a version was committed after the start of the given xact
  // if so, the xact is aborted before throwing a BadWriteException
  @throws(classOf[Exception])
  def write(xact: Long, key: Int, value: Int) {
    val t = getTransaction(xact)

    def abort(): Nothing = {
      rollback(xact)
      throw BadWriteException(xact, key, value)
    }

    @tailrec
    def rec(versions: List[Version], acc: List[Version]): List[Version] = versions match {
      // If no other version exists, we insert a new one
      case Nil =>
        TemporaryVersion(value, xact) :: acc

      // If there is a prevsiously uncommitted version from us, we override it
      case TemporaryVersion(_, owner) :: vs if owner == xact =>
        rec(vs, TemporaryVersion(value, xact) :: acc)

      // If an uncommitted version exists from some other xact, we abort
      case TemporaryVersion(_, _) :: _ =>
        abort()

      // If there is a committed version with a more recent timestamp, we abort
      case CommittedVersion(_, timestamp) :: _ if timestamp > t.startTimestamp =>
        abort()

      // Otherwise we recurse
      case (v: CommittedVersion) :: vs =>
        rec(vs, v :: acc)
    }

    val versions    = storage(key)
    val newVersions = rec(versions, Nil) // might abort in there

    storage += key -> newVersions
    t.undoBuffer += key
  }

  // attempt to commit the given xact;
  // if xact is invalid, a NoSuchXactException is thrown;
  // on failures, the xact is aborted and a BadCommitException is thrown
  @throws(classOf[Exception])
  def commit(xact: Long) {
    // (1) either the transaction has no write operation (read-only) and
    //     no validation is required
    // (2) or it has at least one write operation and
    //     we have to make sure there is no conflict with any running transaction

    val t = getTransaction(xact)

    val isValid =
      if (t.isReadOnly) true
      else {
        val potentialConflicts = commits filter { x => x.commitTimestamp > t.startTimestamp }
        val readsBad = potentialConflicts exists { x => !(x.undoBuffer & t.readPreds).isEmpty }
        // TODO pred on modquery
        val modqueryBad = false

        !(readsBad || modqueryBad)
      }

    if (isValid) {
      startAndCommitTimestampGen += 1 // SHOULD BE USED
      validate(xact, startAndCommitTimestampGen)
    } else {
      rollback(xact)
      throw BadCommitException(xact)
    }
  }

  // remove any write pending validation emitted by the given xact
  // if xact is invalid, a NoSuchXactException is thrown
  @throws(classOf[Exception])
  def rollback(xact: Long) {
    val t = getTransaction(xact)

    for { key <- t.undoBuffer } {
      val versions = storage(key) filter {
        case TemporaryVersion(_, owner) if owner == xact => false
        case _ => true
      }

      storage += key -> versions
    }

    xacts -= xact
  }

  // return the Transaction corresponding to the given xact if any,
  // throw a NoSuchXactException if none exists
  private def getTransaction(xact: Long): Transaction =
    xacts get xact getOrElse { throw NoSuchXactException(xact) }

  // assuming the given transaction passes the checks in commit,
  // transform the uncommitted versions of any written value into
  // durable committed values
  private def validate(xact: Long, commitTimestamp: Long): Unit = {
    val t = getTransaction(xact)

    @tailrec
    def rec(versions: List[Version], acc: List[Version]): List[Version] = versions match {
      case TemporaryVersion(value, owner) :: vs if owner == xact =>
        val newV = CommittedVersion(value, commitTimestamp)
        rec(vs, newV :: acc)

      case Nil => acc
      case v :: vs => rec(vs, v :: acc)
    }

    for { key <- t.undoBuffer } {
      val versions    = storage(key)
      val newVersions = rec(versions, Nil)
      storage += key -> newVersions
    }

    xacts -= xact
    commits += t

    t.commitTimestamp = commitTimestamp
  }

  // read the most recent and visible version that was committed before the
  // given transaction began, if any
  private def getMostRecentReadableVersion(t: Transaction, key: Int): Option[Int] = {
    val vals = storage(key) collect {
      case CommittedVersion(value, ts) if ts < t.startTimestamp => (value, ts)
    } sortBy { _._2 }

    // the more recent value is at the end
    vals match {
      case Nil     => None
      case vs :+ v => Some(v._1)
    }
  }

  // here we assume key is in the undoBuffer of t
  private def getTemporaryVersion(t: Transaction, xact: Long, key: Int): Int = {
    require(t.undoBuffer contains key)

    val opt = storage(key) collectFirst {
      case TemporaryVersion(value, owner) if owner == xact => value
    }

    // the write operation *must* have added a TemporaryVersion into the storage
    assert(opt.isDefined)
    opt.get
  }
}

