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
  import scala.collection.mutable.{ Map => MutableMap }
  import scala.annotation.tailrec

  private var startAndCommitTimestampGen: Long = 0
  private var transactionIdGen: Long = 1L << 62

  private case class Transaction(startTimestamp: Long) {
    var undoBuffer: List[Int] = Nil // list of keys updated by this xact
  }

  //lList of active xacts
  private val xacts = MutableMap[Long, Transaction]()

  sealed abstract class Version
  case class CommittedVersion(value: Int, timestamp: Long) extends Version
  case class TemporaryVersion(value: Int, xactOwner: Long) extends Version

  // key-(value+version) storage
  private val storage = MutableMap[Int, List[Version]]().withDefaultValue(Nil)


  case class NoSuchKeyException(xact: Long, key: Int) extends Exception
  case class BadWriteException(xact: Long, key: Int, value: Int) extends Exception
  case class BadCommitException(xact: Long) extends Exception
  case class NoSuchXactException(xact: Long) extends Exception

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
    /* TODO */
    0 // FIX THIS
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
    val transaction = getTransaction(xact)

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
      case CommittedVersion(_, timestamp) :: _ if timestamp > transaction.startTimestamp =>
        abort()

      // Otherwise we recurse
      case (v: CommittedVersion) :: vs =>
        rec(vs, v :: acc)
    }

    val versions    = storage(key)
    val newVersions = rec(versions, Nil) // might abort in there

    storage += key -> newVersions
  }

  // attempt to commit the given xact;
  // if xact is invalid, a NoSuchXactException is thrown;
  // on failures, the xact is aborted and a BadCommitException is thrown
  @throws(classOf[Exception])
  def commit(xact: Long) {
    val isValid: Boolean = false  // FIX THIS
    /* TODO */
    if (isValid) {
      startAndCommitTimestampGen += 1 //SHOULD BE USED
    }
  }

  // remove any write pending validation emitted by the given xact
  // if xact is invalid, a NoSuchXactException is thrown
  @throws(classOf[Exception])
  def rollback(xact: Long) {
    /* TODO */
  }

  // return the Transaction corresponding to the given xact if any,
  // throw a NoSuchXactException if none exists
  private def getTransaction(xact: Long) =
    xacts get xact getOrElse { throw NoSuchXactException(xact) }
}

