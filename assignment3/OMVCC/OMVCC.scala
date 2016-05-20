import java.util._

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
  /* TODO -- your versioned key-value store data structure */

  private var startAndCommitTimestampGen: Long = 0
  private var transactionIdGen: Long = 1L << 62

  case class NoSuchKeyException(xact: Long, key: Int) extends Exception
  case class BadWriteException(xact: Long, key: Int, value: Int) extends Exception
  case class BadCommitException(xact: Long) extends Exception
  case class NoSuchXactException(xact: Long) extends Exception

  // returns transaction id == logical start timestamp
  def begin: Long = {
    startAndCommitTimestampGen += 1   // SHOULD BE USED
    transactionIdGen += 1             // SHOULD BE USED
    transactionIdGen
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
  // writing fails when attempting to update a value with an uncommitted
  // version and the xact is aborted before throwing a BadWriteException
  @throws(classOf[Exception])
  def write(xact: Long, key: Int, value: Int) {
    /* TODO */
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
}

