package org.apache.spark.sql.exchange.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ConvertToLocalRelation, PushDownPredicate, ReorderJoin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SharedSparkSession extends BeforeAndAfterEach
  with BeforeAndAfterAll
  with Eventually { self: Suite =>

  protected def sparkConf = {
    Logger.getLogger("org").setLevel(Level.OFF)
    new SparkConf()
      //.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, PushDownPredicate.ruleName)
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ReorderJoin.ruleName)
  }

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _spark: TestSparkSession = null

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def spark: SparkSession = _spark

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(sparkConf)
  }

  /**
   * Initialize the [[TestSparkSession]].  Generally, this is just called from
   * beforeAll; however, in test using styles other than FunSuite, there is
   * often code that relies on the session between test group constructs and
   * the actual tests, which may need this session.  It is purely a semantic
   * difference, but semantically, it makes more sense to call
   * 'initializeSession' between a 'describe' and an 'it' call than it does to
   * call 'beforeAll'.
   */
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSparkSession
    }
  }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  protected override def beforeAll(): Unit = {
    initializeSession()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    //DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Clear all persistent datasets after each test
    spark.sharedState.cacheManager.clearCache()
    // files can be closed from other threads, so wait a bit
    // normally this doesn't take more than 1s
//    eventually(timeout(10.seconds), interval(2.seconds)) {
//      DebugFilesystem.assertNoOpenStreams()
//    }
  }
}

