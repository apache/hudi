package org.apache.hudi.functional

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.log4j.LogManager
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConversions.asScalaBuffer

class TestIncrementalReadWithFullTableScan extends HoodieClientTestBase {

  var spark: SparkSession = null
  private val log = LogManager.getLogger(classOf[TestIncrementalReadWithFullTableScan])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1"
  )


  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testFailEarlyForIncrViewQueryForNonExistingFiles(tableType: HoodieTableType): Unit = {
    // Create 10 commits
    for (i <- 1 to 10) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.write.format("org.apache.hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
        .option("hoodie.cleaner.commits.retained", "3")
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "5")
        .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val hoodieMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build()
    /**
     * State of timeline after 10 commits
     * +------------------+--------------------------------------+
     * |     Archived     |            Active Timeline           |
     * +------------------+--------------+-----------------------+
     * | C0   C1   C2  C3 |    C4   C5   |   C6    C7   C8   C9  |
     * +------------------+--------------+-----------------------+
     * |          Data cleaned           |  Data exists in table |
     * +---------------------------------+-----------------------+
     */

    val completedCommits = hoodieMetaClient.getCommitsTimeline.filterCompletedInstants() // C4 to C9
    //Anything less than 2 is a valid commit in the sense no cleanup has been done for those commit files
    var startTs = completedCommits.nthInstant(0).get().getTimestamp //C4
    var endTs = completedCommits.nthInstant(1).get().getTimestamp //C5

    //Calling without the fallback should result in Path does not exist
    var hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), startTs)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTs)
      .load(basePath)

    val msg = "Should fail with Path does not exist"
    tableType match {
      case HoodieTableType.COPY_ON_WRITE =>
        assertThrows(classOf[AnalysisException], new Executable {
          override def execute(): Unit = {
            hoodieIncViewDF.count()
          }
        }, msg)
      case HoodieTableType.MERGE_ON_READ =>
        val exp = assertThrows(classOf[SparkException], new Executable {
          override def execute(): Unit = {
            hoodieIncViewDF.count()
          }
        }, msg)
        assertTrue(exp.getMessage.contains("FileNotFoundException"))
    }


    //Should work with fallback enabled
    hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), startTs)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTs)
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key(), "true")
      .load(basePath)
    assertEquals(100, hoodieIncViewDF.count())

    //Test out for archived commits
    val archivedInstants = hoodieMetaClient.getArchivedTimeline.getInstants.distinct().toArray
    startTs = archivedInstants(0).asInstanceOf[HoodieInstant].getTimestamp //C0
    endTs = completedCommits.nthInstant(1).get().getTimestamp //C5

    //Calling without the fallback should result in Path does not exist
    hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), startTs)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTs)
      .load(basePath)

    tableType match {
      case HoodieTableType.COPY_ON_WRITE =>
        assertThrows(classOf[AnalysisException], new Executable {
          override def execute(): Unit = {
            hoodieIncViewDF.count()
          }
        }, msg)
      case HoodieTableType.MERGE_ON_READ =>
        val exp = assertThrows(classOf[SparkException], new Executable {
          override def execute(): Unit = {
            hoodieIncViewDF.count()
          }
        }, msg)
        assertTrue(exp.getMessage.contains("FileNotFoundException"))
    }

    //Should work with fallback enabled
    hoodieIncViewDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), startTs)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTs)
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key(), "true")
      .load(basePath)
    assertEquals(500, hoodieIncViewDF.count())
  }
}
