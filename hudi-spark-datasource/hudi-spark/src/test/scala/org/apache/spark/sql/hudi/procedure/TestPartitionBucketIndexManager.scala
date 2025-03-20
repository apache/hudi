package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.bucket.PartitionBucketIndexUtils

class TestPartitionBucketIndexManager extends HoodieSparkProcedureTestBase {

  test("Case1: Test Call drop_partition Procedure For Multiple Partitions: '*' stands for all partitions in leaf partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val metaClient = getTableMetaClient(tablePath)
      val expressions = "\\d{4}-(06-(01|17|18)|11-(01|10|11)),256"
      val rule ="regex"
      val defaultBucketNumber = 10
      PartitionBucketIndexUtils.initHashingConfig(metaClient, expressions, rule, defaultBucketNumber, null)

      spark.sql(s"""call PartitionBucketIndexManager(table => '$tableName', show-config => 'true')""").collect()

    }
  }

  private def getTableMetaClient(tablePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
      .build()
  }

}
