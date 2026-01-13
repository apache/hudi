package org.apache.spark.sql.hudi.common

import org.apache.hudi.sync.common.model.PartitionValueExtractor

import java.util

class TestSlashPartitionValueExtractor extends PartitionValueExtractor {

  override def extractPartitionValuesInPath(partitionPath: String): util.List[String] = {
    val partitionSplitsSeq = partitionPath.split("/")
    java.util.Arrays.asList(partitionSplitsSeq: _*)
  }
}
