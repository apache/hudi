package org.apache.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}

/**
 * This is an extension of [[FileScnaRDD]], implementing [[HoodieUnsafeRDD]]
 */
class HoodieFileScanRDD(@transient private val sparkSession: SparkSession,
                        read: PartitionedFile => Iterator[InternalRow],
                        @transient filePartitions: Seq[FilePartition])
  extends FileScanRDD(sparkSession, read, filePartitions)
    with HoodieUnsafeRDD {

  override final def collect(): Array[InternalRow] = super[HoodieUnsafeRDD].collect()
}
