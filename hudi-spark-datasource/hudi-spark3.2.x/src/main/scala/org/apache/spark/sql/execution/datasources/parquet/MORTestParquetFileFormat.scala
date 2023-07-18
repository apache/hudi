package org.apache.spark.sql.execution.datasources.parquet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieBootstrapRelation.createPartitionedFile
import org.apache.hudi.{HoodieSparkUtils, InternalRowBroadcast}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.net.URI

class MORTestParquetFileFormat(private val shouldAppendPartitionValues: Boolean) extends Spark32HoodieParquetFileFormat(shouldAppendPartitionValues) {


  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataReader =   super.buildReaderWithPartitionValues(sparkSession, StructType(dataSchema.fields ++ partitionSchema.fields), StructType(Seq.empty), StructType(requiredSchema.fields ++ partitionSchema.fields), filters, options, hadoopConf, shouldAppendOverride = false)
    val bootstrapDataReader =  super.buildBaseReaderWithPartitionValues(sparkSession, StructType(dataSchema.fields.filterNot(sf => isMetaField(sf.name))),
      partitionSchema, StructType(requiredSchema.fields.filterNot(sf => isMetaField(sf.name))), Seq.empty, options, hadoopConf, shouldAppendOverride = true)
    val skeletonReader = super.buildSkeletonReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty),
      HoodieSparkUtils.getMetaSchema, Seq.empty, options, hadoopConf, shouldAppendOverride = false)

    (file: PartitionedFile) => {
      if (file.partitionValues.isInstanceOf[InternalRowBroadcast]) {
        val broadcast = file.partitionValues.asInstanceOf[InternalRowBroadcast]
        val filePath = new Path(new URI(file.filePath))
        val fileSlice =  broadcast.getSlice(FSUtils.getFileId(filePath.getName)).get
        val partitionValues = broadcast.getInternalRow
        val baseFile = fileSlice.getBaseFile.get()
        if (baseFile.getBootstrapBaseFile.isPresent) {
          val dataFile = createPartitionedFile(
            partitionValues, baseFile.getBootstrapBaseFile.get.getHadoopPath,
            0, baseFile.getBootstrapBaseFile.get().getFileLen)
          val skeletonFile = createPartitionedFile(
            InternalRow.empty, baseFile.getHadoopPath, 0, baseFile.getFileLen)
          merge(skeletonReader(skeletonFile), bootstrapDataReader(dataFile), requiredSchema.toAttributes ++ partitionSchema.toAttributes)
        } else {
          val dataFile = createPartitionedFile(InternalRow.empty, baseFile.getHadoopPath, 0, baseFile.getFileLen)
          dataReader(dataFile)
        }
      } else {
        dataReader(file)
      }
    }
  }

  def merge(skeletonFileIterator: Iterator[InternalRow], dataFileIterator: Iterator[InternalRow], mergedSchema: Seq[Attribute]): Iterator[InternalRow] = {
    val combinedRow = new JoinedRow()
    val unsafeProjection = GenerateUnsafeProjection.generate(mergedSchema, mergedSchema)
    skeletonFileIterator.zip(dataFileIterator).map(i => unsafeProjection(combinedRow(i._1, i._2)))
  }
}
