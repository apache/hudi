package com.uber.hoodie.bench.writer;

import com.uber.hoodie.AvroConversionUtils;
import java.io.IOException;
import javax.ws.rs.NotSupportedException;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Spark based avro sink writer. We don't use this yet since we cannot control result file size.
 */
public class SparkAvroDeltaInputWriter implements DeltaInputWriter<JavaRDD<GenericRecord>> {

  private static final String AVRO_FORMAT_PACKAGE = "com.databricks.spark.avro";
  public SparkSession sparkSession;
  private String schemaStr;
  // TODO : the base path has to be a new path every time for spark avro
  private String basePath;

  public SparkAvroDeltaInputWriter(SparkSession sparkSession, String schemaStr, String basePath) {
    this.sparkSession = sparkSession;
    this.schemaStr = schemaStr;
    this.basePath = basePath;
  }

  @Override
  public void writeData(JavaRDD<GenericRecord> iData) throws IOException {
    AvroConversionUtils.createDataFrame(iData.rdd(), schemaStr, sparkSession).write()
        .format(AVRO_FORMAT_PACKAGE).save(basePath);
  }

  @Override
  public boolean canWrite() {
    throw new NotSupportedException("not applicable for spark based writer");
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public WriteStats getWriteStats() {
    throw new NotSupportedException("not applicable for spark based writer");
  }

}
