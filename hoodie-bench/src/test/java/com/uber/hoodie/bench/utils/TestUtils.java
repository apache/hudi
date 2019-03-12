package com.uber.hoodie.bench.utils;

import com.uber.hoodie.AvroConversionUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class TestUtils {

  /**
   * Create a RDD of generic records for testing purposes
   */
  public static JavaRDD<GenericRecord> makeRDD(JavaSparkContext jsc, int numRecords) {
    return jsc.parallelize(generateGenericRecords(numRecords));
  }

  /**
   * Generate generic records
   */
  public static List<GenericRecord> generateGenericRecords(int numRecords) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    return dataGenerator.generateGenericRecords(numRecords);
  }

  public static void createAvroFiles(JavaSparkContext jsc, SparkSession sparkSession, String basePath, int numFiles,
      int numRecordsPerFile) {
    Schema schema = HoodieTestDataGenerator.avroSchema;
    for (int i = 0; i < numFiles; i++) {
      JavaRDD<GenericRecord> rdd = makeRDD(jsc, numRecordsPerFile);
      AvroConversionUtils.createDataFrame(rdd.rdd(), schema.toString(), sparkSession).write()
          .format("com.databricks.spark.avro").save(basePath + "/" + i);
    }
  }

  public static Schema getSchema() {
    return HoodieTestDataGenerator.avroSchema;
  }

}
