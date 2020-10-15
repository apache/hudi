package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Random;

public class AddColumnTransform implements Transformer {

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    rowDataset.sqlContext().udf().register("add_column_func", new CustomeUDF(), DataTypes.StringType);
    return rowDataset.withColumn("ds", functions.callUDF("add_column_func", rowDataset.col("time")));
  }

  public static class CustomeUDF implements UDF1<String, String> {
    @Override
    public String call(String str) throws Exception {
      return new Random().toString();
    }
  }
}