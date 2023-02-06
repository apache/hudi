package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieUnsafeUtils$;
import org.apache.spark.sql.Row;

public abstract class BulkInsertPartitionerBase<T> implements BulkInsertPartitioner<T> {

  protected static Dataset<Row> tryCoalesce(Dataset<Row> dataset, int outputSparkPartitions) {
    if (HoodieUnsafeUtils$.MODULE$.getNumPartitions(dataset) != outputSparkPartitions) {
      return dataset.coalesce(outputSparkPartitions);
    }

    return dataset;
  }

  protected static <T> JavaRDD<HoodieRecord<T>> tryCoalesce(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitionsCount) {
    if (records.getNumPartitions() == outputSparkPartitionsCount) {
      // NOTE: In case incoming RDD's partition count matches the target one,
      //       we short-circuit coalescing altogether (since this isn't done by Spark itself)
      return records;
    }

    return records.coalesce(outputSparkPartitionsCount);
  }

}
