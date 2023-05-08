package org.apache.hudi.client.bootstrap;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.datasources.SparkParsePartitionUtil;
import org.apache.spark.sql.internal.SQLConf;

public class PartitionUtils {


  public static Object[] getPartitionFieldVals(Option<String[]> partitionFields,
                                               String partitionPath,
                                               String bootstrapBasePath,
                                               Schema writerSchema,
                                               Configuration hadoopConf) {
    if (!partitionFields.isPresent()) {
      return new Object[0];
    }
    SparkParsePartitionUtil sparkParsePartitionUtil = SparkAdapterSupport$.MODULE$.sparkAdapter().getSparkParsePartitionUtil();
    return HoodieSparkUtils.parsePartitionColumnValues(
        partitionFields.get(),
        partitionPath,
        new CachingPath(bootstrapBasePath),
        AvroConversionUtils.convertAvroSchemaToStructType(writerSchema),
        hadoopConf.get("timeZone", SQLConf.get().sessionLocalTimeZone()),
        sparkParsePartitionUtil,
        hadoopConf.getBoolean("spark.sql.sources.validatePartitionColumns", true));
  }
}
