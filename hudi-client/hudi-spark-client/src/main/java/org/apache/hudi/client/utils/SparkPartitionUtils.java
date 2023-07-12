/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.utils;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.datasources.SparkParsePartitionUtil;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

public class SparkPartitionUtils {

  public static Object[] getPartitionFieldVals(Option<String[]> partitionFields,
                                               String partitionPath,
                                               String basePath,
                                               Schema writerSchema,
                                               Configuration hadoopConf,
                                               Boolean typeInference) {
    if (!partitionFields.isPresent()) {
      return new Object[0];
    }
    return getPartitionFieldValsStruct(partitionFields.get(), partitionPath, basePath,
        AvroConversionUtils.convertAvroSchemaToStructType(writerSchema), hadoopConf, typeInference);
  }

  public static Object[] getPartitionFieldValsStruct(String[] partitionFields,
                                               String partitionPath,
                                               String basePath,
                                               StructType writerSchema,
                                               Configuration hadoopConf,
                                               Boolean typeInference) {
    SparkParsePartitionUtil sparkParsePartitionUtil = SparkAdapterSupport$.MODULE$.sparkAdapter().getSparkParsePartitionUtil();
    return HoodieSparkUtils.parsePartitionColumnValues(
        partitionFields,
        partitionPath,
        new CachingPath(basePath),
        writerSchema,
        hadoopConf.get("timeZone", SQLConf.get().sessionLocalTimeZone()),
        sparkParsePartitionUtil,
        hadoopConf.getBoolean("spark.sql.sources.validatePartitionColumns", true),
        typeInference);
  }
}
