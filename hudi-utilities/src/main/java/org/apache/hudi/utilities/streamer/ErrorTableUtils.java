/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_WRITE_FAILURE_STRATEGY;
import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
import static org.apache.spark.sql.functions.lit;

public final class ErrorTableUtils {
  public static Option<BaseErrorTableWriter> getErrorTableWriter(HoodieStreamer.Config cfg,
                                                                 SparkSession sparkSession,
                                                                 TypedProperties props,
                                                                 HoodieSparkEngineContext hoodieSparkContext,
                                                                 FileSystem fs,
                                                                 Option<HoodieIngestionMetrics> metrics) {
    String errorTableWriterClass = props.getString(ERROR_TABLE_WRITE_CLASS.key());
    ValidationUtils.checkState(!StringUtils.isNullOrEmpty(errorTableWriterClass),
                               "Missing error table config " + ERROR_TABLE_WRITE_CLASS);

    Class<?>[] legacyArgClass = new Class[]{HoodieStreamer.Config.class,
        SparkSession.class, TypedProperties.class, HoodieSparkEngineContext.class, FileSystem.class};
    Class<?>[] argClass = new Class[] {HoodieStreamer.Config.class,
        SparkSession.class, TypedProperties.class, HoodieSparkEngineContext.class, FileSystem.class, Option.class};

    try {
      if (ReflectionUtils.hasConstructor(errorTableWriterClass, argClass)) {
        return Option.of((BaseErrorTableWriter) ReflectionUtils.getClass(errorTableWriterClass).getConstructor(argClass)
            .newInstance(cfg, sparkSession, props, hoodieSparkContext, fs, metrics));
      } else if (ReflectionUtils.hasConstructor(errorTableWriterClass, legacyArgClass)) {
        return Option.of((BaseErrorTableWriter) ReflectionUtils.getClass(errorTableWriterClass).getConstructor(legacyArgClass)
            .newInstance(cfg, sparkSession, props, hoodieSparkContext, fs));
      } else {
        throw new HoodieException(String.format("The configured Error table class %s does not have the appropriate constructor", errorTableWriterClass));
      }
    } catch (Exception exception) {
      throw new HoodieException("Could not load Error Table class " + BaseErrorTableWriter.class.getName(), exception);
    }
  }

  public static HoodieErrorTableConfig.ErrorWriteFailureStrategy getErrorWriteFailureStrategy(
      TypedProperties props) {
    String writeFailureStrategy = props.getString(ERROR_TABLE_WRITE_FAILURE_STRATEGY.key(), ERROR_TABLE_WRITE_FAILURE_STRATEGY.defaultValue());
    return HoodieErrorTableConfig.ErrorWriteFailureStrategy.valueOf(writeFailureStrategy);
  }

  /**
   * validates for constraints on ErrorRecordColumn when ErrorTable enabled configs are set.
   * @param dataset
   */
  public static void validate(Dataset<Row> dataset) {
    if (!isErrorTableCorruptRecordColumnPresent(dataset)) {
      throw new HoodieValidationException(String.format("Invalid condition, columnName=%s "
              + "is not present in transformer " + "output schema", ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
    }
  }

  public static Dataset<Row> addNullValueErrorTableCorruptRecordColumn(Dataset<Row> dataset) {
    if (!isErrorTableCorruptRecordColumnPresent(dataset)) {
      dataset = dataset.withColumn(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, lit(null));
    }
    return dataset;
  }

  private static boolean isErrorTableCorruptRecordColumnPresent(Dataset<Row> dataset) {
    return Arrays.stream(dataset.columns()).anyMatch(col -> col.equals(ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
  }
}
