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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_WRITE_FAILURE_STRATEGY;

public final class ErrorTableUtils {

  public static Option<BaseErrorTableWriter> getErrorTableWriter(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
      TypedProperties props, JavaSparkContext jssc, FileSystem fs) {
    String errorTableWriterClass = props.getString(ERROR_TABLE_WRITE_CLASS.key());
    ValidationUtils.checkState(!StringUtils.isNullOrEmpty(errorTableWriterClass),
        "Missing error table config " + ERROR_TABLE_WRITE_CLASS);

    Class<?>[] argClassArr = new Class[] {HoodieDeltaStreamer.Config.class,
        SparkSession.class, TypedProperties.class, JavaSparkContext.class, FileSystem.class};
    String errMsg = "Unable to instantiate ErrorTableWriter with arguments type " + Arrays.toString(argClassArr);
    ValidationUtils.checkArgument(ReflectionUtils.hasConstructor(BaseErrorTableWriter.class.getName(), argClassArr), errMsg);

    try {
      return Option.of((BaseErrorTableWriter) ReflectionUtils.getClass(errorTableWriterClass).getConstructor(argClassArr)
          .newInstance(cfg, sparkSession, props, jssc, fs));
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new HoodieException(errMsg, e);
    }
  }

  public static HoodieErrorTableConfig.ErrorWriteFailureStrategy getErrorWriteFailureStrategy(
      TypedProperties props) {
    String writeFailureStrategy = props.getString(ERROR_TABLE_WRITE_FAILURE_STRATEGY.key());
    return HoodieErrorTableConfig.ErrorWriteFailureStrategy.valueOf(writeFailureStrategy);
  }
}
