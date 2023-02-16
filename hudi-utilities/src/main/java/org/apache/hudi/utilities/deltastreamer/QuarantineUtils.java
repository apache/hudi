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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public final class QuarantineUtils {

  public static BaseQuarantineTableWriter getQuarantineTableWriter(
      String quarantineTableWriterClass, HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
      TypedProperties props, JavaSparkContext jssc, FileSystem fs) {
    Class<?>[] argClassArr = new Class[] {HoodieDeltaStreamer.Config.class,
        SparkSession.class, TypedProperties.class, JavaSparkContext.class, FileSystem.class};
    String errMsg = "Unable to instantiate QuarantineTableWriter with arguments type " + Arrays.toString(argClassArr);
    ValidationUtils.checkArgument(ReflectionUtils.hasConstructor(BaseQuarantineTableWriter.class.getName(), argClassArr), errMsg);

    try {
      return (BaseQuarantineTableWriter) ReflectionUtils.getClass(quarantineTableWriterClass).getConstructor(argClassArr)
          .newInstance(cfg, sparkSession, props, jssc, fs);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new HoodieException(errMsg, e);
    }
  }

}
