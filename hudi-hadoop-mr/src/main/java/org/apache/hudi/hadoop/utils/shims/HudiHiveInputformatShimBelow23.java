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

package org.apache.hudi.hadoop.utils.shims;

import java.io.IOException;
import java.lang.reflect.Method;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When the hive version is less than hive version 2.3
 */
public class HudiHiveInputformatShimBelow23 implements HudiHiveInputformatShim {

  public static final Logger LOG = LoggerFactory.getLogger(HudiHiveInputformatShimBelow23.class);

  private static HudiHiveInputformatShimBelow23 INSTANCE = new HudiHiveInputformatShimBelow23();
  private static Method PUSH_PROJECT_METHOD;

  static {
    try {
      PUSH_PROJECT_METHOD = HiveInputFormat.class.getDeclaredMethod("pushProjectionsAndFilters", JobConf.class,
              Class.class, String.class, String.class);
      PUSH_PROJECT_METHOD.setAccessible(true);
    } catch (Exception e) {
      LOG.trace("can not find  HiveInputFormat.pushProjectionsAndFilters", e);
    }
  }

  public static HudiHiveInputformatShim getInstance() {
    return INSTANCE;
  }

  @Override
  public void invokePushProjectAndFilters(JobConf job, Class<?> inputFormatClass, Path splitPath, HiveInputFormat inputFormat)
      throws IOException {
    try {
      PUSH_PROJECT_METHOD.invoke(inputFormat, job, inputFormatClass, splitPath.toString(), splitPath.toUri().getPath());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
