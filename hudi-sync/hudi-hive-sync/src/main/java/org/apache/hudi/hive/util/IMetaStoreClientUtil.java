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

package org.apache.hudi.hive.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hudi.exception.HoodieException;

import java.lang.reflect.InvocationTargetException;

/**
 * Utilities for fetching Hive metastore client.
 */
public class IMetaStoreClientUtil {

  /**
   * Returns the Hive metastore client with given Hive conf.
   */
  public static IMetaStoreClient getMSC(HiveConf hiveConf) throws HiveException, MetaException {
    IMetaStoreClient metaStoreClient;
    try {
      metaStoreClient = ((Hive) Hive.class.getMethod("getWithoutRegisterFns", HiveConf.class).invoke(null, hiveConf)).getMSC();
    } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException ex) {
      try {
        metaStoreClient = Hive.get(hiveConf).getMSC();
      } catch (RuntimeException e) {
        if (e.getMessage() != null && (e.getMessage().contains("not org.apache.hudi.org.apache.hadoop")
            || e.getMessage().contains("MetaStoreFilterHook")
            || e.getMessage().contains("DefaultMetaStoreFilterHookImpl"))) {
          throw new HoodieException(
              "Hive Metastore compatibility issue detected. This usually happens due to:\n"
                  + "  1. Hive version mismatch\n"
                  + "  2. Conflicting Hive libraries in classpath\n"
                  + "  3. Incompatible hudi-spark-bundle version\n\n"
                  + "To resolve:\n"
                  + "  - For Hive 2.x: Use hudi-spark-bundle with 'hive2' classifier\n"
                  + "  - For Hive 3.x: Use hudi-spark-bundle with 'hive3' classifier\n"
                  + "  - Ensure no conflicting Hive jars in Spark classpath\n"
                  + "  - Check: https://hudi.apache.org/docs/syncing_metastore\n\n"
                  + "Technical details: " + e.getMessage(), e);
        }
        throw e;
      }
    }
    return metaStoreClient;
  }
}
