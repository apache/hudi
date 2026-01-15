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

package org.apache.hudi.utils;

import org.apache.hudi.DataSourceReadOptions$;
import org.apache.hudi.DataSourceWriteOptions$;

import java.util.Arrays;
import java.util.List;

public class HoodieSparkConfigs {

  private static final List<Object> SPARK_CONFIG_OBJECTS = Arrays.asList(DataSourceReadOptions$.MODULE$, DataSourceWriteOptions$.MODULE$);

  HoodieSparkConfigs() {
  }

  public static List<Object> getSparkConfigObjects() {
    return SPARK_CONFIG_OBJECTS;
  }

  public static String name(Object sparkConfigObject) {
    if (DataSourceReadOptions$.MODULE$.equals(sparkConfigObject)) {
      return "Read Options";
    } else if (DataSourceWriteOptions$.MODULE$.equals(sparkConfigObject)) {
      return "Write Options";
    }
    throw new IllegalArgumentException("Unknown Spark Object " + sparkConfigObject.getClass().getName());
  }

  public static String className() {
    return "org.apache.hudi.DataSourceOptions.scala";
  }

  public static String description(Object sparkConfigObject) {
    if (DataSourceReadOptions$.MODULE$.equals(sparkConfigObject)) {
      return "Options useful for reading tables via `read.format.option(...)`\n";
    } else if (DataSourceWriteOptions$.MODULE$.equals(sparkConfigObject)) {
      return "You can pass down any of the WriteClient level configs directly using `options()` or `option(k,v)` methods.\n" +
          "\n" +
          "```java\n" +
          "inputDF.write()\n" +
          ".format(\"org.apache.hudi\")\n" +
          ".options(clientOpts) // any of the Hudi client opts can be passed in as well\n" +
          ".option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), \"_row_key\")\n" +
          ".option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), \"partition\")\n" +
          ".option(HoodieTableConfig.ORDERING_FIELDS(), \"timestamp\")\n" +
          ".option(HoodieWriteConfig.TABLE_NAME, tableName)\n" +
          ".mode(SaveMode.Append)\n" +
          ".save(basePath);\n" +
          "```\n" +
          "\n" +
          "Options useful for writing tables via `write.format.option(...)`\n";
    }
    throw new IllegalArgumentException("Unknown Spark Object " + sparkConfigObject.getClass().getName());
  }
}
