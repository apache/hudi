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

package org.apache.hudi.aws.sync;

import org.apache.hudi.hive.testutils.HiveTestUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.time.format.DateTimeFormatter;

/**
 * Utility class for creating Hudi test tables with proper schema and metadata.
 * Provides reusable methods for integration tests that need realistic Hudi table structures.
 */
public class HudiTestTableUtils {

  /**
   * Creates a Hudi COW table with schema using HiveTestUtil infrastructure.
   * This method properly initializes the required static fields in HiveTestUtil
   * and creates a complete table structure with schema files.
   *
   * @param tablePath the path where the table should be created
   * @param instantTime the instant time for the table creation
   * @param schemaFileName the schema file name (e.g., "/simple-test-evolved.avsc")
   * @throws Exception if table creation fails
   */
  public static void createHudiTableWithSchema(String tablePath, String instantTime, String schemaFileName) throws Exception {
    // Save original HiveTestUtil static values
    java.lang.reflect.Field configField = HiveTestUtil.class.getDeclaredField("configuration");
    configField.setAccessible(true);
    Object originalConfig = configField.get(null);

    java.lang.reflect.Field basePathField = HiveTestUtil.class.getDeclaredField("basePath");
    basePathField.setAccessible(true);
    String originalBasePath = (String) basePathField.get(null);

    java.lang.reflect.Field fileSystemField = HiveTestUtil.class.getDeclaredField("fileSystem");
    fileSystemField.setAccessible(true);
    Object originalFileSystem = fileSystemField.get(null);

    java.lang.reflect.Field dtfOutField = HiveTestUtil.class.getDeclaredField("dtfOut");
    dtfOutField.setAccessible(true);
    Object originalDtfOut = dtfOutField.get(null);

    try {
      // Initialize minimal required HiveTestUtil fields
      configField.set(null, new Configuration());
      basePathField.set(null, tablePath);
      fileSystemField.set(null, FileSystem.get(new Configuration()));
      dtfOutField.set(null, DateTimeFormatter.ofPattern("yyyy/MM/dd"));

      // Create table with schema
      HiveTestUtil.createCOWTableWithSchema(instantTime, schemaFileName);

    } finally {
      // Restore original values
      configField.set(null, originalConfig);
      basePathField.set(null, originalBasePath);
      fileSystemField.set(null, originalFileSystem);
      dtfOutField.set(null, originalDtfOut);
    }
  }

  /**
   * Creates a Hudi COW table with the default schema for testing.
   * Uses a sensible default instant time and the evolved schema file.
   *
   * @param tablePath the path where the table should be created
   * @throws Exception if table creation fails
   */
  public static void createDefaultHudiTable(String tablePath) throws Exception {
    String instantTime = "20231108220000000";
    String schemaFileName = "/simple-test-evolved.avsc";
    createHudiTableWithSchema(tablePath, instantTime, schemaFileName);
  }
}