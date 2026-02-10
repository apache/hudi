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

package org.apache.hudi.examples.quickstart;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Paths;

import static org.apache.hudi.examples.quickstart.HoodieSparkQuickstart.runQuickstart;

public class TestHoodieSparkQuickstart implements SparkProvider {
  protected static HoodieSparkEngineContext context;

  private static SparkSession spark;
  private static SQLContext sqlContext;
  private static JavaSparkContext jsc;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  @Override
  public SparkSession spark() {
    return spark;
  }

  @Override
  public SparkConf conf() {
    return conf(SparkClientFunctionalTestHarness.getSparkSqlConf());
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }

  @Override
  public HoodieSparkEngineContext context() {
    return context;
  }

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  public String tablePath(String tableName) {
    return Paths.get(basePath(), tableName).toString();
  }

  @BeforeEach
  public synchronized void runBeforeEach() {
    initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
  }

  @Test
  public void printClasspath() throws Exception {
    // Write to both console and file
    String outputFile = "/tmp/hudi-classpath-debug.txt";
    java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(outputFile));

    System.err.println("=== CLASSPATH DEBUG START ===");
    writer.println("=== CLASSPATH DEBUG START ===");

    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(System.getProperty("path.separator"));

    System.err.println("Total classpath entries: " + paths.length);
    writer.println("Total classpath entries: " + paths.length);

    boolean foundHudiCommon = false;
    for (String path : paths) {
      writer.println(path);
      if (path.contains("hudi-common")) {
        System.err.println("FOUND hudi-common: " + path);
        writer.println("FOUND hudi-common: " + path);
        foundHudiCommon = true;
      }
    }

    System.err.println("hudi-common found in classpath: " + foundHudiCommon);
    writer.println("hudi-common found in classpath: " + foundHudiCommon);

    // Try to load the class
    System.err.println("=== Attempting to load AvroSchemaRepair ===");
    writer.println("=== Attempting to load AvroSchemaRepair ===");
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.schema.AvroSchemaRepair");
      System.err.println("SUCCESS: AvroSchemaRepair loaded from: " +
          clazz.getProtectionDomain().getCodeSource().getLocation());
      writer.println("SUCCESS: AvroSchemaRepair loaded from: " +
          clazz.getProtectionDomain().getCodeSource().getLocation());
    } catch (ClassNotFoundException e) {
      System.err.println("FAILED: AvroSchemaRepair NOT FOUND");
      writer.println("FAILED: AvroSchemaRepair NOT FOUND");
      e.printStackTrace(System.err);
      e.printStackTrace(writer);
    }

    // Check classloader hierarchy
    System.err.println("=== CLASSLOADER HIERARCHY ===");
    writer.println("=== CLASSLOADER HIERARCHY ===");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    int level = 0;
    while (cl != null) {
      System.err.println("Level " + level + ": " + cl.getClass().getName());
      writer.println("Level " + level + ": " + cl.getClass().getName());
      cl = cl.getParent();
      level++;
    }

    System.err.println("=== CLASSPATH DEBUG END ===");
    writer.println("=== CLASSPATH DEBUG END ===");
    writer.close();

    System.err.println("Output written to: " + outputFile);
  }

  @Test
  public void testHoodieSparkQuickstart() {
    String tableName = "spark_quick_start";
    String tablePath = tablePath(tableName);

    try {
      runQuickstart(jsc, spark, tableName, tablePath);
    } finally {
      Utils.deleteRecursively(new File(tablePath));
    }
  }
}
