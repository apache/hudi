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

package org.apache.hudi.crypot;

import org.apache.hudi.crypot.kms.InMemoryKMS;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;

public class TestParquetEncryption extends HoodieClientTestBase {

  private SparkSession spark;

  private HashMap<String, String> commonOpts = new HashMap();

  @BeforeEach
  public void setUp() throws Exception {
    commonOpts.put("hoodie.insert.shuffle.parallelism", "4");
    commonOpts.put("hoodie.upsert.shuffle.parallelism", "4");
    commonOpts.put("hoodie.bulkinsert.shuffle.parallelism", "4");
    commonOpts.put("hoodie.delete.shuffle.parallelism", "4");
    commonOpts.put("hoodie.datasource.write.recordkey.field", "_row_key");
    commonOpts.put("hoodie.datasource.write.partitionpath.field", "partition");
    commonOpts.put("hoodie.datasource.write.precombine.field", "timestamp");
    commonOpts.put("hoodie.table.name", "hoodie_test");

    initPath();
    initSparkContexts();
    spark = sqlContext.sparkSession();
    initTestDataGenerator();
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
  }

  @Test
  public void testEncryption() {

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    jsc.hadoopConfiguration().set("parquet.crypto.factory.class", PropertiesDrivenCryptoFactory.class.getName());
    jsc.hadoopConfiguration().set("parquet.encryption.kms.client.class", InMemoryKMS.class.getName());
    jsc.hadoopConfiguration().set("parquet.encryption.footer.key", "k1");
    jsc.hadoopConfiguration().set("parquet.encryption.column.keys", "k2:rider,_row_key");
    jsc.hadoopConfiguration().set("hoodie.parquet.encryption.key.list", "k1:AAECAwQFBgcICQoLDA0ODw==, k2:AAECAAECAAECAAECAAECAA==");

    List<String> records1 = recordsToStrings(dataGen.generateInserts("000", 100));
    Dataset<Row> inputDF1 = spark.read().json(jsc.parallelize(records1, 2));

    inputDF1.write().format("org.apache.hudi")
        .options(commonOpts)
        .mode(SaveMode.Overwrite)
        .save(basePath);


    //1. no footer key, no column key
    jsc.hadoopConfiguration().clear();
    Assertions.assertThrows(Exception.class, () -> spark.read().format("org.apache.hudi").load(basePath).count());
    Assertions.assertThrows(Exception.class, () -> spark.read().format("org.apache.hudi").load(basePath).select("rider").show(1));

    //2 has footer key, has column key
    jsc.hadoopConfiguration().set("parquet.crypto.factory.class", PropertiesDrivenCryptoFactory.class.getName());
    jsc.hadoopConfiguration().set("parquet.encryption.kms.client.class", InMemoryKMS.class.getName());
    jsc.hadoopConfiguration().set("hoodie.parquet.encryption.key.list", "k1:AAECAwQFBgcICQoLDA0ODw==, k2:AAECAAECAAECAAECAAECAA==");
    Assertions.assertEquals(100, spark.read().format("org.apache.hudi").load(basePath).count());
    Assertions.assertDoesNotThrow(() -> spark.read().format("org.apache.hudi").load(basePath).select("rider").show(1));
  }
}