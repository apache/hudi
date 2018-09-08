/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import static org.junit.Assert.assertEquals;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.UtilitiesTestBase;
import com.uber.hoodie.utilities.schema.FilebasedSchemaProvider;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic tests against all subclasses of {@link DFSSource}
 */
public class TestDFSSource extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testJsonDFSSource() throws IOException {
    dfs.mkdirs(new Path(dfsBasePath + "/jsonFiles"));
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.dfs.root", dfsBasePath + "/jsonFiles");
    JsonDFSSource jsonSource = new JsonDFSSource(props, jsc, schemaProvider);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Optional.empty(), jsonSource.fetchNewData(Optional.empty(), Long.MAX_VALUE).getKey());
    UtilitiesTestBase.Helpers.saveStringsToDFS(
        Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 100)), dfs,
        dfsBasePath + "/jsonFiles/1.json");
    assertEquals(Optional.empty(), jsonSource.fetchNewData(Optional.empty(), 10).getKey());
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch1 = jsonSource.fetchNewData(Optional.empty(), 1000000);
    assertEquals(100, fetch1.getKey().get().count());

    // 2. Produce new data, extract new data
    UtilitiesTestBase.Helpers.saveStringsToDFS(
        Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 10000)),
        dfs, dfsBasePath + "/jsonFiles/2.json");
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch2 = jsonSource.fetchNewData(
        Optional.of(fetch1.getValue()), Long.MAX_VALUE);
    assertEquals(10000, fetch2.getKey().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch3 = jsonSource.fetchNewData(
        Optional.of(fetch1.getValue()), Long.MAX_VALUE);
    assertEquals(10000, fetch3.getKey().get().count());
    assertEquals(fetch2.getValue(), fetch3.getValue());

    // 4. Extract with latest checkpoint => no new data returned
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch4 = jsonSource.fetchNewData(
        Optional.of(fetch2.getValue()), Long.MAX_VALUE);
    assertEquals(Optional.empty(), fetch4.getKey());
  }
}
