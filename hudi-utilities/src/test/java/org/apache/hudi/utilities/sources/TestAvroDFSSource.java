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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.sources.AbstractDFSSourceTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic tests for {@link TestAvroDFSSource}.
 */
public class TestAvroDFSSource extends AbstractDFSSourceTestBase {

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    this.dfsRoot = basePath + "/avroFiles";
    this.fileSuffix = ".avro";
  }

  @Override
  protected Source prepareDFSSource(TypedProperties props) {
    props.setProperty("hoodie.streamer.source.dfs.root", dfsRoot);
    try {
      return new AvroDFSSource(props, jsc, sparkSession, schemaProvider);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  protected void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException {
    Helpers.saveAvroToDFS(Helpers.toGenericRecords(records), path);
  }

  /**
   * Regression test: when a single batch contains files with additive schema evolution
   * (one file has the base schema, another has the same fields plus an extra field with a
   * default), reading via {@link AvroDFSSource} configured with the wider reader schema must
   * (a) not fail, (b) return all records from both files, and (c) materialize the wider field
   * as the default for records from the narrow file and as the written value for records from
   * the wider file. Locks in Avro reader/writer schema-resolution behavior.
   */
  @Test
  public void testAdditiveSchemaEvolutionAcrossFiles() throws Exception {
    // Use a unique subdirectory because basePath is static and shared with
    // the parent testReadingFromSource, which writes 10000+ records into dfsRoot
    // and would otherwise pollute this test's read.
    String additiveRoot = basePath + "/avroFilesAdditive";
    fs.mkdirs(new Path(additiveRoot));

    Schema narrowSchema = HoodieTestDataGenerator.AVRO_SCHEMA;
    Schema widerSchema = addStringFieldWithDefault(narrowSchema, "additive_field", "DEFAULT");

    // File A: narrow writer schema, no additive_field.
    int narrowCount = 30;
    List<GenericRecord> narrowRecords = Helpers.toGenericRecords(
        dataGenerator.generateInserts("000", narrowCount));
    Path pathA = new Path(additiveRoot, "narrow" + fileSuffix);
    Helpers.saveAvroToDFS(narrowRecords, pathA, narrowSchema);

    // File B: wider writer schema, additive_field set to a known value.
    int wideCount = 20;
    List<GenericRecord> wideRecords = new ArrayList<>();
    for (GenericRecord narrow : Helpers.toGenericRecords(
        dataGenerator.generateInserts("001", wideCount))) {
      GenericRecord wide = new GenericData.Record(widerSchema);
      for (Schema.Field f : narrowSchema.getFields()) {
        wide.put(f.name(), narrow.get(f.name()));
      }
      wide.put("additive_field", "WRITTEN");
      wideRecords.add(wide);
    }
    Path pathB = new Path(additiveRoot, "wider" + fileSuffix);
    Helpers.saveAvroToDFS(wideRecords, pathB, widerSchema);

    // Write the wider schema to DFS and point a fresh schema provider at it so the source's
    // reader schema is wider than file A's writer schema.
    Path widerSchemaFile = new Path(basePath + "/wider-source.avsc");
    try (OutputStream out = fs.create(widerSchemaFile)) {
      out.write(widerSchema.toString(true).getBytes(StandardCharsets.UTF_8));
    }
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.dfs.root", additiveRoot);
    props.setProperty("hoodie.streamer.schemaprovider.source.schema.file", widerSchemaFile.toString());
    FilebasedSchemaProvider widerProvider = new FilebasedSchemaProvider(props, jsc);
    AvroDFSSource source = new AvroDFSSource(props, jsc, sparkSession, widerProvider);

    SourceFormatAdapter adapter = new SourceFormatAdapter(source);
    JavaRDD<GenericRecord> fetched =
        adapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch().get();
    List<GenericRecord> read = fetched.collect();

    assertEquals(narrowCount + wideCount, read.size(),
        "Both narrow and wider files should be read in the same batch");

    long defaulted = read.stream()
        .filter(r -> "DEFAULT".equals(String.valueOf(r.get("additive_field"))))
        .count();
    long preserved = read.stream()
        .filter(r -> "WRITTEN".equals(String.valueOf(r.get("additive_field"))))
        .count();
    assertEquals(narrowCount, defaulted,
        "Records from the narrow file should get the wider reader schema's default for additive_field");
    assertEquals(wideCount, preserved,
        "Records from the wider file should preserve the written value of additive_field");
  }

  /**
   * Returns a copy of {@code base} with one extra optional string field appended, defaulting
   * to {@code defaultValue}. The new field has a non-null default so Avro's schema-resolution
   * can fill it in for records read with this schema but written under {@code base}.
   */
  private static Schema addStringFieldWithDefault(Schema base, String fieldName, String defaultValue) {
    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field f : base.getFields()) {
      fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
    }
    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.STRING), null, defaultValue));
    Schema wider = Schema.createRecord(base.getName(), base.getDoc(), base.getNamespace(), false);
    wider.setFields(fields);
    return wider;
  }
}
