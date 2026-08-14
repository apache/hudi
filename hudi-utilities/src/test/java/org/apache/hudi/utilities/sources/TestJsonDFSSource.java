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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.AbstractDFSSourceTestBase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic tests for {@link JsonDFSSource}.
 */
public class TestJsonDFSSource extends AbstractDFSSourceTestBase {

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    this.dfsRoot = basePath + "/jsonFiles";
    this.fileSuffix = ".json";
  }

  @Override
  public Source prepareDFSSource(TypedProperties props) {
    props.setProperty("hoodie.streamer.source.dfs.root", dfsRoot);
    return new JsonDFSSource(props, jsc, sparkSession, schemaProvider);
  }

  @Override
  protected Option<TypedProperties> getSourceFormatAdapterProps() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES.key(), "true");
    return Option.of(properties);
  }

  @Override
  public void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException {
    UtilitiesTestBase.Helpers.saveStringsToDFS(
        Helpers.jsonifyRecords(records), storage, path.toString());
  }

  @Test
  public void testCorruptedSourceFile() throws IOException {
    fs.mkdirs(new Path(dfsRoot));
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieStreamerConfig.ROW_THROW_EXPLICIT_EXCEPTIONS.key(), "true");
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(prepareDFSSource(props), Option.empty(), Option.of(props));
    generateOneFile("1", "000", 10);
    generateOneFile("2", "000", 10);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(generateOneFile("3", "000", 10), true);

    FileStatus file1Status = files.next();
    InputBatch<Dataset<Row>> batch = sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    corruptFile(file1Status.getPath());
    assertTrue(batch.getBatch().isPresent());
    Throwable t = assertThrows(Exception.class,
        () -> batch.getBatch().get().limit(30).collect());
    while (t != null) {
      if (t instanceof SchemaCompatibilityException) {
        return;
      }
      t = t.getCause();
    }
    throw new AssertionError("Exception does not have SchemaCompatibility in its trace", t);
  }

  protected void corruptFile(Path path) throws IOException {
    PrintStream os = new PrintStream(fs.appendFile(path).build());
    os.println("🤷‍");
    os.flush();
    os.close();
  }
}
