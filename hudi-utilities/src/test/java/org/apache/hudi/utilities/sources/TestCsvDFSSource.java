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
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.AbstractDFSSourceTestBase;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;

/**
 * Basic tests for {@link CsvDFSSource}.
 */
public class TestCsvDFSSource extends AbstractDFSSourceTestBase {

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    this.dfsRoot = basePath + "/jsonFiles";
    this.fileSuffix = ".json";
    this.useFlattenedSchema = true;
    this.schemaProvider = new FilebasedSchemaProvider(
        Helpers.setupSchemaOnDFS("streamer-config", "source-flattened.avsc"), jsc);
  }

  @Override
  public Source prepareDFSSource(TypedProperties props) {
    props.setProperty("hoodie.streamer.source.dfs.root", dfsRoot);
    props.setProperty("hoodie.streamer.csv.header", Boolean.toString(true));
    props.setProperty("hoodie.streamer.csv.sep", "\t");
    return new CsvDFSSource(props, jsc, sparkSession, schemaProvider);
  }

  @Override
  public void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException {
    UtilitiesTestBase.Helpers.saveCsvToDFS(
        true, '\t', Helpers.jsonifyRecords(records), fs, path.toString());
  }
}
