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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOrcUtils {

  @Test
  void testMetadataAndFormatHelpers(@TempDir Path tempDir) throws Exception {
    StoragePath file = new StoragePath(tempDir.resolve("metadata.orc").toUri());
    HoodieStorage storage = HoodieTestUtils.getStorage(file);
    OrcUtils utils = new OrcUtils();
    Properties properties = new Properties();
    properties.setProperty("first", "one");
    properties.setProperty("second", "two");

    utils.writeMetaFile(storage, file, properties);

    assertEquals(HoodieFileFormat.ORC, utils.getFormat());
    assertEquals(0, utils.getRowCount(storage, file));
    assertEquals(Collections.singletonMap("first", "one"),
        utils.readFooter(storage, true, file, "first"));
    assertTrue(utils.readFooter(storage, false, file, "missing").isEmpty());
    assertThrows(MetadataNotFoundException.class,
        () -> utils.readFooter(storage, true, file, "missing"));
    assertEquals(HoodieSchemaUtils.getRecordKeySchema().getFields().get(0).name(),
        utils.readSchema(storage, file).getFields().get(0).name());
    assertThrows(UnsupportedOperationException.class,
        () -> utils.readColumnStatsFromMetadata(storage, file, Collections.emptyList(), HoodieIndexVersion.V1));
    assertThrows(UnsupportedOperationException.class, () -> utils.serializeRecordsToLogBlock(
        storage, Collections.<HoodieRecord>emptyList(), null, null, null, Collections.emptyMap()));
    assertThrows(UnsupportedOperationException.class, () -> utils.serializeRecordsToLogBlock(
        storage, Collections.<HoodieRecord>emptyIterator(), HoodieRecord.HoodieRecordType.AVRO,
        null, null, null, Collections.emptyMap()));

    StoragePath missing = new StoragePath(tempDir.resolve("missing.orc").toUri());
    try (ClosableIterator<Pair<HoodieKey, Long>> iterator = utils.fetchRecordKeysWithPositions(
        storage, missing, Option.empty(), Option.empty())) {
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  void testRecordReadingKeyFilteringAndPositions(@TempDir Path tempDir) throws Exception {
    StoragePath file = new StoragePath(tempDir.resolve("records.orc").toUri());
    HoodieStorage storage = HoodieTestUtils.getStorage(file);
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(schema);
    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem((FileSystem) storage.getFileSystem()).setSchema(orcSchema);
    try (Writer writer = OrcFile.createWriter(HadoopFSUtils.convertToHadoopPath(file), options)) {
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      BytesColumnVector keys = (BytesColumnVector) batch.cols[0];
      for (String key : Arrays.asList("key-1", "key-2", "key-3")) {
        keys.setVal(batch.size++, StringUtils.getUTF8Bytes(key));
      }
      writer.addRowBatch(batch);
    }

    OrcUtils utils = new OrcUtils();
    assertEquals(3, utils.getRowCount(storage, file));
    assertEquals(3, utils.readAvroRecords(storage, file).size());
    assertEquals(3, utils.readAvroRecords(storage, file, schema).size());

    Set<Pair<String, Long>> all = utils.filterRowKeys(storage, file, Collections.emptySet());
    assertEquals(3, all.size());
    Set<Pair<String, Long>> selected = utils.filterRowKeys(
        storage, file, new HashSet<>(Collections.singletonList("key-2")));
    assertEquals(Collections.singleton(Pair.of("key-2", 1L)), selected);

    try (ClosableIterator<Pair<HoodieKey, Long>> iterator = utils.fetchRecordKeysWithPositions(
        storage, file, Option.empty(), Option.of("partition"))) {
      assertTrue(iterator.hasNext());
      Pair<HoodieKey, Long> first = iterator.next();
      assertEquals("key-1", first.getLeft().getRecordKey());
      assertEquals("partition", first.getLeft().getPartitionPath());
      assertEquals(0L, first.getRight());
    }
  }

  @Test
  void testReadSchemaPrefersEmbeddedAvroSchema(@TempDir Path tempDir) throws Exception {
    StoragePath file = new StoragePath(tempDir.resolve("schema.orc").toUri());
    HoodieStorage storage = HoodieTestUtils.getStorage(file);
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(schema);
    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem((FileSystem) storage.getFileSystem()).setSchema(orcSchema);
    try (Writer writer = OrcFile.createWriter(HadoopFSUtils.convertToHadoopPath(file), options)) {
      writer.addUserMetadata("orc.avro.schema", ByteBuffer.wrap(StringUtils.getUTF8Bytes(schema.toString())));
    }

    assertEquals(schema, new OrcUtils().readSchema(storage, file));
  }
}
