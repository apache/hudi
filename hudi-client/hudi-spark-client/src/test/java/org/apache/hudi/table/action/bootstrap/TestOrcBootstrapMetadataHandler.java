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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.OrcUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link OrcBootstrapMetadataHandler}.
 *
 * <p>NOTE: the end-to-end ORC bootstrap coverage lives in {@code TestOrcBootstrap}, which has been
 * disabled since HUDI-7353. This test drives the handler directly instead, so that the ORC arm of
 * {@link MetadataBootstrapHandlerFactory} keeps being exercised.
 */
public class TestOrcBootstrapMetadataHandler extends HoodieClientTestBase {

  private static final String SOURCE_ORC_SCHEMA = "struct<_row_key:string,rider:string,fare:int>";
  private static final String RECORD_KEY_FIELD = "_row_key";
  private static final String SRC_PARTITION_PATH = "2020/04/01";
  // Deliberately different from the source partition path, the way a BootstrapPartitionPathTranslator
  // would rewrite it, so that the two never get transposed unnoticed.
  private static final String PARTITION_PATH = "2020-04-01";
  private static final int NUM_SOURCE_RECORDS = 5;

  private String bootstrapBasePath;
  private StoragePath sourceFilePath;
  private HoodieFileStatus srcFileStatus;

  @BeforeEach
  public void initBootstrapSource() throws IOException {
    bootstrapBasePath = tempDir.resolve("bootstrap_source").toAbsolutePath().toString();
    sourceFilePath = new StoragePath(bootstrapBasePath + "/" + SRC_PARTITION_PATH,
        "src_0" + HoodieFileFormat.ORC.getFileExtension());
    writeOrcSourceFile();

    // Re-initialize the target table so that its base file format is ORC and it points at the ORC source.
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, bootstrapBasePath,
        HoodieFileFormat.ORC, NonpartitionedKeyGenerator.class.getCanonicalName());

    List<Pair<String, List<HoodieFileStatus>>> leafFolders = BootstrapUtils.getAllLeafFoldersWithFiles(
        HoodieFileFormat.ORC, metaClient.getStorage(), bootstrapBasePath, context);
    assertEquals(1, leafFolders.size());
    assertEquals(SRC_PARTITION_PATH, leafFolders.get(0).getKey());
    srcFileStatus = leafFolders.get(0).getValue().get(0);
  }

  @Test
  public void testGetSchemaConvertsOrcTypeDescription() throws IOException {
    HoodieWriteConfig config = bootstrapConfigBuilder().build();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    OrcBootstrapMetadataHandler handler = getOrcHandler(config, table);

    HoodieSchema schema = handler.getSchema(sourceFilePath);

    assertEquals(HoodieSchemaType.RECORD, schema.getType());
    assertEquals(Arrays.asList(RECORD_KEY_FIELD, "rider", "fare"),
        schema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList()));
    assertEquals(Arrays.asList(HoodieSchemaType.STRING, HoodieSchemaType.STRING, HoodieSchemaType.INT),
        schema.getFields().stream().map(f -> f.schema().getType()).collect(Collectors.toList()));
  }

  @Test
  public void testRunMetadataBootstrapWritesSkeletonFile() throws IOException {
    HoodieWriteConfig config = bootstrapConfigBuilder().build();
    assertEquals(HoodieRecord.HoodieRecordType.AVRO, config.getRecordMerger().getRecordType());
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    // Dispatch is purely by the source file's .orc extension.
    assertInstanceOf(OrcBootstrapMetadataHandler.class,
        MetadataBootstrapHandlerFactory.getMetadataHandler(config, table, srcFileStatus));

    // The handle relies on the spark task context, so run the bootstrap inside a task the way
    // SparkBootstrapCommitActionExecutor does.
    KeyGeneratorInterface keyGenerator = newKeyGenerator();
    List<BootstrapWriteStatus> writeStatuses = context
        .parallelize(Collections.singletonList(srcFileStatus), 1)
        .map(fileStatus -> MetadataBootstrapHandlerFactory.getMetadataHandler(config, table, fileStatus)
            .runMetadataBootstrap(SRC_PARTITION_PATH, PARTITION_PATH, keyGenerator))
        .collectAsList();
    assertEquals(1, writeStatuses.size());
    BootstrapWriteStatus writeStatus = writeStatuses.get(0);

    assertFalse(writeStatus.hasErrors());
    assertEquals(PARTITION_PATH, writeStatus.getPartitionPath());

    HoodieWriteStat stat = writeStatus.getStat();
    assertEquals(NUM_SOURCE_RECORDS, stat.getNumWrites());
    assertEquals(NUM_SOURCE_RECORDS, stat.getNumInserts());
    assertEquals(0, stat.getTotalWriteErrors());
    assertEquals(writeStatus.getFileId(), stat.getFileId());

    // The skeleton base file is written in the table's base file format, ie ORC.
    StoragePath skeletonPath = new StoragePath(basePath, stat.getPath());
    assertTrue(skeletonPath.getName().endsWith(HoodieFileFormat.ORC.getFileExtension()));
    assertTrue(stat.getPath().startsWith(PARTITION_PATH + "/"));
    assertTrue(metaClient.getStorage().exists(skeletonPath),
        "Skeleton base file " + skeletonPath + " should exist");

    // Only the record keys of the source file are carried over into the skeleton file.
    Set<Pair<String, Long>> writtenKeys =
        new OrcUtils().filterRowKeys(metaClient.getStorage(), skeletonPath, Collections.emptySet());
    assertEquals(expectedRecordKeys(),
        writtenKeys.stream().map(Pair::getLeft).sorted().collect(Collectors.toList()));

    BootstrapFileMapping mapping = writeStatus.getBootstrapSourceFileMapping();
    assertEquals(bootstrapBasePath, mapping.getBootstrapBasePath());
    assertEquals(SRC_PARTITION_PATH, mapping.getBootstrapPartitionPath());
    assertEquals(srcFileStatus, mapping.getBootstrapFileStatus());
    assertEquals(PARTITION_PATH, mapping.getPartitionPath());
    assertEquals(writeStatus.getFileId(), mapping.getFileId());
  }

  @Test
  public void testExecuteBootstrapRejectsSparkRecordType() {
    HoodieWriteConfig config = bootstrapConfigBuilder()
        .withRecordMergeImplClasses(DefaultSparkRecordMerger.class.getName())
        .build();
    assertEquals(HoodieRecord.HoodieRecordType.SPARK, config.getRecordMerger().getRecordType());
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    OrcBootstrapMetadataHandler handler = getOrcHandler(config, table);

    // The spark reader is not wired up for ORC bootstrap, the handler bails out before touching any handle.
    assertThrows(UnsupportedOperationException.class,
        () -> handler.executeBootstrap(null, sourceFilePath, newKeyGenerator(), PARTITION_PATH, null));
  }

  private OrcBootstrapMetadataHandler getOrcHandler(HoodieWriteConfig config, HoodieTable table) {
    BootstrapMetadataHandler handler =
        MetadataBootstrapHandlerFactory.getMetadataHandler(config, table, srcFileStatus);
    return assertInstanceOf(OrcBootstrapMetadataHandler.class, handler);
  }

  private HoodieWriteConfig.Builder bootstrapConfigBuilder() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable("test_orc_bootstrap")
        .withEmbeddedTimelineServerEnabled(false)
        .withWriteStatusClass(BootstrapWriteStatus.class)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withBootstrapConfig(HoodieBootstrapConfig.newBuilder()
            .withBootstrapBasePath(bootstrapBasePath).build());
  }

  private KeyGeneratorInterface newKeyGenerator() {
    TypedProperties props = new TypedProperties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), RECORD_KEY_FIELD);
    return new NonpartitionedKeyGenerator(props);
  }

  private static List<String> expectedRecordKeys() {
    return IntStream.range(0, NUM_SOURCE_RECORDS).mapToObj(i -> "key" + i).sorted().collect(Collectors.toList());
  }

  private void writeOrcSourceFile() throws IOException {
    TypeDescription orcSchema = TypeDescription.fromString(SOURCE_ORC_SCHEMA);
    OrcFile.WriterOptions options = OrcFile.writerOptions(storageConf.unwrapAs(Configuration.class))
        .setSchema(orcSchema).compress(CompressionKind.ZLIB);
    try (Writer writer = OrcFile.createWriter(new Path(sourceFilePath.toUri()), options)) {
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      BytesColumnVector rowKeys = (BytesColumnVector) batch.cols[0];
      BytesColumnVector riders = (BytesColumnVector) batch.cols[1];
      LongColumnVector fares = (LongColumnVector) batch.cols[2];
      for (int r = 0; r < NUM_SOURCE_RECORDS; r++) {
        int row = batch.size++;
        rowKeys.setVal(row, getUTF8Bytes("key" + r));
        riders.setVal(row, getUTF8Bytes("rider" + r));
        fares.vector[row] = r;
      }
      writer.addRowBatch(batch);
    }
  }
}
