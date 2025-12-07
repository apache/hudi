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

package org.apache.hudi.parquet.io;

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieFileMetadataMerger;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.Version;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestHoodieParquetFileBinaryCopier {

  private final int numRecord = 1000;
  private Configuration conf = new Configuration();
  private List<TestFile> inputFiles = null;
  private String outputFile = null;
  private HoodieParquetFileBinaryCopier writer = null;

  @BeforeEach
  public void setUp() {
    outputFile = TestFileBuilder.createTempFile("test");
  }

  @AfterEach
  public void after() {
    if (outputFile != null) {
      TestFileBuilder.deleteTempFile(outputFile);
    }
    if (inputFiles != null) {
      inputFiles.stream().map(TestFile::getFileName).forEach(TestFileBuilder::deleteTempFile);
    }
  }

  @Test
  public void testBasic() throws Exception {
    MessageType schema = createSchema();
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(schema, "GZIP"));
    inputFiles.add(makeTestFile(schema, "GZIP"));

    writer = parquetFileBinaryCopier(schema, "GZIP");
    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, true);
    writer.close();
    verify(schema, CompressionCodecName.GZIP);
  }

  @Test
  public void testTranslateCodec() throws Exception {
    MessageType schema = createSchema();
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(schema, "GZIP"));
    inputFiles.add(makeTestFile(schema, "UNCOMPRESSED"));

    writer = parquetFileBinaryCopier(schema, "ZSTD");
    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, true);
    writer.close();
    verify(schema, CompressionCodecName.ZSTD);
  }

  @Test
  public void testDifferentSchema() throws Exception {
    MessageType schema1 = new MessageType("schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new GroupType(OPTIONAL, "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
    MessageType schema2 = new MessageType("schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"));
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(schema1, "UNCOMPRESSED"));
    inputFiles.add(makeTestFile(schema2, "UNCOMPRESSED"));

    writer = parquetFileBinaryCopier(schema1, "UNCOMPRESSED");
    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema1, true);
    writer.close();
    verify(schema1, CompressionCodecName.UNCOMPRESSED);
  }

  @Test
  public void testConvertLegacy3LevelArrayType() throws Exception {
    MessageType requiredSchema = new MessageType("schema",
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(REPEATED, "list", new PrimitiveType(REPEATED, BINARY, "element"))
        )
    );
    MessageType inputSchema = new MessageType("schema",
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(OPTIONAL, "bag", new PrimitiveType(REPEATED, BINARY, "array"))
        )
    );
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(requiredSchema, "GZIP"));
    inputFiles.add(makeTestFile(inputSchema, "GZIP"));

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());

    writer = parquetFileBinaryCopier(requiredSchema, "GZIP");
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), requiredSchema, true);
    writer.close();

    List<ColumnDescriptor> columns = inputSchema.getColumns();
    Assertions.assertEquals(2, columns.size());
    verifyColumnConvert(columns.get(0), writer::convertLegacy3LevelArray, false, "Name", "Name");
    verifyColumnConvert(columns.get(1), writer::convertLegacy3LevelArray, true, "Links.bag.array", "Links.list.element");
    verify(requiredSchema, CompressionCodecName.GZIP);
  }

  @Test
  public void testConvertLegacy3LevelArrayTypeInNestedField() throws Exception {
    // two column,
    // column 1: Links is ArrayType with every element is ArrayType
    // column 1: Map is MapType, key of map is BinaryType and value of map is ArrayType
    MessageType requiredSchema = new MessageType("schema",
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(REPEATED, "list",
                new GroupType(OPTIONAL, "element", OriginalType.LIST,
                    new GroupType(REPEATED, "list",
                        new PrimitiveType(REPEATED, BINARY, "element")
                    )
                )
            )
        ),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "key_value",
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new GroupType(OPTIONAL, "value", OriginalType.LIST,
                    new GroupType(REPEATED, "list",
                        new PrimitiveType(REPEATED, BINARY, "element")
                    )
                )
            )
        )
    );

    MessageType inputSchema = new MessageType("schema",
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(OPTIONAL, "bag",
                new GroupType(OPTIONAL, "array", OriginalType.LIST,
                    new GroupType(OPTIONAL, "bag",
                        new PrimitiveType(REPEATED, BINARY, "array")
                    )
                )
            )
        ),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "key_value",
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new GroupType(OPTIONAL, "value", OriginalType.LIST,
                    new GroupType(REPEATED, "bag",
                        new PrimitiveType(REPEATED, BINARY, "array")
                    )
                )
            )
        )
    );

    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(requiredSchema, "GZIP"));
    inputFiles.add(makeTestFile(inputSchema, "GZIP"));

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());

    writer = parquetFileBinaryCopier(requiredSchema, "GZIP");
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), requiredSchema, true);
    writer.close();

    List<ColumnDescriptor> columns = inputSchema.getColumns();
    Assertions.assertEquals(3, columns.size());
    verifyColumnConvert(columns.get(0), writer::convertLegacy3LevelArray, true, "Links.bag.array.bag.array", "Links.list.element.list.element");
    verifyColumnConvert(columns.get(1), writer::convertLegacy3LevelArray, false, "Map.key_value.key", "Map.key_value.key");
    verifyColumnConvert(columns.get(2), writer::convertLegacy3LevelArray, true, "Map.key_value.value.bag.array", "Map.key_value.value.list.element");
    verify(requiredSchema, CompressionCodecName.GZIP);
  }

  @Test
  public void testConvertLegacyMapType() throws Exception {
    MessageType requiredSchema = new MessageType("schema",
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "key_value", OriginalType.MAP_KEY_VALUE,
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new PrimitiveType(OPTIONAL, BINARY, "value")
            )
        )
    );
    MessageType inputSchema = new MessageType("schema",
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "map",
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new PrimitiveType(OPTIONAL, BINARY, "value")
            )
        )
    );
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(requiredSchema, "GZIP"));
    inputFiles.add(makeTestFile(inputSchema, "GZIP"));

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());

    writer = parquetFileBinaryCopier(requiredSchema, "GZIP");
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), requiredSchema, true);
    writer.close();

    List<ColumnDescriptor> columns = inputSchema.getColumns();
    Assertions.assertEquals(3, columns.size());
    verifyColumnConvert(columns.get(0), writer::convertLegacyMap, false, "Name", "Name");
    verifyColumnConvert(columns.get(1), writer::convertLegacyMap, true, "Map.map.key", "Map.key_value.key");
    verifyColumnConvert(columns.get(2), writer::convertLegacyMap, true, "Map.map.value", "Map.key_value.value");
    verify(requiredSchema, CompressionCodecName.GZIP);
  }

  @Test
  public void testConvertLegacyMapTypeInNestedField() throws Exception {
    // two column,
    // column 1: Links is ArrayType with every element is Map
    // column 1: Map is MapType, key of map is BinaryType and value of map is Map
    MessageType requiredSchema = new MessageType("schema",
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(REPEATED, "list",
                new GroupType(OPTIONAL, "element", OriginalType.MAP,
                    new GroupType(REPEATED, "key_value",
                        new PrimitiveType(REQUIRED, BINARY, "key"),
                        new PrimitiveType(REPEATED, BINARY, "value")
                    )
                )
            )
        ),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "key_value",
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new GroupType(OPTIONAL, "value", OriginalType.MAP,
                    new GroupType(REPEATED, "key_value",
                        new PrimitiveType(REPEATED, BINARY, "key"),
                        new PrimitiveType(REPEATED, BINARY, "value")
                    )
                )
            )
        )
    );
    MessageType inputSchema = new MessageType("schema",
        new GroupType(OPTIONAL, "Links", OriginalType.LIST,
            new GroupType(REPEATED, "list",
                new GroupType(OPTIONAL, "element", OriginalType.MAP,
                    new GroupType(REPEATED, "map", OriginalType.MAP_KEY_VALUE,
                        new PrimitiveType(REQUIRED, BINARY, "key"),
                        new PrimitiveType(REPEATED, BINARY, "value")
                    )
                )
            )
        ),
        new GroupType(OPTIONAL, "Map", OriginalType.MAP,
            new GroupType(REPEATED, "map", OriginalType.MAP_KEY_VALUE,
                new PrimitiveType(REQUIRED, BINARY, "key"),
                new GroupType(OPTIONAL, "value", OriginalType.MAP,
                    new GroupType(REPEATED, "map", OriginalType.MAP_KEY_VALUE,
                        new PrimitiveType(REPEATED, BINARY, "key"),
                        new PrimitiveType(REPEATED, BINARY, "value")
                    )
                )
            )
        )
    );


    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(requiredSchema, "GZIP"));
    inputFiles.add(makeTestFile(inputSchema, "GZIP"));

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());

    writer = parquetFileBinaryCopier(requiredSchema, "GZIP");
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), requiredSchema, true);
    writer.close();
    List<ColumnDescriptor> columns = inputSchema.getColumns();
    Assertions.assertEquals(5, columns.size());
    verifyColumnConvert(columns.get(0), writer::convertLegacyMap, true, "Links.list.element.map.key", "Links.list.element.key_value.key");
    verifyColumnConvert(columns.get(1), writer::convertLegacyMap, true, "Links.list.element.map.value", "Links.list.element.key_value.value");
    verifyColumnConvert(columns.get(2), writer::convertLegacyMap, true, "Map.map.key", "Map.key_value.key");
    verifyColumnConvert(columns.get(3), writer::convertLegacyMap, true, "Map.map.value.map.key", "Map.key_value.value.key_value.key");
    verifyColumnConvert(columns.get(4), writer::convertLegacyMap, true, "Map.map.value.map.value", "Map.key_value.value.key_value.value");
    verify(requiredSchema, CompressionCodecName.GZIP);
  }

  @Test
  public void testHoodieMetaColumn() throws Exception {
    MessageType schema = new MessageType("schema",
        new PrimitiveType(OPTIONAL, BINARY, FILENAME_METADATA_FIELD),
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new GroupType(OPTIONAL, "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
    inputFiles = new ArrayList<>();
    inputFiles.add(makeTestFile(schema, "GZIP"));
    inputFiles.add(makeTestFile(schema, "GZIP"));

    writer = parquetFileBinaryCopier(schema, "GZIP");
    List<StoragePath> inputPaths = inputFiles.stream()
        .map(TestFile::getFileName)
        .map(StoragePath::new)
        .collect(Collectors.toList());
    StoragePath outputPath = new StoragePath(outputFile);
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, true);
    writer.close();
    verify(schema, CompressionCodecName.GZIP);
  }

  private TestFile makeTestFile(MessageType schema, String codec) throws IOException {
    return new TestFileBuilder(conf, schema)
        .withNumRecord(numRecord)
        .withCodec(codec)
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .build();
  }

  private HoodieParquetFileBinaryCopier parquetFileBinaryCopier(MessageType schema, String codec) {
    CompressionCodecName codecName = CompressionCodecName.fromConf(codec);
    HoodieFileMetadataMerger metadataMerger = new HoodieFileMetadataMerger();
    return new HoodieParquetFileBinaryCopier(conf, codecName, metadataMerger);
  }

  private MessageType createSchema() {
    return new MessageType("schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new GroupType(OPTIONAL, "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private void validateColumnData(MessageType requiredSchema) throws IOException {
    Path outputFilePath = new Path(outputFile);
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), outputFilePath)
        .withConf(conf)
        .build();

    // Get total number of rows from input files
    int totalRows = 0;
    for (TestFile inputFile : inputFiles) {
      totalRows += inputFile.getFileContent().length;
    }

    for (int i = 0; i < totalRows; i++) {
      Group group = reader.read();
      assertNotNull(group);
      SimpleGroup expectGroup = inputFiles.get(i / numRecord).getFileContent()[i % numRecord];
      checkField(requiredSchema, expectGroup, group);
    }

    reader.close();
  }

  private void checkField(GroupType schema, Group expectGroup, Group actualGroup) {
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (expectGroup.getType().getFieldCount() - 1 < i) {
        assertEquals(0, actualGroup.getFieldRepetitionCount(i));
        continue;
      }
      Type type = schema.getType(i);
      if (FILENAME_METADATA_FIELD.equals(type.getName())) {
        Path outputFilePath = new Path(outputFile);
        assertEquals(outputFilePath.getName(), actualGroup.getString(FILENAME_METADATA_FIELD, 0));
        assertNotEquals(actualGroup.getString(FILENAME_METADATA_FIELD, 0),
            expectGroup.getString(FILENAME_METADATA_FIELD, 0));
        continue;
      }

      if (type.isPrimitive()) {
        PrimitiveType primitiveType = (PrimitiveType) type;
        if (primitiveType.getPrimitiveTypeName().equals(INT32)) {
          assertEquals(expectGroup.getInteger(i, 0), actualGroup.getInteger(i, 0));
        } else if (primitiveType.getPrimitiveTypeName().equals(INT64)) {
          assertEquals(expectGroup.getLong(i, 0), actualGroup.getLong(i, 0));
        } else if (primitiveType.getPrimitiveTypeName().equals(BINARY)) {
          assertEquals(expectGroup.getString(i, 0), actualGroup.getString(i, 0));
        }
      } else {
        GroupType groupType = (GroupType) type;
        checkField(groupType, expectGroup.getGroup(i, 0), actualGroup.getGroup(i, 0));
      }
    }
  }

  private ParquetMetadata getFileMetaData(String file) throws IOException {
    return ParquetFileReader.readFooter(conf, new Path(file));
  }

  private void verify(MessageType requiredSchema, CompressionCodecName expectedCodec) throws Exception {
    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType fileSchema = pmd.getFileMetaData().getSchema();
    assertEquals(requiredSchema, fileSchema);

    // Verify codec has been translated
    verifyCodec(outputFile, expectedCodec);

    // Verify the data are not changed
    validateColumnData(requiredSchema);

    // Verify the page index
    validatePageIndex(requiredSchema);

    // Verify original.created.by is preserved
    validateCreatedBy();
  }

  private void verifyCodec(String file, CompressionCodecName expectedCodecs) throws IOException {
    Set<CompressionCodecName> codecs = new HashSet<>();
    ParquetMetadata pmd = getFileMetaData(file);
    for (int i = 0; i < pmd.getBlocks().size(); i++) {
      BlockMetaData block = pmd.getBlocks().get(i);
      for (int j = 0; j < block.getColumns().size(); ++j) {
        ColumnChunkMetaData columnChunkMetaData = block.getColumns().get(j);
        codecs.add(columnChunkMetaData.getCodec());
      }
    }
    assertEquals(new HashSet<CompressionCodecName>() {
      {
        add(expectedCodecs);
      }
    }, codecs);
  }

  /**
   * Verify the page index is correct.
   *
   * @param columnIdxs the idx of column to be validated.
   */
  private void validatePageIndex(MessageType requiredSchema) throws Exception {
    ParquetMetadata outMetaData = getFileMetaData(outputFile);

    int inputFileIndex = 0;
    TransParquetFileReader inReader = new TransParquetFileReader(
        HadoopInputFile.fromPath(new Path(inputFiles.get(inputFileIndex).getFileName()), conf),
        HadoopReadOptions.builder(conf).build()
    );
    ParquetMetadata inMetaData = inReader.getFooter();


    Path outputFilePath = new Path(outputFile);
    try (TransParquetFileReader outReader = new TransParquetFileReader(
        HadoopInputFile.fromPath(outputFilePath, conf),
        HadoopReadOptions.builder(conf).build())) {

      for (int outBlockId = 0, inBlockId = 0; outBlockId < outMetaData.getBlocks().size(); ++outBlockId, ++inBlockId) {
        // Refresh reader of input file
        if (inBlockId == inMetaData.getBlocks().size()) {
          inReader = new TransParquetFileReader(
              HadoopInputFile.fromPath(new Path(inputFiles.get(++inputFileIndex).getFileName()), conf),
              HadoopReadOptions.builder(conf).build());
          inMetaData = inReader.getFooter();
          inBlockId = 0;
        }

        BlockMetaData inBlockMetaData = inMetaData.getBlocks().get(inBlockId);
        BlockMetaData outBlockMetaData = outMetaData.getBlocks().get(outBlockId);
        int inputColumns = inBlockMetaData.getColumns().size();

        List<Type> fields = requiredSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
          ColumnChunkMetaData outChunk = outBlockMetaData.getColumns().get(i);
          ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
          OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
          if (inputColumns - 1 < i) {
            assertEquals(ASCENDING, outColumnIndex.getBoundaryOrder());
            assertEquals(1, outColumnIndex.getNullCounts().size());
            assertEquals(outChunk.getValueCount(), outColumnIndex.getNullCounts().get(0));
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            assertEquals(outOffsetIndex.getPageCount(), outOffsets.size());
            for (int k = 0; k < outOffsetIndex.getPageCount(); k++) {
              assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
            }
            continue;
          }

          ColumnChunkMetaData inChunk = inBlockMetaData.getColumns().get(i);
          ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
          OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);

          if (outChunk.getPath().toDotString().equals(FILENAME_METADATA_FIELD)) {
            assertEquals(ASCENDING, outColumnIndex.getBoundaryOrder());
            assertEquals(1, outColumnIndex.getNullCounts().size());
            assertEquals(0, outColumnIndex.getNullCounts().get(0));
            assertEquals(outputFilePath.getName(), Binary.fromReusedByteBuffer(outColumnIndex.getMaxValues().get(0)).toStringUsingUTF8());
            assertEquals(outputFilePath.getName(), Binary.fromReusedByteBuffer(outColumnIndex.getMinValues().get(0)).toStringUsingUTF8());
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            assertEquals(outOffsetIndex.getPageCount(), outOffsets.size());
            for (int k = 0; k < outOffsetIndex.getPageCount(); k++) {
              assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
            }
            continue;
          }

          if (inColumnIndex != null) {
            assertEquals(inColumnIndex.getBoundaryOrder(), outColumnIndex.getBoundaryOrder());
            assertEquals(inColumnIndex.getMaxValues(), outColumnIndex.getMaxValues());
            assertEquals(inColumnIndex.getMinValues(), outColumnIndex.getMinValues());
            assertEquals(inColumnIndex.getNullCounts(), outColumnIndex.getNullCounts());
          }
          if (inOffsetIndex != null) {
            List<Long> inOffsets = getOffsets(inReader, inChunk);
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            assertEquals(inOffsets.size(), outOffsets.size());
            assertEquals(inOffsets.size(), inOffsetIndex.getPageCount());
            assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
            for (int k = 0; k < inOffsetIndex.getPageCount(); k++) {
              assertEquals(inOffsetIndex.getFirstRowIndex(k), outOffsetIndex.getFirstRowIndex(k));
              assertEquals(inOffsetIndex.getLastRowIndex(k, inChunk.getValueCount()),
                  outOffsetIndex.getLastRowIndex(k, outChunk.getValueCount()));
              assertEquals(inOffsetIndex.getOffset(k), (long) inOffsets.get(k));
              assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
            }
          }
        }
      }
    }
  }

  private List<Long> getOffsets(TransParquetFileReader reader, ColumnChunkMetaData chunk) throws IOException {
    List<Long> offsets = new ArrayList<>();
    reader.setStreamPosition(chunk.getStartingPos());
    long readValues = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      long curOffset = reader.getPos();
      PageHeader pageHeader = reader.readPageHeader();
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          writer.readBlock(pageHeader.getCompressed_page_size(), reader);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          offsets.add(curOffset);
          writer.readBlock(pageHeader.getCompressed_page_size(), reader);
          readValues += headerV1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          offsets.add(curOffset);
          int rlLength = headerV2.getRepetition_levels_byte_length();
          writer.readBlock(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          writer.readBlock(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          writer.readBlock(payLoadLength, reader);
          readValues += headerV2.getNum_values();
          break;
        default:
          throw new IOException("Not recognized page type");
      }
    }
    return offsets;
  }

  private void validateCreatedBy() throws Exception {
    Set<String> createdBySet = new HashSet<>();
    for (TestFile inputFile : inputFiles) {
      ParquetMetadata pmd = getFileMetaData(inputFile.getFileName());
      createdBySet.add(pmd.getFileMetaData().getCreatedBy());
      assertNull(pmd.getFileMetaData().getKeyValueMetaData().get(HoodieParquetFileBinaryCopier.ORIGINAL_CREATED_BY_KEY));
    }

    // Verify created_by from input files have been deduplicated
    Object[] inputCreatedBys = createdBySet.toArray();
    assertEquals(1, inputCreatedBys.length);

    // Verify created_by has been set
    FileMetaData outFMD = getFileMetaData(outputFile).getFileMetaData();
    final String createdBy = outFMD.getCreatedBy();
    assertNotNull(createdBy);
    assertEquals(createdBy, Version.FULL_VERSION);

    // Verify original.created.by has been set
    String inputCreatedBy = (String) inputCreatedBys[0];
    String originalCreatedBy = outFMD.getKeyValueMetaData().get(HoodieParquetFileBinaryCopier.ORIGINAL_CREATED_BY_KEY);
    assertEquals(inputCreatedBy, originalCreatedBy);
  }

  public static class TestFileBuilder {
    private MessageType schema;
    private Configuration conf;
    private Map<String, String> extraMeta = new HashMap<>();
    private int numRecord = 100000;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
    private int pageSize = ParquetProperties.DEFAULT_PAGE_SIZE;
    private String codec = "ZSTD";
    private String[] encryptColumns = {};
    private ParquetCipher cipher = ParquetCipher.AES_GCM_V1;
    private Boolean footerEncryption = false;

    public TestFileBuilder(Configuration conf, MessageType schema) {
      this.conf = conf;
      this.schema = schema;
      conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    }

    public TestFileBuilder withNumRecord(int numRecord) {
      this.numRecord = numRecord;
      return this;
    }

    public TestFileBuilder withEncrytionAlgorithm(ParquetCipher cipher) {
      this.cipher = cipher;
      return this;
    }

    public TestFileBuilder withExtraMeta(Map<String, String> extraMeta) {
      this.extraMeta = extraMeta;
      return this;
    }

    public TestFileBuilder withWriterVersion(ParquetProperties.WriterVersion writerVersion) {
      this.writerVersion = writerVersion;
      return this;
    }

    public TestFileBuilder withPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public TestFileBuilder withCodec(String codec) {
      this.codec = codec;
      return this;
    }

    public TestFileBuilder withEncryptColumns(String[] encryptColumns) {
      this.encryptColumns = encryptColumns;
      return this;
    }

    public TestFileBuilder withFooterEncryption() {
      this.footerEncryption = true;
      return this;
    }

    public TestFile build()
        throws IOException {
      String fileName = createTempFile("test");
      SimpleGroup[] fileContent = createFileContent(schema);
      ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(fileName))
          .withConf(conf)
          .withWriterVersion(writerVersion)
          .withExtraMetaData(extraMeta)
          .withValidation(true)
          .withPageSize(pageSize)
          .withCompressionCodec(CompressionCodecName.valueOf(codec));
      try (ParquetWriter writer = builder.build()) {
        for (int i = 0; i < fileContent.length; i++) {
          writer.write(fileContent[i]);
        }
      }
      return new TestFile(fileName, fileContent);
    }

    private SimpleGroup[] createFileContent(MessageType schema) {
      SimpleGroup[] simpleGroups = new SimpleGroup[numRecord];
      for (int i = 0; i < simpleGroups.length; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        for (Type type : schema.getFields()) {
          addValueToSimpleGroup(g, type);
        }
        simpleGroups[i] = g;
      }
      return simpleGroups;
    }

    private void addValueToSimpleGroup(Group g, Type type) {
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = (PrimitiveType) type;
        if (primitiveType.getPrimitiveTypeName().equals(INT32)) {
          g.add(type.getName(), getInt());
        } else if (primitiveType.getPrimitiveTypeName().equals(INT64)) {
          g.add(type.getName(), getLong());
        } else if (primitiveType.getPrimitiveTypeName().equals(BINARY)) {
          g.add(type.getName(), getString());
        }
        // Only support 3 types now, more can be added later
      } else {
        GroupType groupType = (GroupType) type;
        Group parentGroup = g.addGroup(groupType.getName());
        for (Type field : groupType.getFields()) {
          addValueToSimpleGroup(parentGroup, field);
        }
      }
    }

    private static long getInt() {
      return ThreadLocalRandom.current().nextInt(10000);
    }

    private static long getLong() {
      return ThreadLocalRandom.current().nextLong(100000);
    }

    private static String getString() {
      char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 100; i++) {
        sb.append(chars[ThreadLocalRandom.current().nextInt(10)]);
      }
      return sb.toString();
    }

    public static String createTempFile(String prefix) {
      try {
        return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
      } catch (IOException e) {
        throw new AssertionError("Unable to create temporary file", e);
      }
    }

    public static void deleteTempFile(String file) {
      try {
        Files.delete(Paths.get(file));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static class TestFile {
    @Getter
    private final String fileName;
    @Getter
    private final SimpleGroup[] fileContent;

    public TestFile(String fileName, SimpleGroup[] fileContent) {
      this.fileName = fileName;
      this.fileContent = fileContent;
    }
  }

  private void verifyColumnConvert(
      ColumnDescriptor column,
      Function<String[], Boolean> converter,
      Boolean changed,
      String originalPathStr,
      String convertedPathStr) {
    String[] path = column.getPath();
    String[] originalPath = Arrays.copyOf(path, path.length);
    assertEquals(changed, converter.apply(path));
    assertEquals(originalPathStr, pathToString(originalPath));
    assertEquals(convertedPathStr, pathToString(path));
  }

  private String pathToString(String[] path) {
    return Arrays.stream(path).collect(Collectors.joining("."));
  }
}