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

import org.apache.hudi.io.storage.HoodieFileMetadataMerger;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.Version;
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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestHoodieParquetFileBinaryCopier {

  private final int numRecord = 1;
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
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, new Properties());
    writer.close();

    // Verify the schema are not changed
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType fileSchema = pmd.getFileMetaData().getSchema();
    assertEquals(schema, fileSchema);
    validateSchema(fileSchema);

    // Verify codec
    verifyCodec(outputFile, CompressionCodecName.GZIP);

    // Verify the merged data are not changed
    validateColumnData();

    // Verify the page index
    validatePageIndex(0, 1, 2, 3, 4);

    // Verify original.created.by is preserved
    validateCreatedBy();
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
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, new Properties());
    writer.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType fileSchema = pmd.getFileMetaData().getSchema();
    assertEquals(schema, fileSchema);
    validateSchema(fileSchema);

    // Verify codec has been translated
    verifyCodec(outputFile, CompressionCodecName.ZSTD);

    // Verify the data are not changed for the columns not pruned
    validateColumnData();

    // Verify the page index
    validatePageIndex(0, 1, 2, 3, 4);

    // Verify original.created.by is preserved
    validateCreatedBy();
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
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema1, new Properties());
    writer.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    validateSchema(schema);

    // Verify codec has been translated
    verifyCodec(outputFile, CompressionCodecName.UNCOMPRESSED);

    // Verify the data are not changed
    validateColumnData();

    // Verify the page index
    validatePageIndex(0, 1, 2);

    // Verify original.created.by is preserved
    validateCreatedBy();
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
    writer.binaryCopy(inputPaths, Collections.singletonList(outputPath), schema, new Properties());
    writer.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType fileSchema = pmd.getFileMetaData().getSchema();
    assertEquals(schema, fileSchema);

    // Verify codec has been translated
    verifyCodec(outputFile, CompressionCodecName.GZIP);

    // Verify the data are not changed
    validateColumnData();

    // Verify the page index
    validatePageIndex(1, 2, 3, 4);

    // Verify original.created.by is preserved
    validateCreatedBy();
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

  private void validateSchema(MessageType schema) {
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 4);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Gender");
    assertEquals(fields.get(3).getName(), "Links");
    List<Type> subFields = fields.get(3).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");
  }

  private void validateColumnData() throws IOException {
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
      if (group.getType().containsField(FILENAME_METADATA_FIELD)) {
        assertEquals(group.getString(FILENAME_METADATA_FIELD, 0), outputFilePath.getName());
        assertNotEquals(group.getString(FILENAME_METADATA_FIELD, 0),
            expectGroup.getString(FILENAME_METADATA_FIELD, 0));
      }
      assertEquals(group.getLong("DocId", 0), expectGroup.getLong("DocId", 0));
      assertArrayEquals(group.getBinary("Name", 0).getBytes(),
          expectGroup.getBinary("Name", 0).getBytes());
      assertArrayEquals(group.getBinary("Gender", 0).getBytes(),
          expectGroup.getBinary("Gender", 0).getBytes());

      if (expectGroup.getType().containsField("Links")) {
        Group subGroup = group.getGroup("Links", 0);
        Group expectSubGroup = expectGroup.getGroup("Links", 0);
        assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(),
            expectSubGroup.getBinary("Backward", 0).getBytes());
        assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(),
            expectSubGroup.getBinary("Forward", 0).getBytes());
      }
    }

    reader.close();
  }

  private ParquetMetadata getFileMetaData(String file) throws IOException {
    return ParquetFileReader.readFooter(conf, new Path(file));
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
  private void validatePageIndex(Integer... columnIdxs) throws Exception {
    ParquetMetadata outMetaData = getFileMetaData(outputFile);

    int inputFileIndex = 0;
    TransParquetFileReader inReader = new TransParquetFileReader(
        HadoopInputFile.fromPath(new Path(inputFiles.get(inputFileIndex).getFileName()), conf),
        HadoopReadOptions.builder(conf).build()
    );
    ParquetMetadata inMetaData = inReader.getFooter();

    try (TransParquetFileReader outReader = new TransParquetFileReader(
        HadoopInputFile.fromPath(new Path(outputFile), conf),
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

        for (int j = 0; j < columnIdxs.length; j++) {
          ColumnChunkMetaData inChunk = inBlockMetaData.getColumns().get(columnIdxs[j]);
          ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
          OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
          ColumnChunkMetaData outChunk = outBlockMetaData.getColumns().get(columnIdxs[j]);
          ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
          OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
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
    private final String fileName;
    private final SimpleGroup[] fileContent;

    public TestFile(String fileName, SimpleGroup[] fileContent) {
      this.fileName = fileName;
      this.fileContent = fileContent;
    }

    public String getFileName() {
      return this.fileName;
    }

    public SimpleGroup[] getFileContent() {
      return this.fileContent;
    }
  }
}