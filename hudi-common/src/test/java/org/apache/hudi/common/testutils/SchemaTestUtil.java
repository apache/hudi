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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.genPseudoRandomUUID;

/**
 * A utility class for testing schema.
 */
@NoArgsConstructor
public final class SchemaTestUtil {

  private static final String RESOURCE_SAMPLE_DATA = "/sample.data";
  private static final MercifulJsonConverter CONVERTER = new MercifulJsonConverter();

  private final Random random = new Random(0xDEED);

  public static HoodieSchema getSimpleSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test.avsc"));
  }
  
  public static HoodieSchema getSchemaFromResourceFilePath(String filePath) throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream(filePath));
  }

  public static HoodieSchema getSchema(String path) throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream(path));
  }

  public static List<IndexedRecord> generateTestRecords(int from, int limit) throws IOException, URISyntaxException {
    return toRecords(getSimpleSchema(), getSimpleSchema(), from, limit);
  }

  public static List<IndexedRecord> generateTestRecords(String schemaPath, String dataPath) throws IOException, URISyntaxException {
    return toRecords(getSchema(schemaPath), getSchema(schemaPath), dataPath);
  }

  public static List<GenericRecord> generateTestGenericRecords(int from, int limit) throws IOException, URISyntaxException {
    return toRecords(getSimpleSchema(), getSimpleSchema(), from, limit);
  }

  public static List<String> generateTestJsonRecords(int from, int limit) throws IOException, URISyntaxException {
    Path dataPath = initializeSampleDataPath();

    try (Stream<String> stream = Files.lines(dataPath)) {
      return stream.skip(from).limit(limit).collect(Collectors.toList());
    } catch (IOException e) {
      throw new HoodieIOException("Could not read data from " + RESOURCE_SAMPLE_DATA, e);
    }
  }

  public static HoodieSchema getSchemaFromFields(List<String> fields) {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    List<HoodieSchemaField> hoodieFields = fields.stream()
        .map(fieldName -> HoodieSchemaField.of(fieldName, stringSchema))
        .collect(Collectors.toList());
    return HoodieSchema.createRecord("test_schema", "test_namespace", null, hoodieFields);
  }

  private static <T extends IndexedRecord> List<T> toRecords(HoodieSchema writerSchema, HoodieSchema readerSchema, int from, int limit)
      throws IOException, URISyntaxException {
    GenericDatumReader<T> reader = new GenericDatumReader<>(writerSchema.toAvroSchema(), readerSchema.toAvroSchema());
    Path dataPath = initializeSampleDataPath();

    try (Stream<String> stream = Files.lines(dataPath)) {
      return stream.skip(from).limit(limit).map(s -> {
        try {
          return reader.read(null, DecoderFactory.get().jsonDecoder(writerSchema.toAvroSchema(), s));
        } catch (IOException e) {
          throw new HoodieIOException("Could not read data from " + RESOURCE_SAMPLE_DATA, e);
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new HoodieIOException("Could not read data from " + RESOURCE_SAMPLE_DATA, e);
    }
  }

  private static <T extends IndexedRecord> List<T> toRecords(HoodieSchema writerSchema, HoodieSchema readerSchema, String path)
      throws IOException, URISyntaxException {
    GenericDatumReader<T> reader = new GenericDatumReader<>(writerSchema.toAvroSchema(), readerSchema.toAvroSchema());
    Path dataPath = initializeSampleDataPath(path);

    try (Stream<String> stream = Files.lines(dataPath)) {
      return stream.map(s -> {
        try {
          return reader.read(null, DecoderFactory.get().jsonDecoder(writerSchema.toAvroSchema(), s));
        } catch (IOException e) {
          throw new HoodieIOException("Could not read data from " + path, e);
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new HoodieIOException("Could not read data from " + path, e);
    }
  }

  /**
   * Required to register the necessary JAR:// file system.
   * @return Path to the sample data in the resource file.
   * @throws IOException
   * @throws URISyntaxException
   */
  private static Path initializeSampleDataPath() throws IOException, URISyntaxException {
    URI resource = SchemaTestUtil.class.getResource(RESOURCE_SAMPLE_DATA).toURI();
    if (resource.toString().contains("!")) {
      return uriToPath(resource);
    } else {
      return Paths.get(resource);
    }
  }

  private static Path initializeSampleDataPath(String path) throws IOException, URISyntaxException {
    URI resource = SchemaTestUtil.class.getResource(path).toURI();
    if (resource.toString().contains("!")) {
      return uriToPath(resource);
    } else {
      return Paths.get(resource);
    }
  }

  public static Path uriToPath(URI uri) throws IOException {
    final Map<String, String> env = new HashMap<>();
    final String[] array = uri.toString().split("!");
    FileSystem fs;
    try {
      fs = FileSystems.getFileSystem(URI.create(array[0]));
    } catch (FileSystemNotFoundException e) {
      fs = FileSystems.newFileSystem(URI.create(array[0]), env);
    }
    return fs.getPath(array[1]);
  }

  /**
   * Generates a list of random UUIDs
   *
   * @param size          Number of UUIDs to return.
   * @param existingUUIDs Existing UUIDs to include.
   * @return A list of UUIDs.
   */
  public List<String> genRandomUUID(int size, List<String> existingUUIDs) {
    Set<String> uuidSet = new HashSet<>(existingUUIDs);
    while (uuidSet.size() < size) {
      uuidSet.add(genRandomUUID());
    }
    return new ArrayList<>(uuidSet);
  }

  public List<IndexedRecord> generateHoodieTestRecords(int from, int limit)
      throws IOException, URISyntaxException {
    String instantTime = InProcessTimeGenerator.createNewInstantTime();
    List<String> recorKeyList = genRandomUUID(limit, Collections.emptyList());
    return generateHoodieTestRecords(from, recorKeyList, "0000/00/00", instantTime);
  }

  /**
   * Generates test records.
   *
   * @param from          Offset to start picking records.
   * @param recordKeyList Record keys to use.
   * @param partitionPath Partition path to use.
   * @param instantTime   Hudi instant time.
   * @return A list of {@link IndexedRecord}.
   * @throws IOException
   * @throws URISyntaxException
   */
  public List<IndexedRecord> generateHoodieTestRecords(int from,
                                                       List<String> recordKeyList,
                                                       String partitionPath,
                                                       String instantTime)
      throws IOException, URISyntaxException {
    List<IndexedRecord> records = generateTestRecords(from, recordKeyList.size());
    HoodieSchema hoodieFieldsSchema = HoodieSchemaUtils.addMetadataFields(getSimpleSchema());
    List<IndexedRecord> recordsWithMetaFields = new ArrayList<>();
    for (int i = 0; i < recordKeyList.size(); i++) {
      GenericRecord newRecord = HoodieAvroUtils.rewriteRecord((GenericRecord) records.get(i), hoodieFieldsSchema.toAvroSchema());
      newRecord.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recordKeyList.get(i));
      newRecord.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionPath);
      newRecord.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
      recordsWithMetaFields.add(newRecord);
    }
    return recordsWithMetaFields;
  }

  public List<HoodieRecord> generateHoodieTestRecords(int from, int limit, HoodieSchema schema)
      throws IOException, URISyntaxException {
    List<IndexedRecord> records = generateTestRecords(from, limit);
    return records.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s, schema.toAvroSchema()))
        .map(p -> convertToHoodieRecords(p, genRandomUUID(), "000/00/00")).collect(Collectors.toList());
  }

  private static HoodieRecord convertToHoodieRecords(IndexedRecord iRecord, String key, String partitionPath) {
    return new HoodieAvroRecord<>(new HoodieKey(key, partitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) iRecord)));
  }

  public static List<IndexedRecord> updateHoodieTestRecords(List<String> oldRecordKeys, List<IndexedRecord> newRecords,
      String instantTime) {

    return newRecords.stream().map(p -> {
      ((GenericRecord) p).put(HoodieRecord.RECORD_KEY_METADATA_FIELD, oldRecordKeys.remove(0));
      ((GenericRecord) p).put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "0000/00/00");
      ((GenericRecord) p).put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
      return p;
    }).collect(Collectors.toList());

  }

  public List<HoodieRecord> generateHoodieTestRecordsWithoutHoodieMetadata(int from, int limit)
      throws IOException, URISyntaxException {
    List<IndexedRecord> iRecords = generateTestRecords(from, limit);
    return iRecords.stream().map(r -> new HoodieAvroRecord<>(new HoodieKey(genRandomUUID(), "0000/00/00"),
        new HoodieAvroPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
  }

  public static List<HoodieRecord> updateHoodieTestRecordsWithoutHoodieMetadata(List<HoodieRecord> oldRecords,
      HoodieSchema schema, String fieldNameToUpdate, String newValue) {
    return oldRecords.stream().map(r -> {
      try {
        GenericRecord rec = (GenericRecord) ((HoodieAvroRecord) r).getData().getInsertValue(schema.toAvroSchema()).get();
        rec.put(fieldNameToUpdate, newValue);
        return new HoodieAvroRecord<>(r.getKey(), new HoodieAvroPayload(Option.of(rec)));
      } catch (IOException io) {
        throw new HoodieIOException("unable to get data from hoodie record", io);
      }
    }).collect(Collectors.toList());
  }

  public static HoodieSchema getEvolvedSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test-evolved.avsc"));
  }

  public static HoodieSchema getEvolvedCompatibleSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test-evolved-compatible.avsc"));
  }

  public static List<IndexedRecord> generateEvolvedTestRecords(int from, int limit)
      throws IOException, URISyntaxException {
    return toRecords(getSimpleSchema(), getEvolvedSchema(), from, limit);
  }

  public static HoodieSchema getComplexEvolvedSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/complex-test-evolved.avsc"));
  }

  public static HoodieSchema getTimestampEvolvedSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/timestamp-test-evolved.avsc"));
  }

  public static HoodieSchema getTimestampWithLogicalTypeSchema() throws IOException {
    return new HoodieSchema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/timestamp-logical-type.avsc"));
  }

  public static GenericRecord generateAvroRecordFromJson(HoodieSchema schema, int recordNumber, String instantTime,
                                                         String fileId) throws IOException {
    return generateAvroRecordFromJson(schema, recordNumber, instantTime, fileId, true);
  }

  public static GenericRecord generateAvroRecordFromJson(HoodieSchema schema, int recordNumber, String instantTime,
      String fileId, boolean populateMetaFields) throws IOException {
    SampleTestRecord record = new SampleTestRecord(instantTime, recordNumber, fileId, populateMetaFields);
    return CONVERTER.convert(record.toJsonString(), schema.toAvroSchema());
  }

  public static HoodieSchema getSchemaFromResource(Class<?> clazz, String name, boolean withHoodieMetadata) {
    try (InputStream schemaInputStream = clazz.getResourceAsStream(name)) {
      HoodieSchema schema = new HoodieSchema.Parser().parse(schemaInputStream);
      return withHoodieMetadata ? HoodieSchemaUtils.addMetadataFields(schema) : schema;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to get schema from resource `%s` for class `%s`", name, clazz.getName()));
    }
  }

  public static HoodieSchema getSchemaFromResource(Class<?> clazz, String name) {
    return getSchemaFromResource(clazz, name, false);
  }

  public static HoodieSchema getHoodieSchemaFromResource(Class<?> clazz, String name) {
    return getHoodieSchemaFromResource(clazz, name, false);
  }

  public static HoodieSchema getHoodieSchemaFromResource(Class<?> clazz, String name, boolean withHoodieMetadata) {
    try (InputStream schemaInputStream = clazz.getResourceAsStream(name)) {
      HoodieSchema schema = new HoodieSchema.Parser().parse(schemaInputStream);
      return withHoodieMetadata ? HoodieSchema.addMetadataFields(schema, false) : schema;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to get schema from resource `%s` for class `%s`", name, clazz.getName()));
    }
  }

  public static List<IndexedRecord> generateTestRecordsForSchema(HoodieSchema schema) {
    RandomData generator = new RandomData(schema, 1000);
    List<IndexedRecord> records = new ArrayList<>();
    for (Object o : generator) {
      IndexedRecord record = (IndexedRecord) o;
      records.add(record);
    }
    return records;
  }

  //Taken from test pkg 1.8.2 avro. This is available as a util class in latest versions. When we upgrade avro we can remove this
  static class RandomData implements Iterable<Object> {
    private final HoodieSchema root;
    private final long seed;
    private final int count;

    public RandomData(HoodieSchema schema, int count) {
      this(schema, count, System.currentTimeMillis());
    }

    public RandomData(HoodieSchema schema, int count, long seed) {
      this.root = schema;
      this.seed = seed;
      this.count = count;
    }

    @SuppressWarnings(value = "unchecked")
    private static Object generate(HoodieSchema schema, Random random, int d) {
      switch (schema.getType()) {
        case RECORD:
          GenericRecord record = new GenericData.Record(schema.toAvroSchema());
          for (HoodieSchemaField field : schema.getFields()) {
            record.put(field.name(), generate(field.schema(), random, d + 1));
          }
          return record;
        case ENUM:
          List<String> symbols = schema.getEnumSymbols();
          return new GenericData.EnumSymbol(schema.toAvroSchema(), symbols.get(random.nextInt(symbols.size())));
        case ARRAY:
          int length = (random.nextInt(5) + 2) - d;
          GenericArray<Object> array =
              new GenericData.Array<>(Math.max(length, 0), schema.toAvroSchema());
          for (int i = 0; i < length; i++) {
            array.add(generate(schema.getElementType(), random, d + 1));
          }
          return array;
        case MAP:
          length = (random.nextInt(5) + 2) - d;
          Map<Object, Object> map = new HashMap<Object, Object>(length <= 0 ? 0 : length);
          for (int i = 0; i < length; i++) {
            map.put(randomUtf8(random, 40),
                generate(schema.getValueType(), random, d + 1));
          }
          return map;
        case UNION:
          List<HoodieSchema> types = schema.getTypes();
          //Dropping the null at the end.
          return generate(types.get(random.nextInt(types.size() - 1)), random, d);
        case FIXED:
          byte[] bytes = new byte[schema.getFixedSize()];
          random.nextBytes(bytes);
          return new GenericData.Fixed(schema.toAvroSchema(), bytes);
        case STRING:
          return randomUtf8(random, 40);
        case BYTES:
          return randomBytes(random, 40);
        case INT:
          return random.nextInt();
        case LONG:
          return random.nextLong();
        case FLOAT:
          return random.nextFloat();
        case DOUBLE:
          return random.nextDouble();
        case BOOLEAN:
          return random.nextBoolean();
        case NULL:
          return null;
        default:
          throw new RuntimeException("Unknown type: " + schema);
      }
    }

    private static Utf8 randomUtf8(Random rand, int maxLength) {
      Utf8 utf8 = new Utf8().setLength(rand.nextInt(maxLength));
      for (int i = 0; i < utf8.getLength(); i++) {
        utf8.getBytes()[i] = (byte) ('a' + rand.nextInt('z' - 'a'));
      }
      return utf8;
    }

    private static ByteBuffer randomBytes(Random rand, int maxLength) {
      ByteBuffer bytes = ByteBuffer.allocate(rand.nextInt(maxLength));
      bytes.limit(bytes.capacity());
      rand.nextBytes(bytes.array());
      return bytes;
    }

    public Iterator<Object> iterator() {
      return new Iterator<Object>() {
        private int n;
        private Random random = new Random(seed);

        public boolean hasNext() {
          return n < count;
        }

        public Object next() {
          n++;
          return generate(root, random, 0);
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private String genRandomUUID() {
    return genPseudoRandomUUID(random).toString();
  }
}
