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
import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility class for testing schema.
 */
public final class SchemaTestUtil {

  private static final String RESOURCE_SAMPLE_DATA = "/sample.data";

  public static Schema getSimpleSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test.avsc"));
  }

  public static List<IndexedRecord> generateTestRecords(int from, int limit) throws IOException, URISyntaxException {
    return toRecords(getSimpleSchema(), getSimpleSchema(), from, limit);
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

  private static <T extends IndexedRecord> List<T> toRecords(Schema writerSchema, Schema readerSchema, int from, int limit)
      throws IOException, URISyntaxException {
    GenericDatumReader<T> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    Path dataPath = initializeSampleDataPath();

    try (Stream<String> stream = Files.lines(dataPath)) {
      return stream.skip(from).limit(limit).map(s -> {
        try {
          return reader.read(null, DecoderFactory.get().jsonDecoder(writerSchema, s));
        } catch (IOException e) {
          throw new HoodieIOException("Could not read data from " + RESOURCE_SAMPLE_DATA, e);
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new HoodieIOException("Could not read data from " + RESOURCE_SAMPLE_DATA, e);
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
      return Paths.get(SchemaTestUtil.class.getResource(RESOURCE_SAMPLE_DATA).toURI());
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

  public static List<IndexedRecord> generateHoodieTestRecords(int from, int limit)
      throws IOException, URISyntaxException {
    List<IndexedRecord> records = generateTestRecords(from, limit);
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    Schema hoodieFieldsSchema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    return records.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s, hoodieFieldsSchema)).map(p -> {
      p.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, UUID.randomUUID().toString());
      p.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "0000/00/00");
      p.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
      return p;
    }).collect(Collectors.toList());

  }

  public static List<HoodieRecord> generateHoodieTestRecords(int from, int limit, Schema schema)
      throws IOException, URISyntaxException {
    List<IndexedRecord> records = generateTestRecords(from, limit);
    return records.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s, schema))
        .map(p -> convertToHoodieRecords(p, UUID.randomUUID().toString(), "000/00/00")).collect(Collectors.toList());
  }

  private static HoodieRecord convertToHoodieRecords(IndexedRecord iRecord, String key, String partitionPath) {
    return new HoodieLegacyAvroRecord<>(new HoodieKey(key, partitionPath),
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

  public static List<HoodieRecord> generateHoodieTestRecordsWithoutHoodieMetadata(int from, int limit)
      throws IOException, URISyntaxException {

    List<IndexedRecord> iRecords = generateTestRecords(from, limit);
    return iRecords.stream().map(r -> new HoodieLegacyAvroRecord<>(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
        new HoodieAvroPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
  }

  public static List<HoodieRecord> updateHoodieTestRecordsWithoutHoodieMetadata(List<HoodieRecord> oldRecords,
      Schema schema, String fieldNameToUpdate, String newValue) {
    return oldRecords.stream().map(r -> {
      try {
        GenericRecord rec = (GenericRecord) ((HoodieLegacyAvroRecord) r).getData().getInsertValue(schema).get();
        rec.put(fieldNameToUpdate, newValue);
        return new HoodieLegacyAvroRecord<>(r.getKey(), new HoodieAvroPayload(Option.of(rec)));
      } catch (IOException io) {
        throw new HoodieIOException("unable to get data from hoodie record", io);
      }
    }).collect(Collectors.toList());
  }

  public static Schema getEvolvedSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test-evolved.avsc"));
  }

  public static Schema getEvolvedCompatibleSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/simple-test-evolved-compatible.avsc"));
  }

  public static List<IndexedRecord> generateEvolvedTestRecords(int from, int limit)
      throws IOException, URISyntaxException {
    return toRecords(getSimpleSchema(), getEvolvedSchema(), from, limit);
  }

  public static Schema getComplexEvolvedSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/complex-test-evolved.avsc"));
  }

  public static Schema getTimestampEvolvedSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/timestamp-test-evolved.avsc"));
  }

  public static Schema getTimestampWithLogicalTypeSchema() throws IOException {
    return new Schema.Parser().parse(SchemaTestUtil.class.getResourceAsStream("/timestamp-logical-type.avsc"));
  }

  public static GenericRecord generateAvroRecordFromJson(Schema schema, int recordNumber, String instantTime,
                                                         String fileId) throws IOException {
    return generateAvroRecordFromJson(schema, recordNumber, instantTime, fileId, true);
  }

  public static GenericRecord generateAvroRecordFromJson(Schema schema, int recordNumber, String instantTime,
      String fileId, boolean populateMetaFields) throws IOException {
    SampleTestRecord record = new SampleTestRecord(instantTime, recordNumber, fileId, populateMetaFields);
    MercifulJsonConverter converter = new MercifulJsonConverter();
    return converter.convert(record.toJsonString(), schema);
  }

  public static Schema getSchemaFromResource(Class<?> clazz, String name, boolean withHoodieMetadata) {
    try {
      Schema schema = new Schema.Parser().parse(clazz.getResourceAsStream(name));
      return withHoodieMetadata ? HoodieAvroUtils.addMetadataFields(schema) : schema;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to get schema from resource `%s` for class `%s`", name, clazz.getName()));
    }
  }

  public static Schema getSchemaFromResource(Class<?> clazz, String name) {
    return getSchemaFromResource(clazz, name, false);
  }

  public static List<IndexedRecord> generateTestRecordsForSchema(Schema schema) {
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
    private final Schema root;
    private final long seed;
    private final int count;

    public RandomData(Schema schema, int count) {
      this(schema, count, System.currentTimeMillis());
    }

    public RandomData(Schema schema, int count, long seed) {
      this.root = schema;
      this.seed = seed;
      this.count = count;
    }

    @SuppressWarnings(value = "unchecked")
    private static Object generate(Schema schema, Random random, int d) {
      switch (schema.getType()) {
        case RECORD:
          GenericRecord record = new GenericData.Record(schema);
          for (Schema.Field field : schema.getFields()) {
            record.put(field.name(), generate(field.schema(), random, d + 1));
          }
          return record;
        case ENUM:
          List<String> symbols = schema.getEnumSymbols();
          return new GenericData.EnumSymbol(schema, symbols.get(random.nextInt(symbols.size())));
        case ARRAY:
          int length = (random.nextInt(5) + 2) - d;
          GenericArray<Object> array =
              new GenericData.Array(length <= 0 ? 0 : length, schema);
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
          List<Schema> types = schema.getTypes();
          //Dropping the null at the end.
          return generate(types.get(random.nextInt(types.size() - 1)), random, d);
        case FIXED:
          byte[] bytes = new byte[schema.getFixedSize()];
          random.nextBytes(bytes);
          return new GenericData.Fixed(schema, bytes);
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
}
