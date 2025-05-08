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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.io.hadoop.HoodieHFileUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public class HoodieSparkHFileReader implements HoodieSparkFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkHFileReader.class);
  public static final String SCHEMA_KEY = "schema";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";
  public static final String KEY_FIELD_NAME = "key";

  private final StoragePath path;
  private final HoodieStorage storage;
  private final StorageConfiguration<?> storageConf;
  private final CacheConfig config;
  private final Option<byte[]> content;
  private final Lazy<Schema> schema;
  private final List<Predicate> predicates;

  // NOTE: Reader is ONLY THREAD-SAFE for {@code Scanner} operating in Positional Read ("pread")
  //       mode (ie created w/ "pread = true")
  // Common reader is not used for the iterators since they can be closed independently.
  // Use {@link getSharedReader()} instead of accessing directly.
  private Option<HFile.Reader> sharedReader;
  // NOTE: Scanner caches read blocks, therefore it's important to re-use scanner
  //       wherever possible
  private Option<HFileScanner> sharedScanner;

  private final Object sharedLock = new Object();

  public HoodieSparkHFileReader(StoragePath path,
                                HoodieStorage storage, StorageConfiguration<?> storageConf,
                                Option<Schema> schemaOpt,
                                Option<byte[]> content,
                                List<Predicate> predicates) {
    this.path = path;
    this.storage = storage;
    this.storageConf = storageConf;
    this.config = new CacheConfig(storageConf.unwrapAs(Configuration.class));
    this.content = content;
    this.predicates = predicates;

    // Shared reader is instantiated lazily.
    this.sharedReader = Option.empty();
    this.sharedScanner = Option.empty();
    this.schema = schemaOpt.map(Lazy::eagerly)
        .orElseGet(() -> Lazy.lazily(() -> fetchSchema(getSharedHFileReader())));
  }

  private static Schema fetchSchema(HFile.Reader reader) {
    HFileInfo fileInfo = reader.getHFileInfo();
    return new Schema.Parser().parse(new String(fileInfo.get(getUTF8Bytes(SCHEMA_KEY))));
  }

  /**
   * Instantiates the shared HFile reader if not instantiated
   * @return the shared HFile reader
   */
  private HFile.Reader getSharedHFileReader() {
    if (!sharedReader.isPresent()) {
      synchronized (sharedLock) {
        if (!sharedReader.isPresent()) {
          sharedReader = Option.of(getHFileReader());
        }
      }
    }
    return sharedReader.get();
  }

  private HFile.Reader getHFileReader() {
    if (content.isPresent()) {
      return HoodieHFileUtils.createHFileReader(storage, path, content.get());
    }
    return HoodieHFileUtils.createHFileReader(storage, path, config, storageConf.unwrapAs(Configuration.class));
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    // NOTE: This access to reader is thread-safe
    HFileInfo fileInfo = getSharedHFileReader().getHFileInfo();
    return new String[] {new String(fileInfo.get(getUTF8Bytes(KEY_MIN_RECORD))),
        new String(fileInfo.get(getUTF8Bytes(KEY_MAX_RECORD)))};
  }

  @Override
  public BloomFilter readBloomFilter() {
    try {
      // NOTE: This access to reader is thread-safe
      HFileInfo fileInfo = getSharedHFileReader().getHFileInfo();
      ByteBuff buf = getSharedHFileReader().getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK, false).getBufferWithoutHeader();
      // We have to copy bytes here, since we can't reuse buffer's underlying
      // array as is, since it contains additional metadata (header)
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return BloomFilterFactory.fromString(new String(bytes),
          new String(fileInfo.get(getUTF8Bytes(KEY_BLOOM_FILTER_TYPE_CODE))));
    } catch (IOException e) {
      throw new HoodieException("Could not read bloom filter from " + path, e);
    }
  }

  /**
   * Filter keys by availability.
   * <p>
   * Note: This method is performant when the caller passes in a sorted candidate keys.
   *
   * @param candidateRowKeys - Keys to check for the availability
   * @return Subset of candidate keys that are available
   */
  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    // candidateRowKeys must be sorted
    SortedSet<String> sortedCandidateRowKeys = new TreeSet<>(candidateRowKeys);

    synchronized (sharedLock) {
      if (!sharedScanner.isPresent()) {
        // For shared scanner, which is primarily used for point-lookups, we're caching blocks
        // by default, to minimize amount of traffic to the underlying storage
        sharedScanner = Option.of(getHFileScanner(getSharedHFileReader(), true));
      }
      return sortedCandidateRowKeys.stream()
          .filter(k -> {
            try {
              return isKeyAvailable(k, sharedScanner.get());
            } catch (IOException e) {
              LOG.error("Failed to check key availability: " + k);
              return false;
            }
          })
          // Record position is not supported for HFile
          .map(key -> org.apache.hudi.common.util.collection.Pair.of(key, HoodieRecordLocation.INVALID_POSITION))
          .collect(Collectors.toSet());
    }
  }

  private boolean isKeyAvailable(String key, HFileScanner keyScanner) throws IOException {
    final KeyValue kv = new KeyValue(getUTF8Bytes(key), null, null, null);
    return keyScanner.seekTo(kv) == 0;
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    HFile.Reader reader = getHFileReader();
    HFileScanner scanner = getHFileScanner(reader, false);

    return new ClosableIterator<HoodieRecord<InternalRow>>() {
      @Override
      public boolean hasNext() {
        try {
          if (predicates.isEmpty()) {
            return scanner.next();
          } else {
            // Filter cells based on given predicates.
            // Right now assume all predicates are conjunctive.
            while (scanner.next()) {
              Cell cell = scanner.getCell();
              String key = new String(CellUtil.cloneRow(cell));
              boolean found = true;
              for (Predicate predicate : predicates) {
                // If key fails a predicate, go to next record.
                if (!eval(key, predicate)) {
                  found = false;
                }
              }
              // Pass all the predicates.
              if (found) {
                return true;
              }
            }
            // No records could satisfy the predicates.
            return false;
          }
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public HoodieRecord<InternalRow> next() {
        Cell cell = scanner.getCell();
        String key = new String(CellUtil.cloneRow(cell));
        byte[] valueBytes = CellUtil.cloneValue(cell);
        UnsafeRow row = convertToUnsafeRow(valueBytes, readerSchema, requestedSchema);
        HoodieKey hoodieKey = new HoodieKey(key, null);
        return new HoodieSparkRecord(hoodieKey, row, false);
      }

      @Override
      public void close() {
        // No explicit close needed for scanner
      }
    };
  }

  private boolean eval(String key, Predicate predicate) {
    List<Expression> children = predicate.getChildren();
    for (Expression e : children) {
      if (e.getDataType().typeId() != Type.TypeID.STRING) {
        throw new RuntimeException(
            "The predicate parameter type is always string in hfile");
      }
    }

    if (predicate.getOperator() == Expression.Operator.EQ) {
      if (!children.get(1).eval(null).equals(key)) {
        return false;
      }
    } else if (predicate instanceof Predicates.In) {
      boolean found = false;
      for (int i = 1; i < children.size(); i++) {
        if (children.get(i).eval(null).equals(key)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    } else if (predicate instanceof Predicates.StringStartsWithAny) {
      children.remove(0);
      Expression exp = new Predicates.StringStartsWithAny(Literal.from(key), children);
      if (!(boolean) exp.eval(null)) {
        return false;
      }
    }
    return true;
  }

  private UnsafeRow convertToUnsafeRow(byte[] valueBytes, Schema readerSchema, Schema requestedSchema) {
    try {
      assert (requestedSchema != null);
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(readerSchema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(valueBytes, 0, valueBytes.length, null);
      GenericRecord record = datumReader.read(null, decoder);

      // Convert Avro schema to Spark StructType
      StructType sparkSchema = AvroConversionUtils.convertAvroSchemaToStructType(readerSchema);

      // Recursively convert the Avro record to Catalyst InternalRow
      InternalRow internalRow = (InternalRow) convertAvroToCatalyst(record, sparkSchema);

      // Convert to UnsafeRow
      UnsafeProjection projection = UnsafeProjection.create(sparkSchema);
      return projection.apply(internalRow);
    } catch (IOException e) {
      throw new RuntimeException("Error converting bytes to InternalRow", e);
    }
  }

  private Object convertAvroToCatalyst(Object avroObj, DataType sparkType) {
    if (avroObj == null) {
      return null;
    }
    if (sparkType instanceof StructType) {
      GenericRecord record = (GenericRecord) avroObj;
      StructType structType = (StructType) sparkType;
      Object[] values = new Object[structType.fields().length];

      for (int i = 0; i < structType.fields().length; i++) {
        StructField field = structType.fields()[i];
        String fieldName = field.name();
        Schema.Field avroField = record.getSchema().getField(fieldName);
        Object avroValue = record.get(fieldName);
        values[i] = convertAvroToCatalyst(avroValue, field.dataType());
      }
      return new GenericInternalRow(values);

    } else if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      List<?> avroList = (List<?>) avroObj;
      Object[] converted = new Object[avroList.size()];
      for (int i = 0; i < avroList.size(); i++) {
        converted[i] = convertAvroToCatalyst(avroList.get(i), arrayType.elementType());
      }
      return new GenericArrayData(converted);

    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      Map<?, ?> avroMap = (Map<?, ?>) avroObj;

      Object[] keyArray = new Object[avroMap.size()];
      Object[] valueArray = new Object[avroMap.size()];

      int i = 0;
      for (Map.Entry<?, ?> entry : avroMap.entrySet()) {
        keyArray[i] = convertAvroToCatalyst(entry.getKey(), mapType.keyType());
        valueArray[i] = convertAvroToCatalyst(entry.getValue(), mapType.valueType());
        i++;
      }

      ArrayData keyData = new GenericArrayData(keyArray);
      ArrayData valueData = new GenericArrayData(valueArray);
      return new ArrayBasedMapData(keyData, valueData);

    } else if (sparkType instanceof StringType) {
      return UTF8String.fromString(avroObj.toString());
    } else if (sparkType instanceof BinaryType) {
      return ((ByteBuffer) avroObj).array();
    } else {
      // Primitive types (int, long, boolean, etc.)
      return avroObj;
    }
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HFile.Reader reader = getHFileReader();
    HFileScanner scanner = getHFileScanner(reader, false);
    List<String> keys = new ArrayList<>();
    if (scanner.seekTo()) {
      do {
        Cell cell = scanner.getCell();
        String key = new String(CellUtil.cloneRow(cell));
        keys.add(key);
      } while (scanner.next());
    }
    return new ClosableIterator<String>() {
      private final Iterator<String> iter = keys.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public String next() {
        return iter.next();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public Schema getSchema() {
    return null; // Schema should be extracted from Hoodie metadata
  }

  @Override
  public void close() {
    try {
      synchronized (this) {
        if (sharedScanner.isPresent()) {
          sharedScanner.get().close();
        }
        if (sharedReader.isPresent()) {
          sharedReader.get().close();
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error closing the hfile reader", e);
    }
  }

  @Override
  public long getTotalRecords() {
    // NOTE: This access to reader is thread-safe
    return getSharedHFileReader().getEntries();
  }

  private static HFileScanner getHFileScanner(HFile.Reader reader, boolean cacheBlocks) {
    return getHFileScanner(reader, cacheBlocks, true);
  }

  private static HFileScanner getHFileScanner(HFile.Reader reader, boolean cacheBlocks, boolean doSeek) {
    // NOTE: Only scanners created in Positional Read ("pread") mode could share the same reader,
    //       since scanners in default mode will be seeking w/in the underlying stream
    try {
      HFileScanner scanner = reader.getScanner(cacheBlocks, true);
      if (doSeek) {
        scanner.seekTo(); // places the cursor at the beginning of the first data block.
      }
      return scanner;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize HFile scanner for  " + reader.getPath(), e);
    }
  }
}