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

package org.apache.hudi.functional;

import org.apache.hudi.avro.model.DecimalWrapper;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.io.storage.HoodieSeekingFileReader;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColStatsRecordWithMetadataRecord extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestColStatsRecordWithMetadataRecord.class);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieCreateHandle");
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testColsStatsSerDe() throws Exception {

    Random random = new Random();
    // create a data table which will auto create mdt table as well
    HoodieWriteConfig cfg = getConfig();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      writeData(client, InProcessTimeGenerator.createNewInstantTime(), 100, false);
    }

    String fileName = "file.parquet";
    String targetColNamePrefix = "col";
    List<Pair<Comparable, Comparable>> minMaxValues = new ArrayList<>();
    // string
    minMaxValues.add(Pair.of("abcdec", "zyxwvu"));
    // Utf8
    minMaxValues.add(Pair.of(new Utf8(getUTF8Bytes("abcdec")), new Utf8(getUTF8Bytes("zyxwvu"))));
    // Int
    minMaxValues.add(Pair.of(new Integer(-1000), new Integer(999999)));
    // Long
    minMaxValues.add(Pair.of(new Long(-100000L), Long.MAX_VALUE));
    // boolean
    minMaxValues.add(Pair.of(false, true));
    // double
    minMaxValues.add(Pair.of(new Double(0.123), new Double(10.123)));
    // float
    minMaxValues.add(Pair.of(new Float(0.0123), new Float(200.123)));
    // Date
    minMaxValues.add(Pair.of(new Date(1000 * 60 * 60 * 10), new Date(1000 * 60 * 60 * 60)));
    // LocalDate
    minMaxValues.add(Pair.of(LocalDate.ofEpochDay(1000 * 60 * 60 * 10), LocalDate.ofEpochDay(1000 * 60 * 60 * 60)));
    // Timestamp
    minMaxValues.add(Pair.of(new Timestamp(1000 * 60 * 60 * 10), new Timestamp(1000 * 60 * 60 * 60)));
    minMaxValues.add(generateRandomMinMaxValue(random, (Functions.Function1<Random, Comparable>) random1
        -> new Timestamp(random1.nextInt(1000) * 60 * 60 * 1000)));

    //Bytes
    byte[] bytes1 = new byte[10];
    byte[] bytes2 = new byte[10];
    random.nextBytes(bytes1);
    random.nextBytes(bytes2);
    ByteBuffer val1ByteBuffer = ByteBuffer.wrap(bytes1);
    ByteBuffer val2ByteBuffer = ByteBuffer.wrap(bytes2);
    Comparable minValue = val1ByteBuffer;
    Comparable maxValue = val2ByteBuffer;
    if (val1ByteBuffer.compareTo(val2ByteBuffer) >= 0) {
      minValue = val2ByteBuffer;
      maxValue = val1ByteBuffer;
    }
    minMaxValues.add(Pair.of(minValue, maxValue));

    // Big Decimal
    BigDecimal val1 = new BigDecimal(String.format(Locale.ENGLISH, "%5f", random.nextFloat()));
    BigDecimal val2 = new BigDecimal(String.format(Locale.ENGLISH, "%5f", random.nextFloat()));
    if (val1.compareTo(val2) > 0) {
      minMaxValues.add(Pair.of(val2, val1));
    } else {
      minMaxValues.add(Pair.of(val1, val2));
    }

    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    AtomicInteger finalCounter1 = counter;
    minMaxValues.forEach(entry -> {
      columnRangeMetadata.add(HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColNamePrefix + "_" + (finalCounter1.getAndIncrement()),
          entry.getKey(), entry.getValue(), 5, 1000, 123456, 123456));
    });

    // create mdt records
    List<HoodieRecord<HoodieMetadataPayload>> columnStatsRecords =
        HoodieMetadataPayload.createColumnStatsRecords("p1", columnRangeMetadata, false)
            .map(record -> (HoodieRecord<HoodieMetadataPayload>) record).collect(Collectors.toList());

    Collections.sort(columnStatsRecords, new Comparator<HoodieRecord<HoodieMetadataPayload>>() {
      @Override
      public int compare(HoodieRecord<HoodieMetadataPayload> o1, HoodieRecord<HoodieMetadataPayload> o2) {
        return o1.getRecordKey().compareTo(o2.getRecordKey());
      }
    });

    List<HoodieRecord<HoodieMetadataPayload>> expectedColumnStatsRecords =
        HoodieMetadataPayload.createColumnStatsRecords("p1", columnRangeMetadata, false)
            .map(record -> (HoodieRecord<HoodieMetadataPayload>) record).collect(Collectors.toList());

    Collections.sort(expectedColumnStatsRecords, new Comparator<HoodieRecord<HoodieMetadataPayload>>() {
      @Override
      public int compare(HoodieRecord<HoodieMetadataPayload> o1, HoodieRecord<HoodieMetadataPayload> o2) {
        return o1.getRecordKey().compareTo(o2.getRecordKey());
      }
    });

    HoodieWriteConfig mdtWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(cfg, HoodieFailedWritesCleaningPolicy.EAGER);
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder().setBasePath(mdtWriteConfig.getBasePath()).setConf(context.getStorageConf().newInstance()).build();

    HoodieTable table = HoodieSparkTable.create(mdtWriteConfig, context, mdtMetaClient);
    String newCommitTime = InProcessTimeGenerator.createNewInstantTime();
    HoodieCreateHandle handle = new HoodieCreateHandle(mdtWriteConfig, newCommitTime, table, COLUMN_STATS.getPartitionPath(), "col-stats-00001-0", new PhoneyTaskContextSupplier());

    // write the record to hfile.
    Schema writeSchema = new Schema.Parser().parse(mdtWriteConfig.getSchema());
    TypedProperties properties = new TypedProperties();
    columnStatsRecords.forEach(record -> handle.write(record, writeSchema, properties));
    WriteStatus writeStatus = (WriteStatus) handle.close().get(0);
    String filePath = writeStatus.getStat().getPath();

    // read the hfile using base file reader.
    StoragePath baseFilePath = new StoragePath(mdtMetaClient.getBasePath() + "/" + filePath);
    HoodieSeekingFileReader baseFileReader = (HoodieSeekingFileReader<?>) HoodieIOFactory.getIOFactory(mdtMetaClient.getStorage())
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, baseFilePath);

    ClosableIterator itr = baseFileReader.getRecordIterator();
    List<HoodieRecord<HoodieMetadataPayload>> allRecords = new ArrayList<>();
    while (itr.hasNext()) {
      GenericRecord genericRecord = (GenericRecord) ((HoodieRecord) itr.next()).getData();
      HoodieRecord<HoodieMetadataPayload> mdtRec = SpillableMapUtils.convertToHoodieRecordPayload(genericRecord,
          mdtWriteConfig.getPayloadClass(), mdtWriteConfig.getPreCombineField(),
          Pair.of(mdtMetaClient.getTableConfig().getRecordKeyFieldProp(), mdtMetaClient.getTableConfig().getPartitionFieldProp()),
          false, Option.of(COLUMN_STATS.getPartitionPath()), Option.empty());
      allRecords.add(mdtRec);
    }

    assertEquals(columnStatsRecords.size(), allRecords.size());
    // validate the min and max values.
    counter = new AtomicInteger(0);
    AtomicInteger finalCounter = counter;

    allRecords.forEach(record -> {
      HoodieMetadataColumnStats actualColStatsMetadata = record.getData().getColumnStatMetadata().get();
      HoodieMetadataColumnStats expectedColStatsMetadata = expectedColumnStatsRecords.get(finalCounter.getAndIncrement()).getData().getColumnStatMetadata().get();
      LOG.info("Validating " + expectedColStatsMetadata.getColumnName() + ", " + expectedColStatsMetadata.getMinValue().getClass().getSimpleName());
      if (expectedColStatsMetadata.getMinValue().getClass().getSimpleName().equals(DecimalWrapper.class.getSimpleName())) {
        // Big decimal gets wrapped w/ Decimal wrapper and converts to bytes.
        assertEquals(expectedColStatsMetadata.getMinValue().toString(), actualColStatsMetadata.getMinValue().toString());
        assertEquals(expectedColStatsMetadata.getMaxValue().toString(), actualColStatsMetadata.getMaxValue().toString());
      } else {
        assertEquals(expectedColStatsMetadata.getMinValue(), actualColStatsMetadata.getMinValue());
        assertEquals(expectedColStatsMetadata.getMaxValue(), actualColStatsMetadata.getMaxValue());
      }
    });
  }

  @Test
  public void testColsStatsMergeString() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> {
      byte[] bytes = new byte[10];
      random.nextBytes(bytes);
      return new String(bytes, Charset.forName("UTF-8"));
    });
  }

  @Test
  public void testColsStatsMergeInt() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> random.nextInt());
  }

  @Test
  public void testColsStatsMergeLong() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> random.nextLong());
  }

  @Test
  public void testColsStatsMergeDouble() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> random.nextDouble());
  }

  @Test
  public void testColsStatsMergeBoolean() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> random.nextBoolean());
  }

  @Test
  public void testColsStatsMergeFloat() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> random.nextFloat());
  }

  @Test
  public void testColsStatsMergeBytes() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> {
      byte[] bytes = new byte[20];
      random.nextBytes(bytes);
      return ByteBuffer.wrap(bytes);
    });
  }

  @Test
  public void testColsStatsMergeDate() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> new Date(random.nextInt(100) * 60 * 60 * 1000));
  }

  @Test
  public void testColsStatsMergeLocalDate() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> LocalDate.ofEpochDay(random.nextInt(100) * 60 * 60 * 1000));
  }

  @Test
  public void testColsStatsMergeLocalTimestamp() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> new Timestamp(random.nextInt(1000) * 60 * 60 * 1000));
  }

  @Test
  public void testColsStatsMergeBigDecimal() throws Exception {
    generateNColStatsEntriesAndValidateMerge((Functions.Function1<Random, Comparable>) random -> new BigDecimal(String.format(Locale.ENGLISH, "%5f", random.nextFloat())));
  }

  private void generateNColStatsEntriesAndValidateMerge(Functions.Function1<Random, Comparable> randomValueGenFunc) {
    String fileName = "abc.parquet";
    String colName = "colName";
    Random random = new Random();

    List<Pair<Comparable, Comparable>> minMaxValues = new ArrayList<>();
    List<Comparable> allMinValues = new ArrayList<>();
    List<Comparable> allMaxValues = new ArrayList<>();
    // generate 50 min, max values and merge them.
    for (int i = 0; i < 50; i++) {
      Pair<Comparable, Comparable> minMaxValue = generateRandomMinMaxValue(random, randomValueGenFunc);
      minMaxValues.add(Pair.of(minMaxValue.getKey(), minMaxValue.getValue()));
      allMinValues.add(minMaxValue.getKey());
      allMaxValues.add(minMaxValue.getValue());
    }

    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata = new ArrayList<>();
    minMaxValues.forEach(entry -> {
      columnRangeMetadata.add(HoodieColumnRangeMetadata.<Comparable>create(fileName, colName,
          entry.getKey(), entry.getValue(), 5, 1000, 123456, 123456));
    });

    HoodieColumnRangeMetadata<Comparable> mergedColStatsRangeMetadata = (HoodieColumnRangeMetadata<Comparable>) columnRangeMetadata.stream()
        .reduce((left, right) -> HoodieColumnRangeMetadata.merge(left, right)).get();

    Object finalMin = getExpectedMinValue(allMinValues);
    Object finalMax = getExpectedMaxValue(allMaxValues);

    assertEquals(finalMin, mergedColStatsRangeMetadata.getMinValue());
    assertEquals(finalMax, mergedColStatsRangeMetadata.getMaxValue());
  }

  private Comparable getExpectedMinValue(List<Comparable> allValues) {
    return allValues.stream().reduce((left, right) -> {
      if (left.compareTo(right) < 0) {
        return left;
      } else {
        return right;
      }
    }).get();
  }

  private Comparable getExpectedMaxValue(List<Comparable> allValues) {
    return allValues.stream().reduce((left, right) -> {
      if (left.compareTo(right) >= 0) {
        return left;
      } else {
        return right;
      }
    }).get();
  }

  private Pair<Comparable, Comparable> generateRandomMinMaxValue(Random random, Functions.Function1<Random, Comparable> randomValueGenFunc) {
    Comparable value1 = randomValueGenFunc.apply(random);
    Comparable value2 = randomValueGenFunc.apply(random);
    if (value1.compareTo(value2) > 0) {
      return Pair.of(value2, value1);
    } else {
      return Pair.of(value1, value2);
    }
  }

  private List<WriteStatus> writeData(SparkRDDWriteClient client, String instant, int numRecords, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    JavaRDD records = jsc.parallelize(dataGen.generateInserts(instant, numRecords), 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    client.startCommitWithTime(instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  class PhoneyTaskContextSupplier extends TaskContextSupplier {

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> 1;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> 1;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> 1L;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }
  }
}
