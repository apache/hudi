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

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests hoodie write stat {@link HoodieWriteStat}.
 */
public class TestHoodieWriteStat {

  @Test
  public void testSetPaths() {
    String instantTime = TimelineUtils.formatDate(new Date());
    String basePathString = "/data/tables/some-hoodie-table";
    String partitionPathString = "2017/12/31";
    String fileName = UUID.randomUUID().toString();
    String writeToken = "1-0-1";

    StoragePath basePath = new StoragePath(basePathString);
    StoragePath partitionPath = new StoragePath(basePath, partitionPathString);

    StoragePath finalizeFilePath = new StoragePath(partitionPath, FSUtils.makeBaseFileName(instantTime, writeToken, fileName,
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension()));
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new StoragePath(basePath, writeStat.getPath()));

    // test prev base file
    StoragePath prevBaseFilePath = new StoragePath(partitionPath, FSUtils.makeBaseFileName(instantTime, "2-0-3", fileName,
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension()));
    writeStat.setPrevBaseFile(prevBaseFilePath.toString());
    assertEquals(prevBaseFilePath.toString(), writeStat.getPrevBaseFile());

    // test for null tempFilePath
    writeStat = new HoodieWriteStat();
    writeStat.setPath(basePath, finalizeFilePath);
    assertEquals(finalizeFilePath, new StoragePath(basePath, writeStat.getPath()));
    assertNull(writeStat.getTempPath());
  }

  @Test
  public void testRecordsStats() {
    Random random = new Random();

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
    minMaxValues.add(Pair.of(new java.sql.Date(1000 * 60 * 60 * 10), new java.sql.Date(1000 * 60 * 60 * 60)));
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

    Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap = new HashMap<>();
    AtomicInteger counter = new AtomicInteger();
    AtomicInteger finalCounter1 = counter;
    minMaxValues.forEach(entry -> {
      String colName = targetColNamePrefix + "_" + (finalCounter1.getAndIncrement());
      columnRangeMetadataMap.put(colName, HoodieColumnRangeMetadata.<Comparable>create(fileName, colName,
          entry.getKey(), entry.getValue(), 5, 1000, 123456, 123456));
    });

    Map<String, HoodieColumnRangeMetadata<Comparable>> clonedInput = new HashMap<>();
    clonedInput.putAll(columnRangeMetadataMap);

    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setRecordsStats(clonedInput);

    Map<String, HoodieColumnRangeMetadata<Comparable>> actualRecordStats = writeStat.getColumnStats().get();

    assertEquals(columnRangeMetadataMap, actualRecordStats);
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

}
