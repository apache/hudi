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

package org.apache.hudi.common.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class HFileUtils implements Serializable {

  public static void writeToHFile(Configuration conf, Path hFilePath, List<String> keysToWrite, int minBlockSze)
      throws IOException {
    try {
      CacheConfig cacheConf = new CacheConfig(conf);
      FSDataOutputStream fout = createFSOutput(hFilePath, conf);
      HFileContext meta = new HFileContextBuilder()
          .withBlockSize(minBlockSze)
          .build();
      HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(fout)
          .withFileContext(meta)
          .withComparator(new KeyValue.KVComparator())
          .create();

      writeRecords(writer, keysToWrite);
      fout.close();
    } catch (Exception e) {
      System.err.println("Exception thrown while writing to HFile " + hFilePath);
      e.printStackTrace();
    }
  }

  private static FSDataOutputStream createFSOutput(Path name, Configuration conf) throws IOException {
    //if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = name.getFileSystem(conf).create(name);
    return fout;
  }

  private static void writeRecords(HFile.Writer writer, List<String> keysToWrite) throws IOException {
    writeSomeRecords(writer, keysToWrite);
    writer.close();
  }

  private static int writeSomeRecords(HFile.Writer writer, List<String> keysToWrite)
      throws IOException {
    String value = "value";
    KeyValue kv;
    int keySize = keysToWrite.size();
    for (int i = 0; i < (keySize); i++) {
      String key = keysToWrite.get(i);
      kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
          Bytes.toBytes(value + key));
      writer.append(kv);
    }
    return (keySize);
  }

  public static byte[] getSomeKey(String key) {
    KeyValue kv = new KeyValue(Bytes.toBytes(key),
        Bytes.toBytes("family"), Bytes.toBytes("qual"), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put);
    return kv.getKey();
  }

  public static List<byte[]> getKeys(List<String> keys) {
    List<byte[]> toReturn = new ArrayList<>();
    for (String key : keys) {
      toReturn.add(getSomeKey(key));
    }
    return toReturn;
  }

  public static Map<String, String> readAllRecordsSequentially(Path hFilePath, Configuration conf,
      List<byte[]> keysToLookUp) throws IOException {
    conf.set(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, "true");
    conf.set("CACHE_DATA_IN_L1", "true");
    conf.set("hbase.hfile.drop.behind.compaction", "false");
    CacheConfig cacheConf = new CacheConfig(conf);
    FileSystem fileSystem = hFilePath.getFileSystem(conf);

    HFile.Reader reader = HFile.createReader(fileSystem, hFilePath, cacheConf, conf);
    // Load up the index.
    reader.loadFileInfo();
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, false);
    // Align scanner at start of the file.
    scanner.seekTo();
    long startTime = System.currentTimeMillis();
    Map<String, String> keyValuePairs = readAllRecords(scanner);
    /*  for(byte[] keyBytes: keysToLookUp){
      System.out.println("Key to look up " + Bytes.toString(keyBytes));
    }
    for(Map.Entry<String, String> entry: keyValuePairs.entrySet()){
      System.out.println("entry read "+ entry.getKey() +", "+ entry.getValue());
    }
   */
    long startTime1 = System.currentTimeMillis();
    Map<String, String> toReturn = new HashMap<>();
    int counter = 0;
    for (byte[] keyBytes : keysToLookUp) {
      String keyStr = Bytes.toString(keyBytes);
      if (keyValuePairs.containsKey(keyStr)) {
        toReturn.put(keyStr, keyValuePairs.get(keyStr));
      } else {
        counter++;
      }
    }
    /*  System.out.println("ALL: Total time to look up " + keysToLookUp.size() + " keys. time to read all records and
     look up : "
          + (System.currentTimeMillis() - startTime) + ", time to look up " + (System.currentTimeMillis() -
          startTime1));
    System.out.println("ALL: Total unmatched keys " + counter);*/
    if (counter > 0) {
      System.out.println("ALL: Total unmatched keys " + counter);
    }
    reader.close();
    return toReturn;
  }

  public static Map<String, String> readAllRecordsWithSeek(Path hFilePath, Configuration conf,
      List<byte[]> keysToLookUp) throws IOException {
    conf.set(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, "true");
    conf.set("CACHE_DATA_IN_L1", "true");
    conf.set("hbase.hfile.drop.behind.compaction", "false");
    CacheConfig cacheConf = new CacheConfig(conf);
    cacheConf.setCacheDataInL1(true);
    cacheConf.setEvictOnClose(false);
    FileSystem fileSystem = hFilePath.getFileSystem(conf);

    HFile.Reader reader = HFile.createReader(fileSystem, hFilePath, cacheConf, conf);
    // Load up the index.
    reader.loadFileInfo();
    // Get a scanner that caches and that does not use pread.
    HFileScanner scanner = reader.getScanner(true, true);
    // Align scanner at start of the file.
    //scanner.seekTo();

    long startTime = System.currentTimeMillis();
    Map<String, String> toReturn = new HashMap<>();
    int counter = 0;
    for (byte[] keyBytes : keysToLookUp) {
      if (scanner.seekTo(KeyValue.createKeyValueFromKey(keyBytes)) == 0) {
        ByteBuffer readKey = scanner.getKey();
        ByteBuffer readValue = scanner.getValue();
        toReturn.put(Bytes.toString(Bytes.toBytes(readKey)), Bytes.toString(Bytes.toBytes(readValue)));
      } else {
        counter++;
      }
    }
    //System.out.println("SEEK: Total time to look up " + keysToLookUp.size() + " keys " + (System.currentTimeMillis
    // () - startTime));
    System.out.println("SEEK: Total unmatched keys " + counter);
    reader.close();
    return toReturn;
  }

  private static Map<String, String> readAllRecords(HFileScanner scanner) throws IOException {
    return readAndCheckbytes(scanner);
  }

  // read the records and check
  private static Map<String, String> readAndCheckbytes(HFileScanner scanner)
      throws IOException {
    Map<String, String> keyValuePairs = new HashMap<>();
    while (true) {
      ByteBuffer key = scanner.getKey();
      ByteBuffer val = scanner.getValue();
      keyValuePairs.put(Bytes.toString(Bytes.toBytes(key)), Bytes.toString(Bytes.toBytes(val)));
      if (!scanner.next()) {
        break;
      }
    }
    return keyValuePairs;
  }
}
