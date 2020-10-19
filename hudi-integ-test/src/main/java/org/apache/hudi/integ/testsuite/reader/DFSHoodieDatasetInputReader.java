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

package org.apache.hudi.integ.testsuite.reader;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * This class helps to generate updates from an already existing hoodie dataset. It supports generating updates in
 * across partitions, files and records.
 */
public class DFSHoodieDatasetInputReader extends DFSDeltaInputReader {

  private static Logger log = LoggerFactory.getLogger(DFSHoodieDatasetInputReader.class);

  private transient JavaSparkContext jsc;
  private String schemaStr;
  private HoodieTableMetaClient metaClient;

  public DFSHoodieDatasetInputReader(JavaSparkContext jsc, String basePath, String schemaStr) {
    this.jsc = jsc;
    this.schemaStr = schemaStr;
    this.metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
  }

  protected List<String> getPartitions(Option<Integer> partitionsLimit) throws IOException {
    List<String> partitionPaths = FSUtils
        .getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(), false);
    // Sort partition so we can pick last N partitions by default
    Collections.sort(partitionPaths);
    if (!partitionPaths.isEmpty()) {
      ValidationUtils.checkArgument(partitionPaths.size() >= partitionsLimit.get(),
          "Cannot generate updates for more partitions " + "than present in the dataset, partitions "
              + "requested " + partitionsLimit.get() + ", partitions present " + partitionPaths.size());
      return partitionPaths.subList(0, partitionsLimit.get());
    }
    return partitionPaths;

  }

  private JavaPairRDD<String, Iterator<FileSlice>> getPartitionToFileSlice(HoodieTableMetaClient metaClient,
      List<String> partitionPaths) {
    TableFileSystemView.SliceView fileSystemView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants());
    // pass num partitions to another method
    JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSliceList = jsc.parallelize(partitionPaths).mapToPair(p -> {
      return new Tuple2<>(p, fileSystemView.getLatestFileSlices(p).iterator());
    });
    return partitionToFileSliceList;
  }

  @Override
  protected long analyzeSingleFile(String filePath) {
    return SparkBasedReader.readParquet(new SparkSession(jsc.sc()), Arrays.asList(filePath),
        Option.empty(), Option.empty()).count();
  }

  private JavaRDD<GenericRecord> fetchAnyRecordsFromDataset(Option<Long> numRecordsToUpdate) throws IOException {
    return fetchRecordsFromDataset(Option.empty(), Option.empty(), numRecordsToUpdate, Option.empty());
  }

  private JavaRDD<GenericRecord> fetchAnyRecordsFromDataset(Option<Long> numRecordsToUpdate, Option<Integer>
      numPartitions) throws IOException {
    return fetchRecordsFromDataset(numPartitions, Option.empty(), numRecordsToUpdate, Option.empty());
  }

  private JavaRDD<GenericRecord> fetchPercentageRecordsFromDataset(Option<Integer> numPartitions, Option<Integer>
      numFiles, Option<Double> percentageRecordsPerFile) throws IOException {
    return fetchRecordsFromDataset(numPartitions, numFiles, Option.empty(), percentageRecordsPerFile);
  }

  private JavaRDD<GenericRecord> fetchRecordsFromDataset(Option<Integer> numPartitions, Option<Integer>
      numFiles, Option<Long> numRecordsToUpdate) throws IOException {
    return fetchRecordsFromDataset(numPartitions, numFiles, numRecordsToUpdate, Option.empty());
  }

  private JavaRDD<GenericRecord> fetchRecordsFromDataset(Option<Integer> numPartitions, Option<Integer> numFiles,
      Option<Long> numRecordsToUpdate, Option<Double> percentageRecordsPerFile) throws IOException {
    log.info("NumPartitions : {}, NumFiles : {}, numRecordsToUpdate : {}, percentageRecordsPerFile : {}",
        numPartitions, numFiles, numRecordsToUpdate, percentageRecordsPerFile);
    List<String> partitionPaths = getPartitions(numPartitions);
    // Read all file slices in the partition
    JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice = getPartitionToFileSlice(metaClient,
        partitionPaths);
    // TODO : read record count from metadata
    // Read the records in a single file
    long recordsInSingleFile = iteratorSize(readParquetOrLogFiles(getSingleSliceFromRDD(partitionToFileSlice)));
    int numFilesToUpdate;
    long numRecordsToUpdatePerFile;
    if (!numFiles.isPresent() || numFiles.get() == 0) {
      // If num files are not passed, find the number of files to update based on total records to update and records
      // per file
      numFilesToUpdate = (int) (numRecordsToUpdate.get() / recordsInSingleFile);
      log.info("Files to update {}", numFilesToUpdate);
      numRecordsToUpdatePerFile = recordsInSingleFile;
    } else {
      // If num files is passed, find the number of records per file based on either percentage or total records to
      // update and num files passed
      numFilesToUpdate = numFiles.get();
      numRecordsToUpdatePerFile = percentageRecordsPerFile.isPresent() ? (long) (recordsInSingleFile
          * percentageRecordsPerFile.get()) : numRecordsToUpdate.get() / numFilesToUpdate;
    }
    // Adjust the number of files to read per partition based on the requested partition & file counts
    Map<String, Integer> adjustedPartitionToFileIdCountMap = getFilesToReadPerPartition(partitionToFileSlice,
        getPartitions(numPartitions).size(), numFilesToUpdate);
    JavaRDD<GenericRecord> updates = projectSchema(generateUpdates(adjustedPartitionToFileIdCountMap,
        partitionToFileSlice, numFilesToUpdate, (int) numRecordsToUpdatePerFile));
    if (numRecordsToUpdate.isPresent() && numFiles.isPresent() && numFiles.get() != 0 && numRecordsToUpdate.get()
        != numRecordsToUpdatePerFile * numFiles.get()) {
      long remainingRecordsToAdd = (numRecordsToUpdate.get() - (numRecordsToUpdatePerFile * numFiles.get()));
      updates = updates.union(projectSchema(jsc.parallelize(generateUpdates(adjustedPartitionToFileIdCountMap,
          partitionToFileSlice, numFilesToUpdate, (int) remainingRecordsToAdd).take((int) remainingRecordsToAdd))));
    }
    log.info("Finished generating updates");
    return updates;
  }

  private JavaRDD<GenericRecord> projectSchema(JavaRDD<GenericRecord> updates) {
    // The records read from the hoodie dataset have the hoodie record fields, rewrite the record to eliminate them
    return updates
        .map(r -> HoodieAvroUtils.rewriteRecordWithOnlyNewSchemaFields(r, new Schema.Parser().parse(schemaStr)));
  }

  private JavaRDD<GenericRecord> generateUpdates(Map<String, Integer> adjustedPartitionToFileIdCountMap,
      JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice, int numFiles, int numRecordsToReadPerFile) {
    return partitionToFileSlice.map(p -> {
      int maxFilesToRead = adjustedPartitionToFileIdCountMap.get(p._1);
      return iteratorLimit(p._2, maxFilesToRead);
    }).flatMap(p -> p).repartition(numFiles).map(fileSlice -> {
      if (numRecordsToReadPerFile > 0) {
        return iteratorLimit(readParquetOrLogFiles(fileSlice), numRecordsToReadPerFile);
      } else {
        return readParquetOrLogFiles(fileSlice);
      }
    }).flatMap(p -> p).map(i -> (GenericRecord) i);
  }

  private Map<String, Integer> getFilesToReadPerPartition(JavaPairRDD<String, Iterator<FileSlice>>
      partitionToFileSlice, Integer numPartitions, Integer numFiles) {
    int numFilesPerPartition = (int) Math.ceil(numFiles / numPartitions);
    Map<String, Integer> partitionToFileIdCountMap = partitionToFileSlice
        .mapToPair(p -> new Tuple2<>(p._1, iteratorSize(p._2))).collectAsMap();
    long totalExistingFilesCount = partitionToFileIdCountMap.values().stream().reduce((a, b) -> a + b).get();
    ValidationUtils.checkArgument(totalExistingFilesCount >= numFiles, "Cannot generate updates "
        + "for more files than present in the dataset, file requested " + numFiles + ", files present "
        + totalExistingFilesCount);
    Map<String, Integer> partitionToFileIdCountSortedMap = partitionToFileIdCountMap
        .entrySet()
        .stream()
        .sorted(comparingByValue())
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
            LinkedHashMap::new));
    // Limit files to be read per partition
    Map<String, Integer> adjustedPartitionToFileIdCountMap = new HashMap<>();
    partitionToFileIdCountSortedMap.entrySet().stream().forEach(e -> {
      if (e.getValue() <= numFilesPerPartition) {
        adjustedPartitionToFileIdCountMap.put(e.getKey(), e.getValue());
      } else {
        adjustedPartitionToFileIdCountMap.put(e.getKey(), numFilesPerPartition);
      }
    });
    return adjustedPartitionToFileIdCountMap;
  }

  private FileSlice getSingleSliceFromRDD(JavaPairRDD<String, Iterator<FileSlice>> partitionToFileSlice) {
    return partitionToFileSlice.map(f -> {
      FileSlice slice = f._2.next();
      FileSlice newSlice = new FileSlice(slice.getFileGroupId(), slice.getBaseInstantTime());
      if (slice.getBaseFile().isPresent()) {
        newSlice.setBaseFile(slice.getBaseFile().get());
      } else {
        slice.getLogFiles().forEach(l -> {
          newSlice.addLogFile(l);
        });
      }
      return newSlice;
    }).take(1).get(0);
  }

  private Iterator<IndexedRecord> readParquetOrLogFiles(FileSlice fileSlice) throws IOException {
    if (fileSlice.getBaseFile().isPresent()) {
      Iterator<IndexedRecord> itr =
          new ParquetReaderIterator<IndexedRecord>(AvroParquetReader.<IndexedRecord>builder(new
              Path(fileSlice.getBaseFile().get().getPath())).withConf(metaClient.getHadoopConf()).build());
      return itr;
    } else {
      // If there is no data file, fall back to reading log files
      HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(metaClient.getFs(),
          metaClient.getBasePath(),
          fileSlice.getLogFiles().map(l -> l.getPath().getName()).collect(Collectors.toList()),
          new Schema.Parser().parse(schemaStr), metaClient.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants().lastInstant().get().getTimestamp(),
          HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, true, false,
          HoodieMemoryConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE,
          HoodieMemoryConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH);
      // readAvro log files
      Iterable<HoodieRecord<? extends HoodieRecordPayload>> iterable = () -> scanner.iterator();
      Schema schema = new Schema.Parser().parse(schemaStr);
      return StreamSupport.stream(iterable.spliterator(), false)
          .map(e -> {
            try {
              return (IndexedRecord) e.getData().getInsertValue(schema).get();
            } catch (IOException io) {
              throw new UncheckedIOException(io);
            }
          }).iterator();
    }
  }

  /**
   * Returns the number of elements remaining in {@code iterator}. The iterator
   * will be left exhausted: its {@code hasNext()} method will return
   * {@code false}.
   */
  private static int iteratorSize(Iterator<?> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    return count;
  }

  /**
   * Creates an iterator returning the first {@code limitSize} elements of the
   * given iterator. If the original iterator does not contain that many
   * elements, the returned iterator will have the same behavior as the original
   * iterator. The returned iterator supports {@code remove()} if the original
   * iterator does.
   *
   * @param iterator the iterator to limit
   * @param limitSize the maximum number of elements in the returned iterator
   * @throws IllegalArgumentException if {@code limitSize} is negative
   */
  private static <T> Iterator<T> iteratorLimit(
      final Iterator<T> iterator, final int limitSize) {
    ValidationUtils.checkArgument(iterator != null, "iterator is null");
    ValidationUtils.checkArgument(limitSize >= 0, "limit is negative");
    return new Iterator<T>() {
      private int count;

      @Override
      public boolean hasNext() {
        return count < limitSize && iterator.hasNext();
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        count++;
        return iterator.next();
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }

  @Override
  public JavaRDD<GenericRecord> read(long numRecords) throws IOException {
    return fetchAnyRecordsFromDataset(Option.of(numRecords));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, long approxNumRecords) throws IOException {
    return fetchAnyRecordsFromDataset(Option.of(approxNumRecords), Option.of(numPartitions));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, long numRecords) throws IOException {
    return fetchRecordsFromDataset(Option.of(numPartitions), Option.of(numFiles), Option.of(numRecords));
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, double percentageRecordsPerFile)
      throws IOException {
    return fetchPercentageRecordsFromDataset(Option.of(numPartitions), Option.of(numFiles),
        Option.of(percentageRecordsPerFile));
  }
}
