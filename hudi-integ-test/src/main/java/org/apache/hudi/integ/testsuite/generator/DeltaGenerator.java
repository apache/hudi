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

package org.apache.hudi.integ.testsuite.generator;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.converter.Converter;
import org.apache.hudi.integ.testsuite.converter.DeleteConverter;
import org.apache.hudi.integ.testsuite.converter.UpdateConverter;
import org.apache.hudi.integ.testsuite.reader.DFSAvroDeltaInputReader;
import org.apache.hudi.integ.testsuite.reader.DFSHoodieDatasetInputReader;
import org.apache.hudi.integ.testsuite.reader.DeltaInputReader;
import org.apache.hudi.integ.testsuite.schema.SchemaUtils;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterAdapter;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterFactory;
import org.apache.hudi.keygen.BuiltinKeyGenerator;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import scala.Tuple2;

/**
 * The delta generator generates all types of workloads (insert, update) for the given configs.
 */
public class DeltaGenerator implements Serializable {

  private static Logger log = LoggerFactory.getLogger(DeltaGenerator.class);

  private DFSDeltaConfig deltaOutputConfig;
  private transient JavaSparkContext jsc;
  private transient SparkSession sparkSession;
  private String schemaStr;
  private List<String> recordRowKeyFieldNames;
  private List<String> partitionPathFieldNames;
  private int batchId;

  public DeltaGenerator(DFSDeltaConfig deltaOutputConfig, JavaSparkContext jsc, SparkSession sparkSession,
                        String schemaStr, BuiltinKeyGenerator keyGenerator) {
    this.deltaOutputConfig = deltaOutputConfig;
    this.jsc = jsc;
    this.sparkSession = sparkSession;
    this.schemaStr = schemaStr;
    this.recordRowKeyFieldNames = keyGenerator.getRecordKeyFieldNames();
    this.partitionPathFieldNames = keyGenerator.getPartitionPathFields();
  }

  public Pair<Integer, JavaRDD<DeltaWriteStats>> writeRecords(JavaRDD<GenericRecord> records) {
    if (deltaOutputConfig.shouldDeleteOldInputData() && batchId > 1) {
      Path oldInputDir = new Path(deltaOutputConfig.getDeltaBasePath(), Integer.toString(batchId - 1));
      try {
        FileSystem fs = FSUtils.getFs(oldInputDir.toString(), deltaOutputConfig.getConfiguration());
        fs.delete(oldInputDir, true);
      } catch (IOException e) {
        log.error("Failed to delete older input data direcory " + oldInputDir, e);
      }
    }

    // The following creates a new anonymous function for iterator and hence results in serialization issues
    JavaRDD<DeltaWriteStats> ws = records.mapPartitions(itr -> {
      try {
        DeltaWriterAdapter<GenericRecord> deltaWriterAdapter = DeltaWriterFactory
            .getDeltaWriterAdapter(deltaOutputConfig, batchId);
        return Collections.singletonList(deltaWriterAdapter.write(itr)).iterator();
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    }).flatMap(List::iterator);
    batchId++;
    return Pair.of(batchId, ws);
  }

  public int getBatchId() {
    return batchId;
  }

  public JavaRDD<GenericRecord> generateInserts(Config operation) {
    int numPartitions = operation.getNumInsertPartitions();
    long recordsPerPartition = operation.getNumRecordsInsert();
    int minPayloadSize = operation.getRecordSize();
    int startPartition = operation.getStartPartition();

    // Each spark partition below will generate records for a single partition given by the integer index.
    List<Integer> partitionIndexes = IntStream.rangeClosed(0 + startPartition, numPartitions + startPartition - 1)
        .boxed().collect(Collectors.toList());

    JavaRDD<GenericRecord> inputBatch = jsc.parallelize(partitionIndexes, numPartitions)
        .mapPartitionsWithIndex((index, p) -> {
          return new LazyRecordGeneratorIterator(new FlexibleSchemaRecordGenerationIterator(recordsPerPartition,
              minPayloadSize, schemaStr, partitionPathFieldNames, numPartitions, startPartition));
        }, true)
        .map(record -> {
          record.put(SchemaUtils.SOURCE_ORDERING_FIELD, batchId);
          return record;
        });

    if (deltaOutputConfig.getInputParallelism() < numPartitions) {
      inputBatch = inputBatch.coalesce(deltaOutputConfig.getInputParallelism());
    }
    return inputBatch;
  }

  public JavaRDD<GenericRecord> generateUpdates(Config config) throws IOException {
    if (deltaOutputConfig.getDeltaOutputMode() == DeltaOutputMode.DFS) {
      JavaRDD<GenericRecord> inserts = null;
      if (config.getNumRecordsInsert() > 0) {
        inserts = generateInserts(config);
      }
      DeltaInputReader deltaInputReader = null;
      JavaRDD<GenericRecord> adjustedRDD = null;
      if (config.getNumUpsertPartitions() != 0) {
        if (config.getNumUpsertPartitions() < 0) {
          // randomly generate updates for a given number of records without regard to partitions and files
          deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
              ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
          adjustedRDD = deltaInputReader.read(config.getNumRecordsUpsert());
          adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsUpsert());
        } else {
          if (((DFSDeltaConfig) deltaOutputConfig).shouldUseHudiToGenerateUpdates()) {
            deltaInputReader =
                new DFSHoodieDatasetInputReader(jsc, ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(),
                    schemaStr);
            if (config.getFractionUpsertPerFile() > 0) {
              adjustedRDD = deltaInputReader.read(config.getNumUpsertPartitions(), config.getNumUpsertFiles(),
                  config.getFractionUpsertPerFile());
            } else {
              adjustedRDD = deltaInputReader.read(config.getNumUpsertPartitions(), config.getNumUpsertFiles(), config
                  .getNumRecordsUpsert());
            }
          } else {
            deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
                ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
            adjustedRDD = deltaInputReader.read(config.getNumRecordsUpsert());
            adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsUpsert());
          }
        }

        // persist this since we will make multiple passes over this
        int numPartition = Math.min(deltaOutputConfig.getInputParallelism(),
            Math.max(1, config.getNumUpsertPartitions()));
        log.info("Repartitioning records into " + numPartition + " partitions for updates");
        adjustedRDD = adjustedRDD.repartition(numPartition);
        log.info("Repartitioning records done for updates");
        UpdateConverter converter = new UpdateConverter(schemaStr, config.getRecordSize(),
            partitionPathFieldNames, recordRowKeyFieldNames);
        JavaRDD<GenericRecord> convertedRecords = converter.convert(adjustedRDD);
        JavaRDD<GenericRecord> updates = convertedRecords.map(record -> {
          record.put(SchemaUtils.SOURCE_ORDERING_FIELD, batchId);
          return record;
        });
        updates.persist(StorageLevel.DISK_ONLY());
        if (inserts == null) {
          inserts = updates;
        } else {
          inserts = inserts.union(updates);
        }
      }
      return inserts;
      // TODO : Generate updates for only N partitions.
    } else {
      throw new IllegalArgumentException("Other formats are not supported at the moment");
    }
  }

  public JavaRDD<GenericRecord> generateDeletes(Config config) throws IOException {
    if (deltaOutputConfig.getDeltaOutputMode() == DeltaOutputMode.DFS) {
      DeltaInputReader deltaInputReader = null;
      JavaRDD<GenericRecord> adjustedRDD = null;

      if (config.getNumDeletePartitions() < 1) {
        // randomly generate deletes for a given number of records without regard to partitions and files
        deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
            ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
        adjustedRDD = deltaInputReader.read(config.getNumRecordsDelete());
        adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsDelete());
      } else {
        if (((DFSDeltaConfig) deltaOutputConfig).shouldUseHudiToGenerateUpdates()) {
          deltaInputReader =
              new DFSHoodieDatasetInputReader(jsc, ((DFSDeltaConfig) deltaOutputConfig).getDatasetOutputPath(),
                  schemaStr);
          if (config.getFractionUpsertPerFile() > 0) {
            adjustedRDD = deltaInputReader.read(config.getNumDeletePartitions(), config.getNumUpsertFiles(),
                config.getFractionUpsertPerFile());
          } else {
            adjustedRDD = deltaInputReader.read(config.getNumDeletePartitions(), config.getNumUpsertFiles(), config
                .getNumRecordsDelete());
          }
        } else {
          deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
              ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
          adjustedRDD = deltaInputReader.read(config.getNumRecordsDelete());
          adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsDelete());
        }
      }

      log.info("Repartitioning records for delete");
      // persist this since we will make multiple passes over this
      adjustedRDD = adjustedRDD.repartition(jsc.defaultParallelism());
      Converter converter = new DeleteConverter(schemaStr, config.getRecordSize());
      JavaRDD<GenericRecord> convertedRecords = converter.convert(adjustedRDD);
      JavaRDD<GenericRecord> deletes = convertedRecords.map(record -> {
        record.put(SchemaUtils.SOURCE_ORDERING_FIELD, batchId);
        return record;
      });
      deletes.persist(StorageLevel.DISK_ONLY());
      return deletes;
    } else {
      throw new IllegalArgumentException("Other formats are not supported at the moment");
    }
  }

  public Map<Integer, Long> getPartitionToCountMap(JavaRDD<GenericRecord> records) {
    // Requires us to keep the partitioner the same
    return records.mapPartitionsWithIndex((index, itr) -> {
      Iterable<GenericRecord> newIterable = () -> itr;
      // parallelize counting for speed
      long count = StreamSupport.stream(newIterable.spliterator(), true).count();
      return Arrays.asList(new Tuple2<>(index, count)).iterator();
    }, true).mapToPair(i -> i).collectAsMap();
  }

  public Map<Integer, Long> getAdjustedPartitionsCount(Map<Integer, Long> partitionCountMap, long
      recordsToRemove) {
    long remainingRecordsToRemove = recordsToRemove;
    Iterator<Map.Entry<Integer, Long>> iterator = partitionCountMap.entrySet().iterator();
    Map<Integer, Long> adjustedPartitionCountMap = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<Integer, Long> entry = iterator.next();
      if (entry.getValue() < remainingRecordsToRemove) {
        remainingRecordsToRemove -= entry.getValue();
        adjustedPartitionCountMap.put(entry.getKey(), 0L);
      } else {
        long newValue = entry.getValue() - remainingRecordsToRemove;
        remainingRecordsToRemove = 0;
        adjustedPartitionCountMap.put(entry.getKey(), newValue);
      }
      if (remainingRecordsToRemove == 0) {
        break;
      }
    }
    return adjustedPartitionCountMap;
  }

  public JavaRDD<GenericRecord> adjustRDDToGenerateExactNumUpdates(JavaRDD<GenericRecord> updates, JavaSparkContext
      jsc, long totalRecordsRequired) {
    Map<Integer, Long> actualPartitionCountMap = getPartitionToCountMap(updates);
    long totalRecordsGenerated = actualPartitionCountMap.values().stream().mapToLong(Long::longValue).sum();
    if (isSafeToTake(totalRecordsRequired, totalRecordsGenerated)) {
      // Generate totalRecordsRequired - totalRecordsGenerated new records and union the RDD's
      // NOTE : This performs poorly when totalRecordsRequired >> totalRecordsGenerated. Hence, always
      // ensure that enough inserts are created before hand (this needs to be noted during the WorkflowDag creation)
      long sizeOfUpdateRDD = totalRecordsGenerated;
      while (totalRecordsRequired != sizeOfUpdateRDD) {
        long recordsToTake = (totalRecordsRequired - sizeOfUpdateRDD) > sizeOfUpdateRDD
            ? sizeOfUpdateRDD : (totalRecordsRequired - sizeOfUpdateRDD);
        if ((totalRecordsRequired - sizeOfUpdateRDD) > recordsToTake && recordsToTake <= sizeOfUpdateRDD) {
          updates = updates.union(updates);
          sizeOfUpdateRDD *= 2;
        } else {
          List<GenericRecord> remainingUpdates = updates.take((int) (recordsToTake));
          updates = updates.union(jsc.parallelize(remainingUpdates));
          sizeOfUpdateRDD = sizeOfUpdateRDD + recordsToTake;
        }
      }
      return updates;
    } else if (totalRecordsRequired < totalRecordsGenerated) {
      final Map<Integer, Long> adjustedPartitionCountMap = getAdjustedPartitionsCount(actualPartitionCountMap,
          totalRecordsGenerated - totalRecordsRequired);
      // limit counts across partitions to meet the exact number of updates required
      JavaRDD<GenericRecord> trimmedRecords = updates.mapPartitionsWithIndex((index, itr) -> {
        int counter = 1;
        List<GenericRecord> entriesToKeep = new ArrayList<>();
        if (!adjustedPartitionCountMap.containsKey(index)) {
          return itr;
        } else {
          long recordsToKeepForThisPartition = adjustedPartitionCountMap.get(index);
          while (counter <= recordsToKeepForThisPartition && itr.hasNext()) {
            entriesToKeep.add(itr.next());
            counter++;
          }
          return entriesToKeep.iterator();
        }
      }, true);
      return trimmedRecords;
    }
    return updates;
  }

  private boolean isSafeToTake(long totalRecords, long totalRecordsGenerated) {
    // TODO : Ensure that the difference between totalRecords and totalRecordsGenerated is not too big, if yes,
    // then there are fewer number of records on disk, hence we need to find another way to generate updates when
    // requiredUpdates >> insertedRecords
    return totalRecords > totalRecordsGenerated;
  }

}
