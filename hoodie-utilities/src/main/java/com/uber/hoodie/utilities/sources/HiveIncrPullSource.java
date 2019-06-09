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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Source to read deltas produced by {@link com.uber.hoodie.utilities.HiveIncrementalPuller}, commit by commit and apply
 * to the target table
 * <p>
 * The general idea here is to have commits sync across the data pipeline.
 * <p>
 * [Source Tables(s)]  ====> HiveIncrementalScanner  ==> incrPullRootPath ==> targetTable {c1,c2,c3,...}
 * {c1,c2,c3,...}       {c1,c2,c3,...}
 * <p>
 * This produces beautiful causality, that makes data issues in ETLs very easy to debug
 */
public class HiveIncrPullSource extends AvroSource {

  private static volatile Logger log = LogManager.getLogger(HiveIncrPullSource.class);

  private final transient FileSystem fs;

  private final String incrPullRootPath;


  /**
   * Configs supported
   */
  static class Config {

    private static final String ROOT_INPUT_PATH_PROP = "hoodie.deltastreamer.source.incrpull.root";
  }

  public HiveIncrPullSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.ROOT_INPUT_PATH_PROP));
    this.incrPullRootPath = props.getString(Config.ROOT_INPUT_PATH_PROP);
    this.fs = FSUtils.getFs(incrPullRootPath, sparkContext.hadoopConfiguration());
  }

  /**
   * Finds the first commit from source, greater than the target's last commit, and reads it out.
   */
  private Optional<String> findCommitToPull(Optional<String> latestTargetCommit)
      throws IOException {

    log.info("Looking for commits ");

    FileStatus[] commitTimePaths = fs.listStatus(new Path(incrPullRootPath));
    List<String> commitTimes = new ArrayList<>(commitTimePaths.length);
    for (FileStatus commitTimePath : commitTimePaths) {
      String[] splits = commitTimePath.getPath().toString().split("/");
      commitTimes.add(splits[splits.length - 1]);
    }
    Collections.sort(commitTimes);
    log.info("Retrieved commit times " + commitTimes);

    if (!latestTargetCommit.isPresent()) {
      // start from the beginning
      return Optional.of(commitTimes.get(0));
    }

    for (String commitTime : commitTimes) {
      //TODO(vc): Add an option to delete consumed commits
      if (commitTime.compareTo(latestTargetCommit.get()) > 0) {
        return Optional.of(commitTime);
      }
    }
    return Optional.empty();
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(
      Optional<String> lastCheckpointStr, long sourceLimit) {
    try {
      // find the source commit to pull
      Optional<String> commitToPull = findCommitToPull(lastCheckpointStr);

      if (!commitToPull.isPresent()) {
        return new InputBatch<>(Optional.empty(),
            lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
      }

      // read the files out.
      List<FileStatus> commitDeltaFiles = Arrays.asList(
          fs.listStatus(new Path(incrPullRootPath, commitToPull.get())));
      String pathStr = commitDeltaFiles.stream().map(f -> f.getPath().toString())
          .collect(Collectors.joining(","));
      JavaPairRDD<AvroKey, NullWritable> avroRDD = sparkContext.newAPIHadoopFile(pathStr,
          AvroKeyInputFormat.class, AvroKey.class, NullWritable.class,
          sparkContext.hadoopConfiguration());
      return new InputBatch<>(Optional.of(avroRDD.keys().map(r -> ((GenericRecord) r.datum()))),
          String.valueOf(commitToPull.get()));
    } catch (IOException ioe) {
      throw new HoodieIOException(
          "Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }
}
