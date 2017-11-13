/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Source to read data from a given DFS directory structure, incrementally
 */
public class DFSSource extends Source {

  /**
   * Configs supported
   */
  static class Config {

    private final static String ROOT_INPUT_PATH_PROP = "hoodie.deltastreamer.source.dfs.root";
  }

  private final static List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");

  private final transient FileSystem fs;

  public DFSSource(PropertiesConfiguration config, JavaSparkContext sparkContext,
      SourceDataFormat dataFormat, SchemaProvider schemaProvider) {
    super(config, sparkContext, dataFormat, schemaProvider);
    this.fs = FSUtils.getFs();
    DataSourceUtils.checkRequiredProperties(config, Arrays.asList(Config.ROOT_INPUT_PATH_PROP));
  }


  public static JavaRDD<GenericRecord> fromAvroFiles(final AvroConvertor convertor, String pathStr,
      JavaSparkContext sparkContext) {
    JavaPairRDD<AvroKey, NullWritable> avroRDD = sparkContext.newAPIHadoopFile(pathStr,
        AvroKeyInputFormat.class,
        AvroKey.class,
        NullWritable.class,
        sparkContext.hadoopConfiguration());
    return avroRDD.keys().map(r -> ((GenericRecord) r.datum()));
  }

  public static JavaRDD<GenericRecord> fromJsonFiles(final AvroConvertor convertor, String pathStr,
      JavaSparkContext sparkContext) {
    return sparkContext.textFile(pathStr).map((String j) -> {
      return convertor.fromJson(j);
    });
  }

  public static JavaRDD<GenericRecord> fromFiles(SourceDataFormat dataFormat,
      final AvroConvertor convertor, String pathStr, JavaSparkContext sparkContext) {
    if (dataFormat == SourceDataFormat.AVRO) {
      return DFSSource.fromAvroFiles(convertor, pathStr, sparkContext);
    } else if (dataFormat == SourceDataFormat.JSON) {
      return DFSSource.fromJsonFiles(convertor, pathStr, sparkContext);
    } else {
      throw new HoodieNotSupportedException("Unsupported data format :" + dataFormat);
    }
  }


  @Override
  public Pair<Optional<JavaRDD<GenericRecord>>, String> fetchNewData(
      Optional<String> lastCheckpointStr, long maxInputBytes) {

    try {
      // obtain all eligible files under root folder.
      List<FileStatus> eligibleFiles = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> fitr = fs
          .listFiles(new Path(config.getString(Config.ROOT_INPUT_PATH_PROP)), true);
      while (fitr.hasNext()) {
        LocatedFileStatus fileStatus = fitr.next();
        if (fileStatus.isDirectory() ||
            IGNORE_FILEPREFIX_LIST.stream()
                .filter(pfx -> fileStatus.getPath().getName().startsWith(pfx)).count() > 0) {
          continue;
        }
        eligibleFiles.add(fileStatus);
      }
      // sort them by modification time.
      eligibleFiles.sort((FileStatus f1, FileStatus f2) -> Long.valueOf(f1.getModificationTime())
          .compareTo(Long.valueOf(f2.getModificationTime())));

      // Filter based on checkpoint & input size, if needed
      long currentBytes = 0;
      long maxModificationTime = Long.MIN_VALUE;
      List<FileStatus> filteredFiles = new ArrayList<>();
      for (FileStatus f : eligibleFiles) {
        if (lastCheckpointStr.isPresent() && f.getModificationTime() <= Long
            .valueOf(lastCheckpointStr.get())) {
          // skip processed files
          continue;
        }

        maxModificationTime = f.getModificationTime();
        currentBytes += f.getLen();
        filteredFiles.add(f);
        if (currentBytes >= maxInputBytes) {
          // we have enough data, we are done
          break;
        }
      }

      // no data to read
      if (filteredFiles.size() == 0) {
        return new ImmutablePair<>(Optional.empty(),
            lastCheckpointStr.isPresent() ? lastCheckpointStr.get()
                : String.valueOf(Long.MIN_VALUE));
      }

      // read the files out.
      String pathStr = filteredFiles.stream().map(f -> f.getPath().toString())
          .collect(Collectors.joining(","));
      String schemaStr = schemaProvider.getSourceSchema().toString();
      final AvroConvertor avroConvertor = new AvroConvertor(schemaStr);

      return new ImmutablePair<>(
          Optional.of(DFSSource.fromFiles(dataFormat, avroConvertor, pathStr, sparkContext)),
          String.valueOf(maxModificationTime));
    } catch (IOException ioe) {
      throw new HoodieIOException(
          "Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }
}
