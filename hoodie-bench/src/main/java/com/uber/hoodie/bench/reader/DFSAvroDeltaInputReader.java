/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.bench.reader;

import com.uber.hoodie.bench.DeltaInputFormat;
import com.uber.hoodie.bench.DeltaSinkType;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * A reader of {@link DeltaSinkType#DFS} and {@link DeltaInputFormat#AVRO}
 */
public class DFSAvroDeltaInputReader extends DFSDeltaInputReader {

  private final SparkSession sparkSession;
  private final String schemaStr;
  private final String basePath;
  private final Optional<String> structName;
  private final Optional<String> nameSpace;
  protected PathFilter filter = (path) -> {
    if (path.toUri().toString().contains(".avro")) {
      return true;
    } else {
      return false;
    }
  };

  public DFSAvroDeltaInputReader(SparkSession sparkSession, String schemaStr, String basePath,
      Optional<String> structName,
      Optional<String> nameSpace) {
    this.sparkSession = sparkSession;
    this.schemaStr = schemaStr;
    this.basePath = basePath;
    this.structName = structName;
    this.nameSpace = nameSpace;
  }

  @Override
  public JavaRDD<GenericRecord> read(long totalRecordsToRead) throws IOException {
    return SparkBasedReader.readAvro(sparkSession, schemaStr, getFilePathsToRead(basePath, filter, totalRecordsToRead),
        structName, nameSpace);
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, long approxNumRecords) throws IOException {
    throw new UnsupportedOperationException("cannot generate updates");
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, long approxNumRecords) throws IOException {
    throw new UnsupportedOperationException("cannot generate updates");
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, double percentageRecordsPerFile)
      throws IOException {
    throw new UnsupportedOperationException("cannot generate updates");
  }


  @Override
  protected long analyzeSingleFile(String filePath) {
    JavaRDD<GenericRecord> recordsFromOneFile = SparkBasedReader
        .readAvro(sparkSession, schemaStr, Arrays.asList(filePath),
            structName, nameSpace);
    return recordsFromOneFile.count();
  }

}
