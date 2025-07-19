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

package org.apache.hudi.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Utility class for performing strict schema validation and merging of Parquet files.
 * This class ensures that all input files have identical schemas before merging.
 */
public class HoodieParquetStrictMerge {
  private Configuration conf;

  public HoodieParquetStrictMerge() {
    conf = new Configuration();
  }

  public HoodieParquetStrictMerge(Configuration configuration) {
    conf = configuration;
  }

  /**
   * Merges multiple Parquet files into a single output file.
   * All input files must have identical schemas, otherwise an exception is thrown.
   *
   * @param inputFiles List of input Parquet file paths
   * @param outputFile Output file path
   * @throws IOException if I/O error occurs
   * @throws IllegalArgumentException if schemas don't match or no input files provided
   */
  public void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException {
    // Check and get schema
    MessageType schema = checkAndGetSchema(inputFiles);

    // Merge data
    ParquetFileWriter writer = new ParquetFileWriter(conf,
            schema, outputFile, ParquetFileWriter.Mode.CREATE);
    writer.start();
    for (Path input: inputFiles) {
      writer.appendFile(HadoopInputFile.fromPath(input, conf));
    }

    //TODO: If there is bloomfilter, we need to merge it using union instead of just adding them together
    writer.end(ParquetFileWriter.mergeMetadataFiles(inputFiles, conf).getFileMetaData().getKeyValueMetaData());
  }

  /**
   * Validates that all input files have the same schema and returns that schema.
   *
   * @param inputFiles List of input Parquet file paths to validate
   * @return The common schema shared by all input files
   * @throws IOException if I/O error occurs
   * @throws IllegalArgumentException if schemas don't match or no input files provided
   */
  private MessageType checkAndGetSchema(List<Path> inputFiles) throws IOException {
    if (inputFiles == null || inputFiles.size() == 0) {
      throw new IllegalArgumentException("No input files provided");
    }

    // 1) Read first file schema
    MessageType firstSchema = ParquetFileReader.readFooter(conf, inputFiles.get(0),
        ParquetMetadataConverter.NO_FILTER).getFileMetaData().getSchema();

    // 2) For all the remaining files, loop all of them and read each of them's schema
    // If any schema is different, throw exception
    for (int i = 1; i < inputFiles.size(); i++) {
      MessageType currentSchema = ParquetFileReader.readFooter(conf, inputFiles.get(i),
          ParquetMetadataConverter.NO_FILTER).getFileMetaData().getSchema();

      if (!firstSchema.equals(currentSchema)) {
        throw new IllegalArgumentException("Schema mismatch: file " + inputFiles.get(0)
            + " has schema " + firstSchema + " but file " + inputFiles.get(i)
            + " has schema " + currentSchema);
      }
    }

    // 3) Return the first schema
    return firstSchema;
  }
}