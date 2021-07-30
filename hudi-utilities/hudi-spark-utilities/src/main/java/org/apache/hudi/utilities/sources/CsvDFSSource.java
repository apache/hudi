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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Reads data from CSV files on DFS as the data source.
 *
 * Internally, we use Spark to read CSV files thus any limitation of Spark CSV also applies here
 * (e.g., limited support for nested schema).
 *
 * You can set the CSV-specific configs in the format of hoodie.deltastreamer.csv.*
 * that are Spark compatible to deal with CSV files in Hudi.  The supported options are:
 *
 *       "sep", "encoding", "quote", "escape", "charToEscapeQuoteEscaping", "comment",
 *       "header", "enforceSchema", "inferSchema", "samplingRatio", "ignoreLeadingWhiteSpace",
 *       "ignoreTrailingWhiteSpace", "nullValue", "emptyValue", "nanValue", "positiveInf",
 *       "negativeInf", "dateFormat", "timestampFormat", "maxColumns", "maxCharsPerColumn",
 *       "mode", "columnNameOfCorruptRecord", "multiLine"
 *
 * Detailed information of these CSV options can be found at:
 * https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-scala.collection.Seq-
 *
 * If the source Avro schema is provided through the {@link org.apache.hudi.utilities.schema.FilebasedSchemaProvider}
 * using "hoodie.deltastreamer.schemaprovider.source.schema.file" config, the schema is
 * passed to the CSV reader without inferring the schema from the CSV file.
 */
public class CsvDFSSource extends RowSource {

  private static final long serialVersionUID = 1L;
  // CsvSource config prefix
  protected static final String CSV_SRC_CONFIG_PREFIX = "hoodie.deltastreamer.csv.";
  // CSV-specific configurations to pass in from Hudi to Spark
  protected static final List<String> CSV_CONFIG_KEYS = Arrays.asList(
      "sep", "encoding", "quote", "escape", "charToEscapeQuoteEscaping", "comment",
      "header", "enforceSchema", "inferSchema", "samplingRatio", "ignoreLeadingWhiteSpace",
      "ignoreTrailingWhiteSpace", "nullValue", "emptyValue", "nanValue", "positiveInf",
      "negativeInf", "dateFormat", "timestampFormat", "maxColumns", "maxCharsPerColumn",
      "mode", "columnNameOfCorruptRecord", "multiLine"
  );

  private final transient DFSPathSelector pathSelector;
  private final StructType sourceSchema;

  public CsvDFSSource(TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = DFSPathSelector.createSourceSelector(props, sparkContext.hadoopConfiguration());
    if (schemaProvider != null) {
      sourceSchema = (StructType) SchemaConverters.toSqlType(schemaProvider.getSourceSchema())
          .dataType();
    } else {
      sourceSchema = null;
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr,
      long sourceLimit) {
    Pair<Option<String>, String> selPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCkptStr, sourceLimit);
    return Pair.of(fromFiles(
        selPathsWithMaxModificationTime.getLeft()), selPathsWithMaxModificationTime.getRight());
  }

  /**
   * Reads the CSV files and parsed the lines into {@link Dataset} of {@link Row}.
   *
   * @param pathStr  The list of file paths, separated by ','.
   * @return  {@link Dataset} of {@link Row} containing the records.
   */
  private Option<Dataset<Row>> fromFiles(Option<String> pathStr) {
    if (pathStr.isPresent()) {
      DataFrameReader dataFrameReader = sparkSession.read().format("csv");
      CSV_CONFIG_KEYS.forEach(optionKey -> {
        String configPropName = CSV_SRC_CONFIG_PREFIX + optionKey;
        String value  = props.getString(configPropName, null);
        // Pass down the Hudi CSV configs to Spark DataFrameReader
        if (value != null) {
          dataFrameReader.option(optionKey, value);
        }
      });
      if (sourceSchema != null) {
        // Source schema is specified, pass it to the reader
        dataFrameReader.schema(sourceSchema);
      }
      dataFrameReader.option("inferSchema", Boolean.toString(sourceSchema == null));

      return Option.of(dataFrameReader.load(pathStr.get().split(",")));
    } else {
      return Option.empty();
    }
  }
}
