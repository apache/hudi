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

package com.uber.hoodie.configs;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.ArrayList;
import java.util.List;

public class HoodieDeltaStreamerJobConfig extends AbstractJobConfig {

  public enum Operation {
    UPSERT, INSERT, BULK_INSERT
  }

  private class OperationConvertor implements IStringConverter<Operation> {

    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  @Parameter(names = {"--target-base-path"}, description = "base path for the target hoodie dataset. "
      + "(Will be created if did not exist first time around. If exists, expected to be a hoodie dataset)",
      required = true)
  public String targetBasePath;

  // TODO: How to obtain hive configs to register?
  @Parameter(names = {"--target-table"}, description = "name of the target table in Hive", required = true)
  public String targetTableName;

  @Parameter(names = {"--storage-type"}, description = "Type of Storage. "
      + "COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
  public String storageType;

  @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
      + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
      + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
      + "to individual classes, for supported properties.")
  public String propsFilePath =
      "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--source-class"}, description = "Subclass of com.uber.hoodie.utilities.sources to read data. "
      + "Built-in options: com.uber.hoodie.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
      + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
  public String sourceClassName = "com.uber.hoodie.utilities.sources.JsonDFSSource ";

  @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
      + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
  public String sourceOrderingField = "ts";

  @Parameter(names = {"--key-generator-class"}, description = "Subclass of com.uber.hoodie.KeyGenerator "
      + "to generate a HoodieKey from the given avro record. Built in: SimpleKeyGenerator (uses "
      + "provided field names as recordkey & partitionpath. Nested fields specified via dot notation, e.g: a.b.c)")
  public String keyGeneratorClass = "com.uber.hoodie.SimpleKeyGenerator";

  @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
      + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
  public String payloadClassName = "com.uber.hoodie.OverwriteWithLatestAvroPayload";

  @Parameter(names = {"--schemaprovider-class"}, description = "subclass of com.uber.hoodie.utilities.schema"
      + ".SchemaProvider to attach schemas to input & target table data, built in options: "
      + "com.uber.hoodie.utilities.schema.FilebasedSchemaProvider."
      + "Source (See com.uber.hoodie.utilities.sources.Source) implementation can implement their own SchemaProvider."
      + " For Sources that return Dataset<Row>, the schema is obtained implicitly. "
      + "However, this CLI option allows overriding the schemaprovider returned by Source.")
  public String schemaProviderClassName = null;

  @Parameter(names = {"--transformer-class"},
      description = "subclass of com.uber.hoodie.utilities.transform.Transformer"
          + ". Allows transforming raw source dataset to a target dataset (conforming to target schema) before writing."
          + " Default : Not set. E:g - com.uber.hoodie.utilities.transform.SqlQueryBasedTransformer (which allows"
          + "a SQL query templated to be passed as a transformation function)")
  public String transformerClassName = null;

  @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
      + "Default: No limit For e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
  public long sourceLimit = Long.MAX_VALUE;

  @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
      + "is purely new data/inserts to gain speed)",
      converter = OperationConvertor.class)
  public Operation operation = Operation.UPSERT;

  @Parameter(names = {"--filter-dupes"}, description = "Should duplicate records from source be dropped/filtered out"
      + "before insert/bulk-insert")
  public Boolean filterDupes = false;

  @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
  public Boolean enableHiveSync = false;

  @Parameter(names = {"--spark-master"}, description = "spark master to use.")
  public String sparkMaster = "local[2]";

  @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
  public Boolean commitOnErrors = false;
}
