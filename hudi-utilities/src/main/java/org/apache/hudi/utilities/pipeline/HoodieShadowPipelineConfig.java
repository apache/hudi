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

package org.apache.hudi.utilities.pipeline;

import org.apache.hudi.client.utils.OperationConverter;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The command line arguments which are supported.
 */
public class HoodieShadowPipelineConfig implements Serializable {

  @Parameter(names = {"--ignore-missing-commits"}, description = "Ignore missing commits during sync", arity = 1)
  public boolean ignoreMissingCommits;

  @Parameter(names = {"--src-path"}, description = "Base path of the production dataset to shadow.", required = true)
  public String srcPath;

  @Parameter(names = {"--dest-path"}, description = "Path to use for creating the shadow dataset.", required = true)
  public String destPath;

  @Parameter(names = {"--dataset-name"}, description = "Table name to use. A suffix of '_shadow' will be added.", required = true)
  public String datasetName;

  @Parameter(names = {"--properties-filepath"}, description = "Path to a file with HUDI properties")
  public String propsFilePath;

  @Parameter(names = {"--runtime-props"}, description = "Runtime properties, the way to use this is by passing value "
      + "as key1=key2;key1=key2 etc", listConverter = StringToListParameterConverter.class)
  public List<String> props = new ArrayList<>();

  @Parameter(names = {"--reuse-hoodie-properties-from-src"}, description = "Instead of setting precombine key, "
      + "record_key and other table properties, this property will help copy the table properties from src to destination.", arity =  1)
  public boolean reuseHoodiePropertiesFileFromSrc = true;

  @Parameter(names = {"--start-partition"}, description = "Starting partition to clone (default=all partitions)")
  public String startPartition = "";

  @Parameter(names = {"--end-partition"}, description = "End partition to clone (default=all partitions)")
  public String endPartition = "";

  @Parameter(names = {"--selected-partitions"}, description = "Comma-separated list of specific partitions to clone (default=all partitions)",
      converter = StringToSetConverter.class)
  public Set<String> selectedPartitions = Collections.emptySet();

  @Parameter(names = {"--max-files-per-partition"}, description = "Maximum no. of files to copy per partition")
  public String maxFilesPerPartition = "";

  @Parameter(names = {"--partition-columns"}, description = "Comma separated source dataset columns which contains the partition path")
  public String partitionColumns = "datestr";

  @Parameter(names = {"--recordkey-column"}, description = "Source dataset column which contains the record key")
  public String recordKeyColumn = "_row_key";

  @Parameter(names = {"--source-instant-time"}, description = "Used while initializing destination dataset."
      + "It is the value upto which the data is copied.")
  public String sourceInstantTime = "";

  @Parameter(names = {"--instants-per-fetch"}, description = "No, of instants to fetch from source table.")
  public String instantsPerFetch = "1";

  @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
      + " to break ties between records with same key in input data. Default: '_row_key'")
  public String sourceOrderingField = "_row_key";

  @Parameter(names = {"--write-meta-fields"}, description = "Write HUDI meta fields like _hoodie_commit_time", arity = 1)
  public boolean writeMetaFields = true;

  @Parameter(names = {"--enable-hive-sync"}, description = "Sync to Hive in the 'test' database.", arity = 1)
  public boolean enableHiveSync = false;

  @Parameter(names = {"--use-source-commit-during-initialization"}, description = "During initialization of "
      + "destination datasets, when this config is enabled, source table's last commit is used instead of recreating "
      + "a new commit in destination table. Used mainly to avoid OOM issues when generating the commit during "
      + "initialization from larger datasets .", arity = 1)
  public boolean useSourceCommitDuringInitialization = false;

  @Parameter(names = {"--use-source-timeline-during-initialization"}, description = "During initialization of "
      + "destination datasets, when this config is enabled, source table's entire timeline is copied. "
      + "This config along with bootstrapWithLatestBaseFiles config are used to run time travel queries.", arity = 1)
  public boolean useSourceTimelineDuringInitialization = false;

  @Parameter(names = {"--bootstrap-with-latest-base-files"}, description = "While bootstraping destination dataset it "
      + "checks if latest base files need to be copied or all the base files need to be copied. This config combined with "
      + "useSourceTimelineDuringInitialization variable can be used to run time travel queries.", arity = 1)
  public Boolean bootstrapWithLatestBaseFiles = true;

  @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode fetching data"
      + " from source and write it to target table", arity = 1)
  public Boolean continuousMode = false;

  @Parameter(names = {"--max-commits-to-sync"}, description = "Maximum number of commits to sync before exiting. This can be used with --continuous mode to "
      + " eventually exist the delta streamer after syncing a certain number of commits", arity = 1)
  public int maxCommitsToSync = Integer.MAX_VALUE;

  @Parameter(names = {"--sleep-time-between-runs"}, description = "Number of minutes to sleep between each attempt of DeltaStreamer sync in continous mode.")
  public long sleepTimeBetweenRunsMins = 5;

  @Parameter(names = {"--hive-database"}, description = "Name of the Hive database to use")
  public String hiveDatabase = "huditmp";

  @Parameter(names = {"--hive-table"}, description = "Name of the Hive table to sync")
  public String hiveTable;

  @Parameter(names = {"--dest-table-type"}, description = "Destination table type either COW or MOR. COW is default value")
  public String destTableType = HoodieTableType.COPY_ON_WRITE.name();

  @Parameter(names = {"--dest-payload-class"}, description = "Destination table's payload class. Default is OverwriteWithLatestAvroPayload")
  public String destPayloadClassName = OverwriteWithLatestAvroPayload.class.getName();

  @Parameter(names = {"--assume-date-partitioning"}, description = "Is the dataset date partitioned?", arity = 1)
  public boolean assumeDatePartitioning = true;

  @Parameter(names = {"--partition-value-extractor-class"},
      description = "Partition value extractor class that extends PartitionValueExtractor")
  public String partitionValueExtractorClass = SlashEncodedDayPartitionValueExtractor.class.getName();

  @Parameter(names = {"--metric-prefix"}, description = "Metrics prefix to use. The '{USER}' placeholder, if present, "
      + "is replaced with the value of --userid at runtime.")
  public String metricPrefix = "hoodie.shadow.pipeline.{USER}";

  @Parameter(names = {"--zookeeper-url"}, description = "Zookeeper connection URL (host:port[,host:port...]) used by the "
      + "Zookeeper-based lock provider that guards the destination table during a run. Leave empty to source the quorum "
      + "from the supplied Hudi properties instead.")
  public String zookeeperUrl = "";

  @Parameter(names = {"--deduplicate"}, description = "De-duplicate input data. Applicable only for inserts.", arity = 1)
  public Boolean deduplicate = false;

  @Parameter(names = {"--operation"}, description = "HUDIStreamer operation type. Takes one of these values : UPSERT (default), INSERT, "
      + "BULK_INSERT, INSERT_OVERWRITE, INSERT_OVERWRITE_TABLE, DELETE_PARTITION",
      converter = OperationConverter.class)
  public WriteOperationType operation = WriteOperationType.UPSERT;

  @Parameter(names = {"--userid"}, description = "Userid (USER_LDAP_UID)", required = true)
  public String userid;

  @Parameter(names = {"--base-file-format", "bff"}, description = "Base file format of the dataset", required = false)
  public String baseFileFormat = "PARQUET";

  @Parameter(names = {"--delete-dest-path"}, description = "Delete the destination path if it exists before starting", arity = 1)
  public Boolean deleteDestPath = false;

  @Parameter(names = {"--validate-before"}, description = "Validate destination dataset before starting sync", arity = 1)
  public Boolean validateBefore = false;

  @Parameter(names = {"--validate-after"}, description = "Validate destination dataset after sync is complete", arity = 1)
  public Boolean validateAfter = false;

  @Parameter(names = {"--schemaprovider-class"}, description = "Class used to derive the schema for the dataset")
  public String schemaProviderClassName;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  @Parameter(names = {"--key-generator"}, description = "extract a key out of incoming records")
  public String keyGenerator = "org.apache.hudi.keygen.SimpleKeyGenerator";

  @Parameter(names = {"--enable-deltastreamer-checkpoint"}, description = "When enabled, it automatically sets"
      + " deltastreamer.checkpoint.key to latest commit inside extraMetadata map", arity = 1)
  public Boolean enableDeltastreamerCheckpoint = false;

  @Parameter(names = {"--create-partition-metafile-without-suffix"}, description = "When creating .hoodie_partition_metadata"
      + " metafile in each destination partition folder, do not add the suffix (such as .parquet). This is needed so that"
      + " downstream users can continue to assume that all HUDI partitions must have"
      + " .hoodie_partition_metadata file", arity = 1)
  public Boolean createPartitionMetafileWithoutSuffix = true;

  @Parameter(names = {"--allow-duplicates-in-record-index"}, description = "Allow duplicate keys in the metadata table's"
      + " record-index HFiles by setting hoodie.hfile.writer.allow.duplicates=true on the metadata write config."
      + " Useful when bootstrapping shadow datasets whose source RLI contains duplicates that would otherwise abort"
      + " the HFile writer.", arity = 1)
  public Boolean allowDuplicatesInRecordIndex = false;

  @Override
  public String toString() {
    return "HoodieShadowPipelineConfig{"
        + "ignoreMissingCommits=" + ignoreMissingCommits
        + ", srcPath='" + srcPath + '\''
        + ", destPath='" + destPath + '\''
        + ", datasetName='" + datasetName + '\''
        + ", propsFilePath='" + propsFilePath + '\''
        + ", startPartition='" + startPartition + '\''
        + ", endPartition='" + endPartition + '\''
        + ", selectedPartition='" + selectedPartitions + '\''
        + ", maxFilesPerPartition='" + maxFilesPerPartition + '\''
        + ", partitionColumns='" + partitionColumns + '\''
        + ", recordKeyColumn='" + recordKeyColumn + '\''
        + ", sourceInstantTime='" + sourceInstantTime + '\''
        + ", instantsPerFetch='" + instantsPerFetch + '\''
        + ", sourceOrderingField='" + sourceOrderingField + '\''
        + ", writeMetaFields=" + writeMetaFields
        + ", enableHiveSync=" + enableHiveSync
        + ", useSourceCommitDuringInitialization=" + useSourceCommitDuringInitialization
        + ", useSourceTimelineDuringInitialization=" + useSourceTimelineDuringInitialization
        + ", bootstrapWithLatestBaseFiles=" + bootstrapWithLatestBaseFiles
        + ", continuousMode=" + continuousMode
        + ", maxCommitsToSync=" + maxCommitsToSync
        + ", hiveDatabase='" + hiveDatabase + '\''
        + ", hiveTable='" + hiveTable + '\''
        + ", destTableType='" + destTableType + '\''
        + ", assumeDatePartitioning=" + assumeDatePartitioning
        + ", metricPrefix='" + metricPrefix + '\''
        + ", zookeeperUrl='" + zookeeperUrl + '\''
        + ", deduplicate=" + deduplicate
        + ", operation='" + operation + '\''
        + ", userid='" + userid + '\''
        + ", help=" + help
        + ", keyGenerator='" + keyGenerator + '\''
        + ", enableDeltastreamerCheckpoint=" + enableDeltastreamerCheckpoint
        + ", create-partition-metafile-without-suffix=" + createPartitionMetafileWithoutSuffix
        + ", allowDuplicatesInRecordIndex=" + allowDuplicatesInRecordIndex
        + '}';
  }
}
