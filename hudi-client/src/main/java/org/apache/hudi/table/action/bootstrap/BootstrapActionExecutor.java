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

package org.apache.hudi.table.action.bootstrap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.MetadataBootstrapKeyGenerator;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload;
import org.apache.hudi.client.bootstrap.BootstrapSourceSchemaProvider;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.bootstrap.FullBootstrapInputProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseCommitActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BootstrapActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T, HoodieBootstrapWriteMetadata> {

  private static final Logger LOG = LogManager.getLogger(BootstrapActionExecutor.class);
  private String bootstrapSchema = null;

  public BootstrapActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config, HoodieTable<?> table) {
    super(jsc, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withAutoCommit(true).withWriteStatusClass(BootstrapWriteStatus.class)
        .withBulkInsertParallelism(config.getBootstrapParallelism())
        .build(), table, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, WriteOperationType.BOOTSTRAP);
  }

  private void checkArguments() {
    ValidationUtils.checkArgument(config.getBootstrapSourceBasePath() != null,
        "Ensure Bootstrap Source Path is set");
    ValidationUtils.checkArgument(config.getBootstrapModeSelectorClass() != null,
        "Ensure Bootstrap Partition Selector is set");
    ValidationUtils.checkArgument(config.getBootstrapKeyGeneratorClass() != null,
        "Ensure bootstrap key generator class is set");
  }

  @Override
  public HoodieBootstrapWriteMetadata execute() {
    checkArguments();
    try {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      Option<HoodieInstant> completetedInstant =
          metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      ValidationUtils.checkArgument(!completetedInstant.isPresent(),
          "Active Timeline is expected to be empty for bootstrapped to be performed. "
              + "If you want to re-bootstrap, please rollback bootstrap first !!");
      Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> partitionSelections =
          listAndProcessSourcePartitions(metaClient);

      // First run metadata bootstrap which will implicitly commit
      HoodieWriteMetadata metadataResult = metadataBootstrap(partitionSelections.get(
          BootstrapMode.METADATA_ONLY_BOOTSTRAP));
      // if there are full bootstrap to be performed, perform that too
      HoodieWriteMetadata fullBootstrapResult =
          fullBootstrap(partitionSelections.get(BootstrapMode.FULL_BOOTSTRAP));
      return new HoodieBootstrapWriteMetadata(metadataResult, fullBootstrapResult);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  protected String getSchemaToStoreInCommit() {
    return bootstrapSchema;
  }

  /**
   * Perform Metadata Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected HoodieWriteMetadata metadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList || partitionFilesList.isEmpty()) {
      return null;
    }

    HoodieTableMetaClient metaClient = table.getMetaClient();
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(State.REQUESTED, metaClient.getCommitActionType(),
            HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        metaClient.getCommitActionType(), HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS), Option.empty());

    JavaRDD<BootstrapWriteStatus> bootstrapWriteStatuses = runMetadataBootstrap(partitionFilesList);

    HoodieWriteMetadata result = new HoodieWriteMetadata();
    updateIndexAndCommitIfNeeded(bootstrapWriteStatuses.map(w -> w), result);
    return result;
  }

  @Override
  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result) {
    // Perform bootstrap index write and then commit. Make sure both record-key and bootstrap-index
    // is all done in a single job DAG.
    Map<String, List<Pair<BootstrapSourceFileMapping, HoodieWriteStat>>> bootstrapSourceAndStats =
        result.getWriteStatuses().collect().stream()
            .map(w -> {
              BootstrapWriteStatus ws = (BootstrapWriteStatus) w;
              return Pair.of(ws.getBootstrapSourceFileMapping(), ws.getStat());
            }).collect(Collectors.groupingBy(w -> w.getKey().getHudiPartitionPath()));
    HoodieTableMetaClient metaClient = table.getMetaClient();
    try (BootstrapIndex.IndexWriter indexWriter = BootstrapIndex.getBootstrapIndex(metaClient)
        .createWriter(config.getBootstrapSourceBasePath())) {
      LOG.info("Starting to write bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
      indexWriter.begin();
      bootstrapSourceAndStats.forEach((key, value) -> indexWriter.appendNextPartition(key,
          value.stream().map(w -> w.getKey()).collect(Collectors.toList())));
      indexWriter.finish();
      LOG.info("Finished writing bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
    }

    super.commit(extraMetadata, result, bootstrapSourceAndStats.values().stream()
        .flatMap(f -> f.stream().map(Pair::getValue)).collect(Collectors.toList()));
    LOG.info("Done Committing metadata bootstrap !!");
  }

  /**
   * Perform Metadata Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected HoodieWriteMetadata fullBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList || partitionFilesList.isEmpty()) {
      return null;
    }
    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());
    FullBootstrapInputProvider inputProvider =
        (FullBootstrapInputProvider) ReflectionUtils.loadClass(config.getFullBootstrapInputProvider(),
            properties, jsc);
    JavaRDD<HoodieRecord> inputRecordsRDD =
        inputProvider.generateInputRecordRDD("bootstrap_source", config.getBootstrapSourceBasePath(),
            partitionFilesList);
    // Start Full Bootstrap
    final HoodieInstant requested = new HoodieInstant(State.REQUESTED, table.getMetaClient().getCommitActionType(),
        HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS);
    table.getActiveTimeline().createNewInstant(requested);
    // Setup correct schema and run bulk insert.
    return new BulkInsertCommitActionExecutor(jsc, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withSchema(bootstrapSchema).build(), table, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
        inputRecordsRDD, Option.empty()).execute();
  }

  private BootstrapWriteStatus handleMetadataBootstrap(String srcPartitionPath, String partitionPath,
      HoodieFileStatus srcFileStatus, MetadataBootstrapKeyGenerator keyGenerator) {

    Path sourceFilePath = FileStatusUtils.toPath(srcFileStatus.getPath());
    HoodieBootstrapHandle bootstrapHandle = new HoodieBootstrapHandle(config, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
        table, partitionPath, FSUtils.createNewFileIdPfx(), table.getSparkTaskContextSupplier());
    Schema avroSchema = null;
    try {
      ParquetMetadata readFooter = ParquetFileReader.readFooter(table.getHadoopConf(), sourceFilePath,
          ParquetMetadataConverter.NO_FILTER);
      MessageType parquetSchema = readFooter.getFileMetaData().getSchema();
      avroSchema = new AvroSchemaConverter().convert(parquetSchema);
      Schema recordKeySchema = HoodieAvroUtils.generateProjectionSchema(avroSchema,
          keyGenerator.getTopLevelRecordKeyFields());
      LOG.info("Schema to be used for reading record Keys :" + recordKeySchema);
      AvroReadSupport.setAvroReadSchema(table.getHadoopConf(), recordKeySchema);
      AvroReadSupport.setRequestedProjection(table.getHadoopConf(), recordKeySchema);

      BoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
          AvroParquetReader.<IndexedRecord>builder(sourceFilePath).withConf(table.getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void>(config,
            new ParquetReaderIterator(reader), new BootstrapRecordWriter(bootstrapHandle), inp -> {
          String recKey = keyGenerator.getRecordKey(inp);
          GenericRecord gr = new GenericData.Record(HoodieAvroUtils.RECORD_KEY_SCHEMA);
          gr.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recKey);
          BootstrapRecordPayload payload = new BootstrapRecordPayload(gr);
          HoodieRecord rec = new HoodieRecord(new HoodieKey(recKey, partitionPath), payload);
          return rec;
        });
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        bootstrapHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
    BootstrapWriteStatus writeStatus = (BootstrapWriteStatus)bootstrapHandle.getWriteStatus();
    BootstrapSourceFileMapping bootstrapSourceFileMapping = new BootstrapSourceFileMapping(
        config.getBootstrapSourceBasePath(), srcPartitionPath, partitionPath,
        srcFileStatus, writeStatus.getFileId());
    writeStatus.setBootstrapSourceFileMapping(bootstrapSourceFileMapping);
    return writeStatus;
  }

  /**
   * Return Bootstrap Mode selections for partitions listed and figure out bootstrap Schema.
   * @param metaClient Hoodie Table Meta Client.
   * @return
   * @throws IOException
   */
  private Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> listAndProcessSourcePartitions(
      HoodieTableMetaClient metaClient) throws IOException {
    //TODO: Added HoodieFilter for manually testing bootstrap from source hudi table. Needs to be reverted.
    final PathFilter hoodieFilter = new HoodieROTablePathFilter();
    List<Pair<String, List<HoodieFileStatus>>> folders =
        FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(),
            config.getBootstrapSourceBasePath(), new PathFilter() {
              @Override
              public boolean accept(Path path) {
                // TODO: Needs to be abstracted out when supporting different formats
                // TODO: Remove hoodieFilter
                return path.getName().endsWith(".parquet") && hoodieFilter.accept(path);
              }
            });

    LOG.info("Fetching Bootstrap Schema !!");
    BootstrapSourceSchemaProvider sourceSchemaProvider = new BootstrapSourceSchemaProvider(config);
    bootstrapSchema = sourceSchemaProvider.getBootstrapSchema(jsc, folders).toString();
    LOG.info("Bootstrap Schema :" + bootstrapSchema);

    BootstrapModeSelector selector =
        (BootstrapModeSelector) ReflectionUtils.loadClass(config.getBootstrapModeSelectorClass(), config);

    Map<BootstrapMode, List<String>> result = selector.select(folders);
    Map<String, List<HoodieFileStatus>> partitionToFiles = folders.stream().collect(
        Collectors.toMap(Pair::getKey, Pair::getValue));

    // Ensure all partitions are accounted for
    ValidationUtils.checkArgument(partitionToFiles.keySet().equals(
        result.values().stream().flatMap(Collection::stream).collect(Collectors.toSet())));

    return result.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue().stream()
        .map(p -> Pair.of(p, partitionToFiles.get(p))).collect(Collectors.toList())))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private JavaRDD<BootstrapWriteStatus> runMetadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    if (null == partitions || partitions.isEmpty()) {
      return jsc.emptyRDD();
    }

    MetadataBootstrapKeyGenerator keyGenerator = new MetadataBootstrapKeyGenerator(config);

    return jsc.parallelize(partitions.stream()
        .map(p -> Pair.of(p.getKey(), Pair.of(keyGenerator.getTranslatedPath(p.getKey()), p.getValue())))
        .flatMap(p -> p.getValue().getValue().stream()
            .map(f -> Pair.of(p.getLeft(), Pair.of(p.getRight().getLeft(), f))))
            .collect(Collectors.toList()),
        config.getBootstrapParallelism())
        .map(partitionFsPair -> {
          return handleMetadataBootstrap(partitionFsPair.getLeft(), partitionFsPair.getRight().getLeft(),
              partitionFsPair.getRight().getRight(),keyGenerator);
        });
  }

  //TODO: Once we decouple commit protocol, we should change the class hierarchy to avoid doing this.
  @Override
  protected Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    return null;
  }

  @Override
  protected Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return null;
  }

  @Override
  protected Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    return null;
  }

  @Override
  protected Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    return null;
  }



  /**
   * Given a path is a part of - Hoodie table = accepts ONLY the latest version of each path - Non-Hoodie table = then
   * always accept
   * <p>
   * We can set this filter, on a query engine's Hadoop Config and if it respects path filters, then you should be able to
   * query both hoodie and non-hoodie tables as you would normally do.
   * <p>
   * hadoopConf.setClass("mapreduce.input.pathFilter.class", org.apache.hudi.hadoop .HoodieROTablePathFilter.class,
   * org.apache.hadoop.fs.PathFilter.class)
   */
  public static class HoodieROTablePathFilter implements PathFilter, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(HoodieROTablePathFilter.class);

    /**
     * Its quite common, to have all files from a given partition path be passed into accept(), cache the check for hoodie
     * metadata for known partition paths and the latest versions of files.
     */
    private HashMap<String, HashSet<Path>> hoodiePathCache;

    /**
     * Paths that are known to be non-hoodie tables.
     */
    private HashSet<String> nonHoodiePathCache;

    /**
     * Hadoop configurations for the FileSystem.
     */
    private SerializableConfiguration conf;

    private transient FileSystem fs;

    public HoodieROTablePathFilter() {
      this(new Configuration());
    }

    public HoodieROTablePathFilter(Configuration conf) {
      this.hoodiePathCache = new HashMap<>();
      this.nonHoodiePathCache = new HashSet<>();
      this.conf = new SerializableConfiguration(conf);
    }

    /**
     * Obtain the path, two levels from provided path.
     *
     * @return said path if available, null otherwise
     */
    private Path safeGetParentsParent(Path path) {
      if (path.getParent() != null && path.getParent().getParent() != null
          && path.getParent().getParent().getParent() != null) {
        return path.getParent().getParent().getParent();
      }
      return null;
    }

    @Override
    public boolean accept(Path path) {

      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking acceptance for path " + path);
      }
      Path folder = null;
      try {
        if (fs == null) {
          fs = path.getFileSystem(conf.get());
        }

        // Assumes path is a file
        folder = path.getParent(); // get the immediate parent.
        // Try to use the caches.
        if (nonHoodiePathCache.contains(folder.toString())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Accepting non-hoodie path from cache: " + path);
          }
          return true;
        }

        if (hoodiePathCache.containsKey(folder.toString())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%s Hoodie path checked against cache, accept => %s \n", path,
                hoodiePathCache.get(folder.toString()).contains(path)));
          }
          return hoodiePathCache.get(folder.toString()).contains(path);
        }

        // Skip all files that are descendants of .hoodie in its path.
        String filePath = path.toString();
        if (filePath.contains("/" + HoodieTableMetaClient.METAFOLDER_NAME + "/")
            || filePath.endsWith("/" + HoodieTableMetaClient.METAFOLDER_NAME)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Skipping Hoodie Metadata file  %s \n", filePath));
          }
          return false;
        }

        // Perform actual checking.
        Path baseDir;
        if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
          HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
          metadata.readFromFS();
          baseDir = getNthParent(folder, metadata.getPartitionDepth());
        } else {
          baseDir = safeGetParentsParent(folder);
        }

        if (baseDir != null) {
          try {
            HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), baseDir.toString());
            HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
                metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(), fs.listStatus(folder));
            List<HoodieBaseFile> latestFiles = fsView.getLatestBaseFiles().collect(Collectors.toList());
            // populate the cache
            if (!hoodiePathCache.containsKey(folder.toString())) {
              hoodiePathCache.put(folder.toString(), new HashSet<>());
            }
            LOG.info("Based on hoodie metadata from base path: " + baseDir.toString() + ", caching " + latestFiles.size()
                + " files under " + folder);
            for (HoodieBaseFile lfile : latestFiles) {
              hoodiePathCache.get(folder.toString()).add(new Path(lfile.getPath()));
            }

            // accept the path, if its among the latest files.
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format("%s checked after cache population, accept => %s \n", path,
                  hoodiePathCache.get(folder.toString()).contains(path)));
            }
            return hoodiePathCache.get(folder.toString()).contains(path);
          } catch (TableNotFoundException e) {
            // Non-hoodie path, accept it.
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format("(1) Caching non-hoodie path under %s \n", folder.toString()));
            }
            nonHoodiePathCache.add(folder.toString());
            return true;
          }
        } else {
          // files is at < 3 level depth in FS tree, can't be hoodie dataset
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("(2) Caching non-hoodie path under %s \n", folder.toString()));
          }
          nonHoodiePathCache.add(folder.toString());
          return true;
        }
      } catch (Exception e) {
        String msg = "Error checking path :" + path + ", under folder: " + folder;
        LOG.error(msg, e);
        throw new HoodieException(msg, e);
      }
    }

    public static Path getNthParent(Path path, int n) {
      Path parent = path;
      for (int i = 0; i < n; i++) {
        parent = parent.getParent();
      }
      return parent;
    }
  }
}
