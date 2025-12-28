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

package org.apache.hudi.hadoop.hive;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.HoodieParquetInputFormatBase;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.realtime.HoodieCombineRealtimeRecordReader;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.CombineHiveRecordReader;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShimsSecure;
import org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This is just a copy of the org.apache.hadoop.hive.ql.io.CombineHiveInputFormat from Hive 2.x Search for **MOD** to
 * see minor modifications to support custom inputformat in CombineHiveInputFormat. See
 * https://issues.apache.org/jira/browse/HIVE-9771
 * <p>
 * <p>
 * CombineHiveInputFormat is a parameterized InputFormat which looks at the path name and determine the correct
 * InputFormat for that path name from mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 * <p>
 * NOTE : This class is implemented to work with Hive 2.x +
 */
public class HoodieCombineHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends HiveInputFormat<K, V> {

  private static final String CLASS_NAME = HoodieCombineHiveInputFormat.class.getName();
  public static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  // max number of threads we can use to check non-combinable paths
  private static final int MAX_CHECK_NONCOMBINABLE_THREAD_NUM = 50;
  private static final int DEFAULT_NUM_PATH_PER_THREAD = 100;
  public static final String INTERNAL_SCHEMA_CACHE_KEY_PREFIX = "hudi.hive.internal.schema.cache.key.prefix";
  public static final String SCHEMA_CACHE_KEY_PREFIX = "hudi.hive.schema.cache.key.prefix";

  protected String getParquetInputFormatClassName() {
    return HoodieParquetInputFormat.class.getName();
  }

  protected String getParquetRealtimeInputFormatClassName() {
    return HoodieParquetRealtimeInputFormat.class.getName();
  }

  protected HoodieCombineFileInputFormatShim createInputFormatShim() {
    return new HoodieCombineHiveInputFormat.HoodieCombineFileInputFormatShim<>();
  }

  /**
   * Create Hive splits based on CombineFileSplit.
   */
  private InputSplit[] getCombineSplits(JobConf job, int numSplits, Map<Path, PartitionDesc> pathToPartitionInfo)
      throws IOException {
    init(job);
    Map<Path, ArrayList<String>> pathToAliases = mrwork.getPathToAliases();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = mrwork.getAliasToWork();
    /* MOD - Initialize a custom combine input format shim that will call listStatus on the custom inputFormat **/
    HoodieCombineHiveInputFormat.HoodieCombineFileInputFormatShim combine = createInputFormatShim();

    InputSplit[] splits;

    if (combine.getInputPathsShim(job).length == 0) {
      throw new IOException("No input paths specified in job");
    }
    List<InputSplit> result = new ArrayList<>();

    // combine splits only from same tables and same partitions. Do not combine splits from multiple
    // tables or multiple partitions.
    Path[] paths = StringInternUtils.internUriStringsInPathArray(combine.getInputPathsShim(job));

    List<Path> inpDirs = new ArrayList<>();
    List<Path> inpFiles = new ArrayList<>();
    Map<CombinePathInputFormat, CombineFilter> poolMap = new HashMap<>();
    Set<Path> poolSet = new HashSet<>();

    for (Path path : paths) {
      PartitionDesc part = getPartitionFromPath(pathToPartitionInfo, path,
          IOPrepareCache.get().allocatePartitionDescMap());
      TableDesc tableDesc = part.getTableDesc();
      if ((tableDesc != null) && tableDesc.isNonNative()) {
        return super.getSplits(job, numSplits);
      }

      // Use HiveInputFormat if any of the paths is not splittable
      Class<?> inputFormatClass = part.getInputFileFormatClass();
      String inputFormatClassName = inputFormatClass.getName();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      LOG.info("Input Format => " + inputFormatClass.getName());
      // **MOD** Set the hoodie filter in the combine
      if (inputFormatClass.getName().equals(getParquetInputFormatClassName())) {
        combine.setHoodieFilter(true);
      } else if (inputFormatClass.getName().equals(getParquetRealtimeInputFormatClassName())) {
        LOG.info("Setting hoodie filter and realtime input format");
        combine.setHoodieFilter(true);
        combine.setRealTime(true);
        if (job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "").isEmpty()) {
          List<String> partitions = new ArrayList<>(part.getPartSpec().keySet());
          if (!partitions.isEmpty()) {
            String partitionStr = String.join("/", partitions);
            LOG.info("Setting Partitions in jobConf - Partition Keys for Path : " + path + " is :" + partitionStr);
            job.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, partitionStr);
          } else {
            job.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
          }
        }
      }
      String deserializerClassName = null;
      try {
        deserializerClassName = part.getDeserializer(job).getClass().getName();
      } catch (Exception e) {
        // ignore
        LOG.error("Getting deserializer class name error ", e);
      }

      // don't combine if inputformat is a SymlinkTextInputFormat
      if (inputFormat instanceof SymlinkTextInputFormat) {
        splits = super.getSplits(job, numSplits);
        return splits;
      }

      Path filterPath = path;

      // Does a pool exist for this path already
      CombineFilter f;
      List<Operator<? extends OperatorDesc>> opList;

      if (!mrwork.isMapperCannotSpanPartns()) {
        // if mapper can span partitions, make sure a splits does not contain multiple
        // opList + inputFormatClassName + deserializerClassName combination
        // This is done using the Map of CombinePathInputFormat to PathFilter

        opList = HiveFileFormatUtils.doGetWorksFromPath(pathToAliases, aliasToWork, filterPath);
        CombinePathInputFormat combinePathInputFormat =
            new CombinePathInputFormat(opList, inputFormatClassName, deserializerClassName);
        f = poolMap.get(combinePathInputFormat);
        if (f == null) {
          f = new CombineFilter(filterPath);
          LOG.info("CombineHiveInputSplit creating pool for " + path + "; using filter path " + filterPath);
          combine.createPool(job, f);
          poolMap.put(combinePathInputFormat, f);
        } else {
          LOG.info("CombineHiveInputSplit: pool is already created for " + path + "; using filter path " + filterPath);
          f.addPath(filterPath);
        }
      } else {
        // In the case of tablesample, the input paths are pointing to files rather than directories.
        // We need to get the parent directory as the filtering path so that all files in the same
        // parent directory will be grouped into one pool but not files from different parent
        // directories. This guarantees that a split will combine all files in the same partition
        // but won't cross multiple partitions if the user has asked so.
        if (!path.getFileSystem(job).getFileStatus(path).isDirectory()) { // path is not directory
          filterPath = path.getParent();
          inpFiles.add(path);
          poolSet.add(filterPath);
        } else {
          inpDirs.add(path);
        }
      }
    }

    // Processing directories
    List<CombineFileSplit> iss = new ArrayList<>();
    if (!mrwork.isMapperCannotSpanPartns()) {
      // mapper can span partitions
      // combine into as few as one split, subject to the PathFilters set
      // using combine.createPool.
      iss = Arrays.asList(combine.getSplits(job, 1));
    } else {
      for (Path path : inpDirs) {
        processPaths(job, combine, iss, path);
      }

      if (inpFiles.size() > 0) {
        // Processing files
        for (Path filterPath : poolSet) {
          combine.createPool(job, new CombineFilter(filterPath));
        }
        processPaths(job, combine, iss, inpFiles.toArray(new Path[0]));
      }
    }

    if (mrwork.getNameToSplitSample() != null && !mrwork.getNameToSplitSample().isEmpty()) {
      iss = sampleSplits(iss);
    }

    for (CombineFileSplit is : iss) {
      final InputSplit csplit;
      if (combine.isRealTime) {
        if (is instanceof HoodieCombineRealtimeHiveSplit) {
          csplit = is;
        } else {
          csplit = new HoodieCombineRealtimeHiveSplit(job, is, pathToPartitionInfo);
        }
      } else {
        csplit = new CombineHiveInputSplit(job, is, pathToPartitionInfo);
      }
      result.add(csplit);
    }

    LOG.info("number of splits " + result.size());
    return result.toArray(new CombineHiveInputSplit[result.size()]);
  }

  /**
   * Gets all the path indices that should not be combined.
   */
  public Set<Integer> getNonCombinablePathIndices(JobConf job, Path[] paths, int numThreads)
      throws ExecutionException, InterruptedException {
    LOG.info("Total number of paths: " + paths.length + ", launching " + numThreads
        + " threads to check non-combinable ones.");
    int numPathPerThread = (int) Math.ceil((double) paths.length / numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Set<Integer>>> futureList = new ArrayList<>(numThreads);
    try {
      for (int i = 0; i < numThreads; i++) {
        int start = i * numPathPerThread;
        int length = i != numThreads - 1 ? numPathPerThread : paths.length - start;
        futureList.add(executor.submit(new CheckNonCombinablePathCallable(paths, start, length, job)));
      }
      Set<Integer> nonCombinablePathIndices = new HashSet<>();
      for (Future<Set<Integer>> future : futureList) {
        nonCombinablePathIndices.addAll(future.get());
      }
      return nonCombinablePathIndices;
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Create Hive splits based on CombineFileSplit.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.GET_SPLITS);
    init(job);

    List<InputSplit> result = new ArrayList<>();

    Path[] paths = getInputPaths(job);

    List<Path> nonCombinablePaths = new ArrayList<>(paths.length / 2);
    List<Path> combinablePaths = new ArrayList<>(paths.length / 2);

    int numThreads = Math.min(MAX_CHECK_NONCOMBINABLE_THREAD_NUM,
        (int) Math.ceil((double) paths.length / DEFAULT_NUM_PATH_PER_THREAD));

    // This check is necessary because for Spark branch, the result array from
    // getInputPaths() above could be empty, and therefore numThreads could be 0.
    // In that case, Executors.newFixedThreadPool will fail.
    if (numThreads > 0) {
      try {
        Set<Integer> nonCombinablePathIndices = getNonCombinablePathIndices(job, paths, numThreads);
        for (int i = 0; i < paths.length; i++) {
          if (nonCombinablePathIndices.contains(i)) {
            nonCombinablePaths.add(paths[i]);
          } else {
            combinablePaths.add(paths[i]);
          }
        }
      } catch (Exception e) {
        LOG.error("Error checking non-combinable path", e);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
        throw new IOException(e);
      }
    }

    // Store the previous value for the path specification
    String oldPaths = job.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);
    LOG.debug("The received input paths are: [{}] against the property {}", oldPaths,
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);

    // Process the normal splits
    if (nonCombinablePaths.size() > 0) {
      FileInputFormat.setInputPaths(job, nonCombinablePaths.toArray(new Path[0]));
      InputSplit[] splits = super.getSplits(job, numSplits);
      Collections.addAll(result, splits);
    }

    // Process the combine splits
    if (combinablePaths.size() > 0) {
      FileInputFormat.setInputPaths(job, combinablePaths.toArray(new Path[0]));
      Map<Path, PartitionDesc> pathToPartitionInfo = this.pathToPartitionInfo != null ? this.pathToPartitionInfo
          : Utilities.getMapWork(job).getPathToPartitionInfo();
      InputSplit[] splits = getCombineSplits(job, numSplits, pathToPartitionInfo);
      Collections.addAll(result, splits);
    }

    // Restore the old path information back
    // This is just to prevent incompatibilities with previous versions Hive
    // if some application depends on the original value being set.
    if (oldPaths != null) {
      job.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, oldPaths);
    }

    // clear work from ThreadLocal after splits generated in case of thread is reused in pool.
    Utilities.clearWorkMapForConf(job);

    // build internal schema for the query
    if (!result.isEmpty()) {
      ArrayList<String> uniqTablePaths = new ArrayList<>();
      Arrays.stream(paths).forEach(path -> {
        final HoodieStorage storage;
        try {
          FileSystem fs = path.getFileSystem(job);
          storage = HoodieStorageUtils.getStorage(
              HadoopFSUtils.convertToStoragePath(path), HadoopFSUtils.getStorageConf(fs.getConf()));
          Option<StoragePath> tablePath = TablePathUtils.getTablePath(storage, HadoopFSUtils.convertToStoragePath(path));
          if (tablePath.isPresent()) {
            uniqTablePaths.add(tablePath.get().toUri().toString());
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

      try {
        for (String path : uniqTablePaths) {
          HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(new HadoopStorageConfiguration(job)).build();
          TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
          String avroSchema = schemaUtil.getTableAvroSchema().toString();
          Option<InternalSchema> internalSchema = schemaUtil.getTableInternalSchemaFromCommitMetadata();
          if (internalSchema.isPresent()) {
            LOG.info("Set internal and avro schema cache with path: {}", path);
            job.set(SCHEMA_CACHE_KEY_PREFIX + "." + path, avroSchema);
            job.set(INTERNAL_SCHEMA_CACHE_KEY_PREFIX + "." + path, SerDeHelper.toJson(internalSchema.get()));
          } else {
            // always sets up the cache so that we can distinguish with the scenario where the cache was never set(e.g. in tests).
            job.set(SCHEMA_CACHE_KEY_PREFIX + "." + path, "");
            job.set(INTERNAL_SCHEMA_CACHE_KEY_PREFIX + "." + path, "");
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to set schema cache", e);
      }
    }

    LOG.info("Number of all splits {}", result.size());
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
    return result.toArray(new InputSplit[result.size()]);
  }

  private void processPaths(JobConf job, CombineFileInputFormatShim combine, List<CombineFileSplit> iss, Path... path)
      throws IOException {
    JobConf currJob = new JobConf(job);
    FileInputFormat.setInputPaths(currJob, path);
    iss.addAll(Arrays.asList(combine.getSplits(currJob, 1)));
  }

  /**
   * HiveFileFormatUtils.getPartitionDescFromPathRecursively is no longer available since Hive 3.
   * This method is to make it compatible with both Hive 2 and Hive 3.
   *
   * @param pathToPartitionInfo
   * @param dir
   * @param cacheMap
   * @return
   * @throws IOException
   */
  private static PartitionDesc getPartitionFromPath(Map<Path, PartitionDesc> pathToPartitionInfo, Path dir,
                                                    Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> cacheMap)
      throws IOException {
    Method method;
    try {
      Class<?> hiveUtilsClass = Class.forName("org.apache.hadoop.hive.ql.io.HiveFileFormatUtils");
      try {
        // HiveFileFormatUtils.getPartitionDescFromPathRecursively method only available in Hive 2.x
        method = hiveUtilsClass.getMethod("getPartitionDescFromPathRecursively", Map.class, Path.class, Map.class);
      } catch (NoSuchMethodException e) {
        // HiveFileFormatUtils.getFromPathRecursively method only available in Hive 3.x
        method = hiveUtilsClass.getMethod("getFromPathRecursively", Map.class, Path.class, Map.class);
      }
      return (PartitionDesc) method.invoke(null, pathToPartitionInfo, dir, cacheMap);
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }

  /**
   * MOD - Just added this for visibility.
   */
  Path[] getInputPaths(JobConf job) throws IOException {
    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      // on tez we're avoiding to duplicate the file info in FileInputFormat.
      if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        try {
          List<Path> paths = Utilities.getInputPathsTez(job, mrwork);
          dirs = paths.toArray(new Path[paths.size()]);
        } catch (Exception e) {
          throw new IOException("Could not create input files", e);
        }
      } else {
        throw new IOException("No input paths specified in job");
      }
    }
    return dirs;
  }

  /**
   * This function is used to sample inputs for clauses like "TABLESAMPLE(1 PERCENT)"
   * <p>
   * First, splits are grouped by alias they are for. If one split serves more than one alias or not for any sampled
   * alias, we just directly add it to returned list. Then we find a list of exclusive splits for every alias to be
   * sampled. For each alias, we start from position of seedNumber%totalNumber, and keep add splits until the total size
   * hits percentage.
   *
   * @return the sampled splits
   */
  private List<CombineFileSplit> sampleSplits(List<CombineFileSplit> splits) {
    HashMap<String, SplitSample> nameToSamples = mrwork.getNameToSplitSample();
    List<CombineFileSplit> retLists = new ArrayList<>();
    Map<String, ArrayList<CombineFileSplit>> aliasToSplitList = new HashMap<>();
    Map<Path, ArrayList<String>> pathToAliases = mrwork.getPathToAliases();
    Map<Path, ArrayList<String>> pathToAliasesNoScheme = removeScheme(pathToAliases);

    // Populate list of exclusive splits for every sampled alias
    //
    for (CombineFileSplit split : splits) {
      String alias = null;
      for (Path path : split.getPaths()) {
        boolean schemeless = path.toUri().getScheme() == null;
        List<String> l =
            HiveFileFormatUtils.doGetAliasesFromPath(schemeless ? pathToAliasesNoScheme : pathToAliases, path);
        // a path for a split unqualified the split from being sampled if:
        // 1. it serves more than one alias
        // 2. the alias it serves is not sampled
        // 3. it serves different alias than another path for the same split
        if (l.size() != 1 || !nameToSamples.containsKey(l.get(0)) || (alias != null && !Objects.equals(l.get(0), alias))) {
          alias = null;
          break;
        }
        alias = l.get(0);
      }

      if (alias != null) {
        // split exclusively serves alias, which needs to be sampled
        // add it to the split list of the alias.
        if (!aliasToSplitList.containsKey(alias)) {
          aliasToSplitList.put(alias, new ArrayList<>());
        }
        aliasToSplitList.get(alias).add(split);
      } else {
        // The split doesn't exclusively serve one alias
        retLists.add(split);
      }
    }

    // for every sampled alias, we figure out splits to be sampled and add
    // them to return list
    //
    for (Map.Entry<String, ArrayList<CombineFileSplit>> entry : aliasToSplitList.entrySet()) {
      ArrayList<CombineFileSplit> splitList = entry.getValue();
      long totalSize = 0;
      for (CombineFileSplit split : splitList) {
        totalSize += split.getLength();
      }

      SplitSample splitSample = nameToSamples.get(entry.getKey());

      long targetSize = splitSample.getTargetSize(totalSize);
      int startIndex = splitSample.getSeedNum() % splitList.size();
      long size = 0;
      for (int i = 0; i < splitList.size(); i++) {
        CombineFileSplit split = splitList.get((startIndex + i) % splitList.size());
        retLists.add(split);
        long splitgLength = split.getLength();
        if (size + splitgLength >= targetSize) {
          LOG.info("Sample alias " + entry.getValue() + " using " + (i + 1) + "splits");
          if (size + splitgLength > targetSize) {
            ((InputSplitShim) split).shrinkSplit(targetSize - size);
          }
          break;
        }
        size += splitgLength;
      }

    }

    return retLists;
  }

  Map<Path, ArrayList<String>> removeScheme(Map<Path, ArrayList<String>> pathToAliases) {
    Map<Path, ArrayList<String>> result = new HashMap<>();
    for (Map.Entry<Path, ArrayList<String>> entry : pathToAliases.entrySet()) {
      Path newKey = Path.getPathWithoutSchemeAndAuthority(entry.getKey());
      StringInternUtils.internUriStringsInPath(newKey);
      result.put(newKey, entry.getValue());
    }
    return result;
  }

  /**
   * Create a generic Hive RecordReader than can iterate over all chunks in a CombinedFileSplit.
   */
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    if (!(split instanceof CombineHiveInputSplit)) {
      return super.getRecordReader(split, job, reporter);
    }

    CombineHiveInputSplit hsplit = (CombineHiveInputSplit) split;

    String inputFormatClassName = null;
    Class<?> inputFormatClass;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    pushProjectionsAndFilters(job, inputFormatClass, hsplit.getPath(0));

    if (inputFormatClass.getName().equals(getParquetRealtimeInputFormatClassName())) {
      HoodieCombineFileInputFormatShim shims = createInputFormatShim();
      IOContextMap.get(job).setInputPath(((CombineHiveInputSplit) split).getPath(0));
      job.set("hudi.hive.realtime","true");
      return shims.getRecordReader(job, ((CombineHiveInputSplit) split).getInputSplitShim(),
          reporter, CombineHiveRecordReader.class);
    } else {
      return ShimLoader.getHadoopShims().getCombineFileInputFormat().getRecordReader(job, (CombineFileSplit) split,
          reporter, CombineHiveRecordReader.class);
    }
  }

  /**
   * This is a marker interface that is used to identify the formats where combine split generation is not applicable.
   */
  public interface AvoidSplitCombination {

    boolean shouldSkipCombine(Path path, Configuration conf) throws IOException;
  }

  /**
   * CombineHiveInputSplit encapsulates an InputSplit with its corresponding inputFormatClassName. A
   * CombineHiveInputSplit comprises of multiple chunks from different files. Since, they belong to a single directory,
   * there is a single inputformat for all the chunks.
   */
  public static class CombineHiveInputSplit extends InputSplitShim {

    private String inputFormatClassName;
    protected CombineFileSplit inputSplitShim;
    private Map<Path, PartitionDesc> pathToPartitionInfo;

    public CombineHiveInputSplit() throws IOException {
      this(ShimLoader.getHadoopShims().getCombineFileInputFormat().getInputSplitShim());
    }

    public CombineHiveInputSplit(CombineFileSplit inputSplitShim) throws IOException {
      this(inputSplitShim.getJob(), inputSplitShim);
    }

    public CombineHiveInputSplit(JobConf job, CombineFileSplit inputSplitShim) throws IOException {
      this(job, inputSplitShim, null);
    }

    public CombineHiveInputSplit(JobConf job, CombineFileSplit inputSplitShim,
                                 Map<Path, PartitionDesc> pathToPartitionInfo) throws IOException {
      this.inputSplitShim = inputSplitShim;
      this.pathToPartitionInfo = pathToPartitionInfo;
      if (job != null) {
        if (this.pathToPartitionInfo == null) {
          this.pathToPartitionInfo = Utilities.getMapWork(job).getPathToPartitionInfo();
        }

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        Path[] ipaths = inputSplitShim.getPaths();
        if (ipaths.length > 0) {
          PartitionDesc part = getPartitionFromPath(this.pathToPartitionInfo, ipaths[0],
              IOPrepareCache.get().getPartitionDescMap());
          inputFormatClassName = part.getInputFileFormatClass().getName();
        }
      }
    }

    public CombineFileSplit getInputSplitShim() {
      return inputSplitShim;
    }

    /**
     * Returns the inputFormat class name for the i-th chunk.
     */
    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    public void setInputFormatClassName(String inputFormatClassName) {
      this.inputFormatClassName = inputFormatClassName;
    }

    @Override
    public JobConf getJob() {
      return inputSplitShim.getJob();
    }

    @Override
    public long getLength() {
      return inputSplitShim.getLength();
    }

    /**
     * Returns an array containing the startoffsets of the files in the split.
     */
    @Override
    public long[] getStartOffsets() {
      return inputSplitShim.getStartOffsets();
    }

    /**
     * Returns an array containing the lengths of the files in the split.
     */
    @Override
    public long[] getLengths() {
      return inputSplitShim.getLengths();
    }

    /**
     * Returns the start offset of the i<sup>th</sup> Path.
     */
    @Override
    public long getOffset(int i) {
      return inputSplitShim.getOffset(i);
    }

    /**
     * Returns the length of the i<sup>th</sup> Path.
     */
    @Override
    public long getLength(int i) {
      return inputSplitShim.getLength(i);
    }

    /**
     * Returns the number of Paths in the split.
     */
    @Override
    public int getNumPaths() {
      return inputSplitShim.getNumPaths();
    }

    /**
     * Returns the i<sup>th</sup> Path.
     */
    @Override
    public Path getPath(int i) {
      return inputSplitShim.getPath(i);
    }

    /**
     * Returns all the Paths in the split.
     */
    @Override
    public Path[] getPaths() {
      return inputSplitShim.getPaths();
    }

    /**
     * Returns all the Paths where this input-split resides.
     */
    @Override
    public String[] getLocations() throws IOException {
      return inputSplitShim.getLocations();
    }

    /**
     * Prints this object as a string.
     */
    @Override
    public String toString() {
      return inputSplitShim.toString()
          + "InputFormatClass: " + inputFormatClassName
          + "\n";
    }

    /**
     * Writable interface.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      inputFormatClassName = Text.readString(in);
      if (HoodieParquetRealtimeInputFormat.class.getName().equals(inputFormatClassName)) {
        String inputShimClassName = Text.readString(in);
        inputSplitShim = ReflectionUtils.loadClass(inputShimClassName);
        inputSplitShim.readFields(in);
      } else {
        inputSplitShim.readFields(in);
      }
    }

    /**
     * Writable interface.
     */
    @Override
    public void write(DataOutput out) throws IOException {
      if (inputFormatClassName == null) {
        if (pathToPartitionInfo == null) {
          pathToPartitionInfo = Utilities.getMapWork(getJob()).getPathToPartitionInfo();
        }

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        PartitionDesc part = getPartitionFromPath(pathToPartitionInfo, inputSplitShim.getPath(0),
            IOPrepareCache.get().getPartitionDescMap());

        // create a new InputFormat instance if this is the first time to see
        // this class
        inputFormatClassName = part.getInputFileFormatClass().getName();
      }
      Text.writeString(out, inputFormatClassName);
      if (HoodieParquetRealtimeInputFormat.class.getName().equals(inputFormatClassName)) {
        // Write Shim Class Name
        Text.writeString(out, inputSplitShim.getClass().getName());
      }
      inputSplitShim.write(out);
    }
  }

  // Splits are not shared across different partitions with different input formats.
  // For example, 2 partitions (1 sequencefile and 1 rcfile) will have 2 different splits
  private static class CombinePathInputFormat {

    private final List<Operator<? extends OperatorDesc>> opList;
    private final String inputFormatClassName;
    private final String deserializerClassName;

    public CombinePathInputFormat(List<Operator<? extends OperatorDesc>> opList, String inputFormatClassName,
                                  String deserializerClassName) {
      this.opList = opList;
      this.inputFormatClassName = inputFormatClassName;
      this.deserializerClassName = deserializerClassName;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CombinePathInputFormat) {
        CombinePathInputFormat mObj = (CombinePathInputFormat) o;
        return (opList.equals(mObj.opList)) && (inputFormatClassName.equals(mObj.inputFormatClassName))
            && (Objects.equals(deserializerClassName, mObj.deserializerClassName));
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (opList == null) ? 0 : opList.hashCode();
    }
  }

  static class CombineFilter implements PathFilter {

    private final Set<String> pStrings = new HashSet<>();

    // store a path prefix in this TestFilter
    // PRECONDITION: p should always be a directory
    public CombineFilter(Path p) {
      // we need to keep the path part only because the Hadoop CombineFileInputFormat will
      // pass the path part only to accept().
      // Trailing the path with a separator to prevent partial matching.
      addPath(p);
    }

    public void addPath(Path p) {
      String pString = p.toUri().getPath();
      pStrings.add(pString);
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    @Override
    public boolean accept(Path path) {
      boolean find = false;
      while (path != null) {
        if (pStrings.contains(path.toUri().getPath())) {
          find = true;
          break;
        }
        path = path.getParent();
      }
      return find;
    }

    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      s.append("PathFilter: ");
      for (String pString : pStrings) {
        s.append(pString + " ");
      }
      return s.toString();
    }
  }

  /**
   * **MOD** This is the implementation of CombineFileInputFormat which is a copy of
   * org.apache.hadoop.hive.shims.HadoopShimsSecure.CombineFileInputFormatShim with changes in listStatus.
   */
  public static class HoodieCombineFileInputFormatShim<K, V> extends CombineFileInputFormat<K, V>
      implements org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim<K, V> {

    private boolean hoodieFilter = false;
    private boolean isRealTime = false;

    protected HoodieParquetInputFormat createParquetInputFormat() {
      return new HoodieParquetInputFormat();
    }

    protected HoodieParquetRealtimeInputFormat createParquetRealtimeInputFormat() {
      return new HoodieParquetRealtimeInputFormat();
    }

    public HoodieCombineFileInputFormatShim() {
    }

    @Override
    public Path[] getInputPathsShim(JobConf conf) {
      try {
        return FileInputFormat.getInputPaths(conf);
      } catch (Exception var3) {
        throw new RuntimeException(var3);
      }
    }

    @Override
    public void createPool(JobConf conf, PathFilter... filters) {
      super.createPool(conf, filters);
    }

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
      throw new IOException("CombineFileInputFormat.getRecordReader not needed.");
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
      LOG.info("Listing status in HoodieCombineHiveInputFormat.HoodieCombineFileInputFormatShim");
      List<FileStatus> result;
      if (hoodieFilter) {
        HoodieParquetInputFormatBase input;
        if (isRealTime) {
          LOG.info("Using HoodieRealtimeInputFormat");
          input = createParquetRealtimeInputFormat();
        } else {
          LOG.info("Using HoodieInputFormat");
          input = createParquetInputFormat();
        }
        input.setConf(job.getConfiguration());
        result = new ArrayList<>(Arrays.asList(input.listStatus(new JobConf(job.getConfiguration()))));
      } else {
        result = super.listStatus(job);
      }

      result.removeIf(stat -> !stat.isFile());
      return result;
    }

    @Override
    public CombineFileSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      long minSize = job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 0L);
      long maxSize = job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, minSize);
      if (job.getLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0L) == 0L) {
        super.setMinSplitSizeNode(minSize);
      }

      if (job.getLong("mapreduce.input.fileinputformat.split.minsize.per.rack", 0L) == 0L) {
        super.setMinSplitSizeRack(minSize);
      }

      if (job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, 0L) == 0L) {
        super.setMaxSplitSize(minSize);
      }
      LOG.info("mapreduce.input.fileinputformat.split.minsize=" + minSize
          + ", mapreduce.input.fileinputformat.split.maxsize=" + maxSize);

      if (isRealTime) {
        job.set("hudi.hive.realtime", "true");
        InputSplit[] splits;
        if (hoodieFilter) {
          HoodieParquetRealtimeInputFormat input = createParquetRealtimeInputFormat();
          input.setConf(job);
          splits = input.getSplits(job, numSplits);
        } else {
          splits = super.getSplits(job, numSplits);
        }
        ArrayList<CombineFileSplit> combineFileSplits = new ArrayList<>();
        HoodieCombineRealtimeFileSplit.Builder builder = new HoodieCombineRealtimeFileSplit.Builder();
        long counter = 0;
        for (int pos = 0; pos < splits.length; pos++) {
          InputSplit split = splits[pos];
          counter += split.getLength();
          builder.addSplit((FileSplit) split);
          if (counter >= maxSize || pos == splits.length - 1) {
            combineFileSplits.add(builder.build(job));
            builder = new HoodieCombineRealtimeFileSplit.Builder();
            counter = 0;
          }
        }
        return combineFileSplits.toArray(new CombineFileSplit[combineFileSplits.size()]);
      } else {
        InputSplit[] splits = super.getSplits(job, numSplits);
        ArrayList inputSplitShims = new ArrayList();

        for (int pos = 0; pos < splits.length; ++pos) {
          CombineFileSplit split = (CombineFileSplit) splits[pos];
          if (split.getPaths().length > 0) {
            inputSplitShims.add(new HadoopShimsSecure.InputSplitShim(job, split.getPaths(), split.getStartOffsets(),
                split.getLengths(), split.getLocations()));
          }
        }
        return (CombineFileSplit[]) inputSplitShims
            .toArray(new HadoopShimsSecure.InputSplitShim[inputSplitShims.size()]);
      }
    }

    @Override
    public HadoopShimsSecure.InputSplitShim getInputSplitShim() {
      return new HadoopShimsSecure.InputSplitShim();
    }

    @Override
    public RecordReader getRecordReader(JobConf job, CombineFileSplit split, Reporter reporter,
                                        Class<RecordReader<K, V>> rrClass) throws IOException {
      isRealTime = Boolean.valueOf(job.get("hudi.hive.realtime", "false"));
      if (isRealTime) {
        List<RecordReader> recordReaders = new LinkedList<>();
        ValidationUtils.checkArgument(split instanceof HoodieCombineRealtimeFileSplit, "Only "
            + HoodieCombineRealtimeFileSplit.class.getName() + " allowed, found " + split.getClass().getName());
        for (InputSplit inputSplit : ((HoodieCombineRealtimeFileSplit) split).getRealtimeFileSplits()) {
          if (split.getPaths().length == 0) {
            continue;
          }
          FileInputFormat inputFormat = HoodieInputFormatUtils.getInputFormat(split.getPath(0).toString(), true, job);
          recordReaders.add(inputFormat.getRecordReader(inputSplit, job, reporter));
        }
        return new HoodieCombineRealtimeRecordReader(job, split, recordReaders);
      }
      return new HadoopShimsSecure.CombineFileRecordReader(job, split, reporter, rrClass);
    }

    public void setHoodieFilter(boolean hoodieFilter) {
      this.hoodieFilter = hoodieFilter;
    }

    public void setRealTime(boolean realTime) {
      isRealTime = realTime;
    }
  }

  private class CheckNonCombinablePathCallable implements Callable<Set<Integer>> {

    private final Path[] paths;
    private final int start;
    private final int length;
    private final JobConf conf;

    public CheckNonCombinablePathCallable(Path[] paths, int start, int length, JobConf conf) {
      this.paths = paths;
      this.start = start;
      this.length = length;
      this.conf = conf;
    }

    @Override
    public Set<Integer> call() throws Exception {
      Set<Integer> nonCombinablePathIndices = new HashSet<Integer>();
      for (int i = 0; i < length; i++) {
        PartitionDesc part = getPartitionFromPath(pathToPartitionInfo, paths[i + start],
            IOPrepareCache.get().allocatePartitionDescMap());
        // Use HiveInputFormat if any of the paths is not splittable
        Class<? extends InputFormat> inputFormatClass = part.getInputFileFormatClass();
        InputFormat<WritableComparable, Writable> inputFormat = getInputFormatFromCache(inputFormatClass, conf);
        if (inputFormat instanceof AvoidSplitCombination
            && ((AvoidSplitCombination) inputFormat).shouldSkipCombine(paths[i + start], conf)) {
          LOG.debug("The path [{}] is being parked for HiveInputFormat.getSplits", paths[i + start]);
          nonCombinablePathIndices.add(i + start);
        }
      }
      return nonCombinablePathIndices;
    }
  }
}
