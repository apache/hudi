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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.DTOUtils;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.timeline.TimelineServiceClient;
import org.apache.hudi.timeline.TimelineServiceClientBase.RequestMethod;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A proxy for table file-system view which translates local View API calls to REST calls to remote timeline service.
 */
public class RemoteHoodieTableFileSystemView implements SyncableFileSystemView, Serializable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

  private static final String SCHEME = "http";
  private static final String BASE_URL = "/v1/hoodie/view";
  public static final String LATEST_PARTITION_SLICES_URL = String.format("%s/%s", BASE_URL, "slices/partition/latest/");
  public static final String LATEST_PARTITION_SLICES_INFLIGHT_URL = String.format("%s/%s", BASE_URL, "slices/partition/latest/inflight/");
  public static final String LATEST_PARTITION_SLICES_STATELESS_URL = String.format("%s/%s", BASE_URL, "slices/partition/latest/stateless/");
  public static final String LATEST_PARTITION_SLICE_URL = String.format("%s/%s", BASE_URL, "slices/file/latest/");
  public static final String LATEST_PARTITION_UNCOMPACTED_SLICES_URL =  String.format("%s/%s", BASE_URL, "slices/uncompacted/partition/latest/");
  public static final String ALL_SLICES_URL = String.format("%s/%s", BASE_URL, "slices/all");
  public static final String LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "slices/merged/beforeoron/latest/");
  public static final String LATEST_SLICE_MERGED_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "slice/merged/beforeoron/latest/");
  public static final String LATEST_SLICES_RANGE_INSTANT_URL = String.format("%s/%s", BASE_URL, "slices/range/latest/");
  public static final String LATEST_SLICES_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "slices/beforeoron/latest/");
  public static final String ALL_LATEST_SLICES_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "slices/all/beforeoron/latest/");

  public static final String PENDING_COMPACTION_OPS_URL = String.format("%s/%s", BASE_URL, "compactions/pending/");
  public static final String PENDING_LOG_COMPACTION_OPS_URL = String.format("%s/%s", BASE_URL, "logcompactions/pending/");

  public static final String LATEST_PARTITION_DATA_FILES_URL = String.format("%s/%s", BASE_URL, "datafiles/latest/partition");
  public static final String LATEST_PARTITION_DATA_FILE_URL = String.format("%s/%s", BASE_URL, "datafile/latest/partition");
  public static final String ALL_DATA_FILES_URL = String.format("%s/%s", BASE_URL, "datafiles/all");
  public static final String LATEST_ALL_DATA_FILES_URL = String.format("%s/%s", BASE_URL, "datafiles/all/latest/");
  public static final String LATEST_DATA_FILE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "datafile/on/latest/");
  public static final String LATEST_DATA_FILES_RANGE_INSTANT_URL = String.format("%s/%s", BASE_URL, "datafiles/range/latest/");
  public static final String LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "datafiles/beforeoron/latest/");
  public static final String ALL_LATEST_BASE_FILES_BEFORE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "basefiles/all/beforeoron/");

  public static final String ALL_FILEGROUPS_FOR_PARTITION_URL = String.format("%s/%s", BASE_URL, "filegroups/all/partition/");
  public static final String ALL_FILEGROUPS_FOR_PARTITION_STATELESS_URL = String.format("%s/%s", BASE_URL, "filegroups/all/partition/stateless/");
  public static final String ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON_URL = String.format("%s/%s", BASE_URL, "filegroups/replaced/beforeoron/");
  public static final String ALL_REPLACED_FILEGROUPS_BEFORE_URL = String.format("%s/%s", BASE_URL, "filegroups/replaced/before/");
  public static final String ALL_REPLACED_FILEGROUPS_AFTER_OR_ON_URL = String.format("%s/%s", BASE_URL, "filegroups/replaced/afteroron/");
  public static final String ALL_REPLACED_FILEGROUPS_PARTITION_URL = String.format("%s/%s", BASE_URL, "filegroups/replaced/partition/");
  
  public static final String PENDING_CLUSTERING_FILEGROUPS_URL = String.format("%s/%s", BASE_URL, "clustering/pending/");

  public static final String LAST_INSTANT_URL = String.format("%s/%s", BASE_URL, "timeline/instant/last");
  public static final String LAST_INSTANTS_URL = String.format("%s/%s", BASE_URL, "timeline/instants/last");

  public static final String TIMELINE_URL = String.format("%s/%s", BASE_URL, "timeline/instants/all");
  public static final String GET_TIMELINE_HASH_URL = String.format("%s/%s", BASE_URL, "timeline/hash");

  // POST Requests
  public static final String REFRESH_TABLE_URL = String.format("%s/%s", BASE_URL, "refresh/");
  public static final String INIT_TIMELINE_URL = String.format("%s/%s", BASE_URL, "inittimeline");
  public static final String CLOSE_TABLE_URL = String.format("%s/%s", BASE_URL, "close/");
  public static final String LOAD_ALL_PARTITIONS_URL = String.format("%s/%s", BASE_URL, "loadallpartitions/");
  public static final String LOAD_PARTITIONS_URL = String.format("%s/%s", BASE_URL, "loadpartitions/");

  public static final String PARTITION_PARAM = "partition";
  public static final String PARTITIONS_PARAM = "partitions";
  public static final String BASEPATH_PARAM = "basepath";
  public static final String INSTANT_PARAM = "instant";
  public static final String MAX_INSTANT_PARAM = "maxinstant";
  public static final String MIN_INSTANT_PARAM = "mininstant";
  public static final String INSTANTS_PARAM = "instants";
  public static final String FILEID_PARAM = "fileid";
  public static final String LAST_INSTANT_TS = "lastinstantts";
  public static final String TIMELINE_HASH = "timelinehash";
  public static final String REFRESH_OFF = "refreshoff";
  public static final String INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM = "includependingcompaction";

  public static final String MULTI_VALUE_SEPARATOR = ",";

  private static final Logger LOG = LoggerFactory.getLogger(RemoteHoodieTableFileSystemView.class);
  private static final TypeReference<List<FileSliceDTO>> FILE_SLICE_DTOS_REFERENCE = new TypeReference<List<FileSliceDTO>>() {};
  private static final TypeReference<List<FileGroupDTO>> FILE_GROUP_DTOS_REFERENCE = new TypeReference<List<FileGroupDTO>>() {};
  private static final TypeReference<Boolean> BOOLEAN_TYPE_REFERENCE = new TypeReference<Boolean>() {};
  private static final TypeReference<String> STRING_TYPE_REFERENCE = new TypeReference<String>() {};
  private static final TypeReference<List<CompactionOpDTO>> COMPACTION_OP_DTOS_REFERENCE = new TypeReference<List<CompactionOpDTO>>() {};
  private static final TypeReference<List<ClusteringOpDTO>> CLUSTERING_OP_DTOS_REFERENCE = new TypeReference<List<ClusteringOpDTO>>() {};
  private static final TypeReference<List<InstantDTO>> INSTANT_DTOS_REFERENCE = new TypeReference<List<InstantDTO>>() {};
  private static final TypeReference<TimelineDTO> TIMELINE_DTO_REFERENCE = new TypeReference<TimelineDTO>() {};
  private static final TypeReference<List<BaseFileDTO>> BASE_FILE_DTOS_REFERENCE = new TypeReference<List<BaseFileDTO>>() {};
  private static final TypeReference<Map<String, List<BaseFileDTO>>> BASE_FILE_MAP_REFERENCE = new TypeReference<Map<String, List<BaseFileDTO>>>() {};
  private static final TypeReference<Map<String, List<FileSliceDTO>>> FILE_SLICE_MAP_REFERENCE = new TypeReference<Map<String, List<FileSliceDTO>>>() {};

  private final String basePath;
  private final HoodieTableMetaClient metaClient;
  private HoodieTimeline timeline;
  private final TimelineServiceClient timelineServiceClient;
  private final boolean remoteInitEnabled;

  private boolean closed = false;

  public RemoteHoodieTableFileSystemView(String server, int port, HoodieTableMetaClient metaClient) {
    this(metaClient, FileSystemViewStorageConfig.newBuilder().withRemoteServerHost(server).withRemoteServerPort(port).build());
  }

  public RemoteHoodieTableFileSystemView(HoodieTableMetaClient metaClient, FileSystemViewStorageConfig viewConf) {
    this(metaClient, TimelineUtils.getVisibleTimelineForFsView(metaClient), viewConf);
  }

  public RemoteHoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline, FileSystemViewStorageConfig viewConf) {
    this(metaClient, timeline, new TimelineServiceClient(viewConf), viewConf);
  }

  @VisibleForTesting
  public RemoteHoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline, TimelineServiceClient timelineServiceClient, FileSystemViewStorageConfig viewConf) {
    this.basePath = metaClient.getBasePath().toString();
    this.metaClient = metaClient;
    this.timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    this.timelineServiceClient = timelineServiceClient;
    this.remoteInitEnabled = viewConf.isRemoteInitEnabled();
    if (remoteInitEnabled) {
      initialiseTimelineInRemoteView(timeline);
    }
  }

  public void initialiseTimelineInRemoteView(HoodieTimeline timeline) {
    try {
      this.timeline = timeline;
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put(BASEPATH_PARAM, basePath);
      String timelineHashFromServer = executeRequest(GET_TIMELINE_HASH_URL, queryParams, STRING_TYPE_REFERENCE, RequestMethod.GET);
      if (!Objects.equals(timelineHashFromServer, timeline.getTimelineHash())) {
        LOG.debug("Initialising timeline in remote view for {}", basePath);
        String body = OBJECT_MAPPER.writeValueAsString(TimelineDTO.fromTimeline(timeline));
        executeRequest(INIT_TIMELINE_URL, queryParams, body, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
      }
    } catch (IOException e) {
      throw new HoodieRemoteException("Failed to initialise timeline remotely in server", e);
    }
  }

  private <T> T executeRequest(String requestPath, Map<String, String> queryParameters, TypeReference<T> reference,
                               RequestMethod method) throws IOException {
    return executeRequest(requestPath, queryParameters, StringUtils.EMPTY_STRING, reference, method);
  }

  private <T> T executeRequest(String requestPath, Map<String, String> queryParameters, String body, TypeReference<T> reference,
                               RequestMethod method) throws IOException {
    ValidationUtils.checkArgument(!closed, "View already closed");
    // Adding mandatory parameters - Last instants affecting file-slice
    timeline.lastInstant().ifPresent(instant -> queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, timeline.getTimelineHash());
    return timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(method, requestPath).addQueryParams(queryParameters).setBody(body).build())
        .getDecodedContent(reference);
  }

  private Map<String, String> getParamsWithPartitionPath(String partitionPath) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(PARTITION_PARAM, partitionPath);
    return paramsMap;
  }

  private Map<String, String> getParams() {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    return paramsMap;
  }

  private Map<String, String> getParams(String paramName, String instant) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(paramName, instant);
    return paramsMap;
  }

  private Map<String, String> getParamsWithAdditionalParam(String partitionPath, String paramName, String paramVal) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(PARTITION_PARAM, partitionPath);
    paramsMap.put(paramName, paramVal);
    return paramsMap;
  }

  private Map<String, String> getParamsWithAdditionalParams(String partitionPath, String[] paramNames,
      String[] paramVals) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(PARTITION_PARAM, partitionPath);
    ValidationUtils.checkArgument(paramNames.length == paramVals.length);
    for (int i = 0; i < paramNames.length; i++) {
      paramsMap.put(paramNames[i], paramVals[i]);
    }
    return paramsMap;
  }

  private Stream<HoodieBaseFile> getLatestBaseFilesFromParams(String requestPath, Map<String, String> paramsMap) {
    try {
      List<BaseFileDTO> dataFiles = executeRequest(requestPath, paramsMap,
          BASE_FILE_DTOS_REFERENCE, RequestMethod.GET);
      return dataFiles.stream().map(BaseFileDTO::toHoodieBaseFile);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestBaseFilesFromParams(LATEST_PARTITION_DATA_FILES_URL, paramsMap);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles() {
    Map<String, String> paramsMap = getParams();
    return getLatestBaseFilesFromParams(LATEST_ALL_DATA_FILES_URL, paramsMap);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    return getLatestBaseFilesFromParams(LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL, paramsMap);
  }

  @Override
  public Map<String, Stream<HoodieBaseFile>> getAllLatestBaseFilesBeforeOrOn(String maxCommitTime) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(MAX_INSTANT_PARAM, maxCommitTime);

    try {
      Map<String, List<BaseFileDTO>> dataFileMap = executeRequest(
          ALL_LATEST_BASE_FILES_BEFORE_ON_INSTANT_URL,
          paramsMap,
          BASE_FILE_MAP_REFERENCE,
          RequestMethod.GET);
      return dataFileMap.entrySet().stream().collect(
          Collectors.toMap(
              Map.Entry::getKey,
              entry -> entry.getValue().stream().map(BaseFileDTO::toHoodieBaseFile)));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Option<HoodieBaseFile> getBaseFileOn(String partitionPath, String instantTime, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(partitionPath,
        new String[] {INSTANT_PARAM, FILEID_PARAM}, new String[] {instantTime, fileId});
    return Option.fromJavaOptional(getLatestBaseFilesFromParams(LATEST_DATA_FILE_ON_INSTANT_URL, paramsMap).findFirst());
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn) {
    Map<String, String> paramsMap = getParams(INSTANTS_PARAM, String.join(MULTI_VALUE_SEPARATOR, commitsToReturn));
    return getLatestBaseFilesFromParams(LATEST_DATA_FILES_RANGE_INSTANT_URL, paramsMap);
  }

  @Override
  public Stream<HoodieBaseFile> getAllBaseFiles(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestBaseFilesFromParams(ALL_DATA_FILES_URL, paramsMap);
  }

  @Override
  public Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, FILEID_PARAM, fileId);
    return Option.fromJavaOptional(getLatestBaseFilesFromParams(LATEST_PARTITION_DATA_FILE_URL, paramsMap).findFirst());
  }

  private Stream<FileSlice> getLatestFileSlicesStreamFromParams(String requestPath, Map<String, String> paramsMap) {
    try {
      List<FileSliceDTO> dataFiles = executeRequest(requestPath, paramsMap,
          FILE_SLICE_DTOS_REFERENCE, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(LATEST_PARTITION_SLICES_URL, paramsMap);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesIncludingInflight(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(LATEST_PARTITION_SLICES_INFLIGHT_URL, paramsMap);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesStateless(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(LATEST_PARTITION_SLICES_STATELESS_URL, paramsMap);
  }

  @Override
  public Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, FILEID_PARAM, fileId);
    return Option.fromJavaOptional(getLatestFileSlicesStreamFromParams(LATEST_PARTITION_SLICE_URL, paramsMap).findFirst());
  }

  @Override
  public Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(LATEST_PARTITION_UNCOMPACTED_SLICES_URL, paramsMap);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime,
      boolean includeFileSlicesInPendingCompaction) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(partitionPath,
        new String[] {MAX_INSTANT_PARAM, INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM},
        new String[] {maxCommitTime, String.valueOf(includeFileSlicesInPendingCompaction)});
    return getLatestFileSlicesStreamFromParams(LATEST_SLICES_BEFORE_ON_INSTANT_URL, paramsMap);
  }

  @Override
  public Map<String, Stream<FileSlice>> getAllLatestFileSlicesBeforeOrOn(String maxCommitTime) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    paramsMap.put(MAX_INSTANT_PARAM, maxCommitTime);

    try {
      Map<String, List<FileSliceDTO>> fileSliceMap = executeRequest(ALL_LATEST_SLICES_BEFORE_ON_INSTANT_URL, paramsMap,
          FILE_SLICE_MAP_REFERENCE, RequestMethod.GET);
      return fileSliceMap.entrySet().stream().collect(
          Collectors.toMap(
              Map.Entry::getKey,
              entry -> entry.getValue().stream().map(FileSliceDTO::toFileSlice)));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxInstantTime);
    return getLatestFileSlicesStreamFromParams(LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, paramsMap);
  }

  @Override
  public Option<FileSlice> getLatestMergedFileSliceBeforeOrOn(String partitionPath, String maxInstantTime, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(partitionPath, new String[] {MAX_INSTANT_PARAM, FILEID_PARAM},
        new String[] {maxInstantTime, fileId});
    return Option.fromJavaOptional(getLatestFileSlicesStreamFromParams(LATEST_SLICE_MERGED_BEFORE_ON_INSTANT_URL, paramsMap).findFirst());
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    Map<String, String> paramsMap = getParams(INSTANTS_PARAM, String.join(MULTI_VALUE_SEPARATOR, commitsToReturn));
    return getLatestFileSlicesStreamFromParams(LATEST_SLICES_RANGE_INSTANT_URL, paramsMap);
  }

  @Override
  public Stream<FileSlice> getAllFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(ALL_SLICES_URL, paramsMap);
  }

  private Stream<HoodieFileGroup> getAllFileGroupsForPartitionFromParams(String requestPath, Map<String, String> paramsMap) {
    try {
      List<FileGroupDTO> fileGroups = executeRequest(requestPath, paramsMap,
          FILE_GROUP_DTOS_REFERENCE, RequestMethod.GET);
      return DTOUtils.fileGroupDTOsToFileGroups(fileGroups, metaClient);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroups(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getAllFileGroupsForPartitionFromParams(ALL_FILEGROUPS_FOR_PARTITION_URL, paramsMap);
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroupsStateless(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getAllFileGroupsForPartitionFromParams(ALL_FILEGROUPS_FOR_PARTITION_STATELESS_URL, paramsMap);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    return getAllFileGroupsForPartitionFromParams(ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON_URL, paramsMap);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    return getAllFileGroupsForPartitionFromParams(ALL_REPLACED_FILEGROUPS_BEFORE_URL, paramsMap);
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsAfterOrOn(String minCommitTime, String partitionPath) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MIN_INSTANT_PARAM, minCommitTime);
    return getAllFileGroupsForPartitionFromParams(ALL_REPLACED_FILEGROUPS_AFTER_OR_ON_URL, paramsMap);
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getAllFileGroupsForPartitionFromParams(ALL_REPLACED_FILEGROUPS_PARTITION_URL, paramsMap);
  }

  public boolean refresh() {
    Map<String, String> paramsMap = getParams();
    try {
      // refresh the local timeline first.
      metaClient.reloadActiveTimeline();
      this.timeline = TimelineUtils.getVisibleTimelineForFsView(metaClient);
      if (remoteInitEnabled) {
        initialiseTimelineInRemoteView(timeline);
        return true;
      } else {
        return executeRequest(REFRESH_TABLE_URL, paramsMap, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
      }
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  private void loadPartitions(String requestPath, Map<String, String> paramsMap) {
    try {
      executeRequest(requestPath, paramsMap, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public void loadAllPartitions() {
    Map<String, String> paramsMap = getParams();
    loadPartitions(LOAD_ALL_PARTITIONS_URL, paramsMap);
  }

  @Override
  public void loadPartitions(List<String> partitionPaths) {
    try {
      String body = OBJECT_MAPPER.writeValueAsString(partitionPaths);
      executeRequest(LOAD_PARTITIONS_URL, getParams(), body, BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  private Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations(String requestPath, Map<String, String> paramsMap) {
    try {
      List<CompactionOpDTO> dtos = executeRequest(requestPath, paramsMap,
          COMPACTION_OP_DTOS_REFERENCE, RequestMethod.GET);
      return dtos.stream().map(CompactionOpDTO::toCompactionOperation);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    Map<String, String> paramsMap = getParams();
    return getPendingCompactionOperations(PENDING_COMPACTION_OPS_URL, paramsMap);
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingLogCompactionOperations() {
    Map<String, String> paramsMap = getParams();
    return getPendingCompactionOperations(PENDING_LOG_COMPACTION_OPS_URL, paramsMap);
  }

  @Override
  public Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering() {
    Map<String, String> paramsMap = getParams();
    try {
      List<ClusteringOpDTO> dtos = executeRequest(PENDING_CLUSTERING_FILEGROUPS_URL, paramsMap,
          CLUSTERING_OP_DTOS_REFERENCE, RequestMethod.GET);
      InstantGenerator factory = metaClient.getInstantGenerator();
      return dtos.stream().map(dto -> ClusteringOpDTO.toClusteringOperation(dto, factory));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    Map<String, String> paramsMap = getParams();
    try {
      List<InstantDTO> instants = executeRequest(LAST_INSTANT_URL, paramsMap, INSTANT_DTOS_REFERENCE, RequestMethod.GET);
      return Option.fromJavaOptional(instants.stream()
          .map(dto -> InstantDTO.toInstant(dto, metaClient.getInstantGenerator())).findFirst());
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public HoodieTimeline getTimeline() {
    Map<String, String> paramsMap = getParams();
    try {
      TimelineDTO timelineDto = executeRequest(TIMELINE_URL, paramsMap, TIMELINE_DTO_REFERENCE, RequestMethod.GET);
      return TimelineDTO.toTimeline(timelineDto, metaClient);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      LOG.info("Closing view for base path: {}", basePath);
      try {
        executeRequest(CLOSE_TABLE_URL, getParams(), BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
      } catch (IOException ex) {
        LOG.warn("Failed to close table", ex);
      }
      closed = true;
    } else {
      LOG.info("Calling close on a closed view for base path: {}", basePath);
    }
  }

  @Override
  public void reset() {
    refresh();
  }

  @Override
  public void sync() {
    refresh();
  }
}
