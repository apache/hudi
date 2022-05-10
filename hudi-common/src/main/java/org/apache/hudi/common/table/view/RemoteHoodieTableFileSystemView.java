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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A proxy for table file-system view which translates local View API calls to REST calls to remote timeline service.
 */
public class RemoteHoodieTableFileSystemView implements SyncableFileSystemView, Serializable {

  private static final String BASE_URL = "/v1/hoodie/view";
  public static final String LATEST_PARTITION_SLICES_URL = String.format("%s/%s", BASE_URL, "slices/partition/latest/");
  public static final String LATEST_PARTITION_SLICE_URL = String.format("%s/%s", BASE_URL, "slices/file/latest/");
  public static final String LATEST_PARTITION_UNCOMPACTED_SLICES_URL =
      String.format("%s/%s", BASE_URL, "slices/uncompacted/partition/latest/");
  public static final String ALL_SLICES_URL = String.format("%s/%s", BASE_URL, "slices/all");
  public static final String LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL =
      String.format("%s/%s", BASE_URL, "slices/merged/beforeoron/latest/");
  public static final String LATEST_SLICES_RANGE_INSTANT_URL = String.format("%s/%s", BASE_URL, "slices/range/latest/");
  public static final String LATEST_SLICES_BEFORE_ON_INSTANT_URL =
      String.format("%s/%s", BASE_URL, "slices/beforeoron/latest/");

  public static final String PENDING_COMPACTION_OPS = String.format("%s/%s", BASE_URL, "compactions/pending/");

  public static final String LATEST_PARTITION_DATA_FILES_URL =
      String.format("%s/%s", BASE_URL, "datafiles/latest/partition");
  public static final String LATEST_PARTITION_DATA_FILE_URL =
      String.format("%s/%s", BASE_URL, "datafile/latest/partition");
  public static final String ALL_DATA_FILES = String.format("%s/%s", BASE_URL, "datafiles/all");
  public static final String LATEST_ALL_DATA_FILES = String.format("%s/%s", BASE_URL, "datafiles/all/latest/");
  public static final String LATEST_DATA_FILE_ON_INSTANT_URL = String.format("%s/%s", BASE_URL, "datafile/on/latest/");

  public static final String LATEST_DATA_FILES_RANGE_INSTANT_URL =
      String.format("%s/%s", BASE_URL, "datafiles/range/latest/");
  public static final String LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL =
      String.format("%s/%s", BASE_URL, "datafiles/beforeoron/latest/");

  public static final String ALL_FILEGROUPS_FOR_PARTITION_URL =
      String.format("%s/%s", BASE_URL, "filegroups/all/partition/");

  public static final String ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON =
      String.format("%s/%s", BASE_URL, "filegroups/replaced/beforeoron/");

  public static final String ALL_REPLACED_FILEGROUPS_BEFORE =
      String.format("%s/%s", BASE_URL, "filegroups/replaced/before/");

  public static final String ALL_REPLACED_FILEGROUPS_PARTITION =
      String.format("%s/%s", BASE_URL, "filegroups/replaced/partition/");
  
  public static final String PENDING_CLUSTERING_FILEGROUPS = String.format("%s/%s", BASE_URL, "clustering/pending/");


  public static final String LAST_INSTANT = String.format("%s/%s", BASE_URL, "timeline/instant/last");
  public static final String LAST_INSTANTS = String.format("%s/%s", BASE_URL, "timeline/instants/last");

  public static final String TIMELINE = String.format("%s/%s", BASE_URL, "timeline/instants/all");

  // POST Requests
  public static final String REFRESH_TABLE = String.format("%s/%s", BASE_URL, "refresh/");

  public static final String PARTITION_PARAM = "partition";
  public static final String BASEPATH_PARAM = "basepath";
  public static final String INSTANT_PARAM = "instant";
  public static final String MAX_INSTANT_PARAM = "maxinstant";
  public static final String INSTANTS_PARAM = "instants";
  public static final String FILEID_PARAM = "fileid";
  public static final String LAST_INSTANT_TS = "lastinstantts";
  public static final String TIMELINE_HASH = "timelinehash";
  public static final String REFRESH_OFF = "refreshoff";
  public static final String INCLUDE_IN_PENDING_COMPACTION_PARAM = "includependingcompaction";
  public static final String INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM = "includefilespendingcompaction";

  private static final Logger LOG = LogManager.getLogger(RemoteHoodieTableFileSystemView.class);

  private final String serverHost;
  private final int serverPort;
  private final String basePath;
  private final HoodieTableMetaClient metaClient;
  private HoodieTimeline timeline;
  private final ObjectMapper mapper;
  private final int timeoutSecs;

  private boolean closed = false;

  private enum RequestMethod {
    GET, POST
  }

  public RemoteHoodieTableFileSystemView(String server, int port, HoodieTableMetaClient metaClient) {
    this(server, port, metaClient, 300);
  }

  public RemoteHoodieTableFileSystemView(String server, int port, HoodieTableMetaClient metaClient, int timeoutSecs) {
    this.basePath = metaClient.getBasePath();
    this.serverHost = server;
    this.serverPort = port;
    this.mapper = new ObjectMapper();
    this.metaClient = metaClient;
    this.timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    this.timeoutSecs = timeoutSecs;
  }

  private <T> T executeRequest(String requestPath, Map<String, String> queryParameters, TypeReference reference,
      RequestMethod method) throws IOException {
    ValidationUtils.checkArgument(!closed, "View already closed");

    URIBuilder builder =
        new URIBuilder().setHost(serverHost).setPort(serverPort).setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    // Adding mandatory parameters - Last instants affecting file-slice
    timeline.lastInstant().ifPresent(instant -> builder.addParameter(LAST_INSTANT_TS, instant.getTimestamp()));
    builder.addParameter(TIMELINE_HASH, timeline.getTimelineHash());

    String url = builder.toString();
    LOG.info("Sending request : (" + url + ")");
    Response response;
    int timeout = this.timeoutSecs * 1000; // msec
    switch (method) {
      case GET:
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
      case POST:
      default:
        response = Request.Post(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        break;
    }
    String content = response.returnContent().asString();
    return (T) mapper.readValue(content, reference);
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

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestBaseFilesFromParams(paramsMap, LATEST_PARTITION_DATA_FILES_URL);
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFiles() {
    Map<String, String> paramsMap = getParams();
    return getLatestBaseFilesFromParams(paramsMap, LATEST_ALL_DATA_FILES);
  }

  private Stream<HoodieBaseFile> getLatestBaseFilesFromParams(Map<String, String> paramsMap, String requestPath) {
    try {
      List<BaseFileDTO> dataFiles = executeRequest(requestPath, paramsMap,
          new TypeReference<List<BaseFileDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(BaseFileDTO::toHoodieBaseFile);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesBeforeOrOn(String partitionPath, String maxCommitTime) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    return getLatestBaseFilesFromParams(paramsMap, LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL);
  }

  @Override
  public Option<HoodieBaseFile> getBaseFileOn(String partitionPath, String instantTime, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(partitionPath,
        new String[] {INSTANT_PARAM, FILEID_PARAM}, new String[] {instantTime, fileId});
    try {
      List<BaseFileDTO> dataFiles = executeRequest(LATEST_DATA_FILE_ON_INSTANT_URL, paramsMap,
          new TypeReference<List<BaseFileDTO>>() {}, RequestMethod.GET);
      return Option.fromJavaOptional(dataFiles.stream().map(BaseFileDTO::toHoodieBaseFile).findFirst());
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieBaseFile> getLatestBaseFilesInRange(List<String> commitsToReturn) {
    Map<String, String> paramsMap =
        getParams(INSTANTS_PARAM, StringUtils.join(commitsToReturn.toArray(new String[0]), ","));
    return getLatestBaseFilesFromParams(paramsMap, LATEST_DATA_FILES_RANGE_INSTANT_URL);
  }

  @Override
  public Stream<HoodieBaseFile> getAllBaseFiles(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestBaseFilesFromParams(paramsMap, ALL_DATA_FILES);
  }

  @Override
  public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_PARTITION_SLICES_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Option<FileSlice> getLatestFileSlice(String partitionPath, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, FILEID_PARAM, fileId);
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_PARTITION_SLICE_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return Option.fromJavaOptional(dataFiles.stream().map(FileSliceDTO::toFileSlice).findFirst());
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_PARTITION_UNCOMPACTED_SLICES_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionPath, String maxCommitTime,
      boolean includeFileSlicesInPendingCompaction, boolean includeFilesInPendingCompaction) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(partitionPath,
        new String[] {MAX_INSTANT_PARAM, INCLUDE_IN_PENDING_COMPACTION_PARAM, INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM},
        new String[] {maxCommitTime, String.valueOf(includeFileSlicesInPendingCompaction), String.valueOf(includeFilesInPendingCompaction)});
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_SLICES_BEFORE_ON_INSTANT_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionPath, String maxInstantTime) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxInstantTime);
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    Map<String, String> paramsMap =
        getParams(INSTANTS_PARAM, StringUtils.join(commitsToReturn.toArray(new String[0]), ","));
    try {
      List<FileSliceDTO> dataFiles = executeRequest(LATEST_SLICES_RANGE_INSTANT_URL, paramsMap,
          new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<FileSlice> getAllFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    try {
      List<FileSliceDTO> dataFiles =
          executeRequest(ALL_SLICES_URL, paramsMap, new TypeReference<List<FileSliceDTO>>() {}, RequestMethod.GET);
      return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getAllFileGroups(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    try {
      List<FileGroupDTO> fileGroups = executeRequest(ALL_FILEGROUPS_FOR_PARTITION_URL, paramsMap,
          new TypeReference<List<FileGroupDTO>>() {}, RequestMethod.GET);
      return fileGroups.stream().map(dto -> FileGroupDTO.toFileGroup(dto, metaClient));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBeforeOrOn(String maxCommitTime, String partitionPath) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    try {
      List<FileGroupDTO> fileGroups = executeRequest(ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON, paramsMap,
          new TypeReference<List<FileGroupDTO>>() {}, RequestMethod.GET);
      return fileGroups.stream().map(dto -> FileGroupDTO.toFileGroup(dto, metaClient));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getReplacedFileGroupsBefore(String maxCommitTime, String partitionPath) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, MAX_INSTANT_PARAM, maxCommitTime);
    try {
      List<FileGroupDTO> fileGroups = executeRequest(ALL_REPLACED_FILEGROUPS_BEFORE, paramsMap,
          new TypeReference<List<FileGroupDTO>>() {}, RequestMethod.GET);
      return fileGroups.stream().map(dto -> FileGroupDTO.toFileGroup(dto, metaClient));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<HoodieFileGroup> getAllReplacedFileGroups(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    try {
      List<FileGroupDTO> fileGroups = executeRequest(ALL_REPLACED_FILEGROUPS_PARTITION, paramsMap,
          new TypeReference<List<FileGroupDTO>>() {}, RequestMethod.GET);
      return fileGroups.stream().map(dto -> FileGroupDTO.toFileGroup(dto, metaClient));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public boolean refresh() {
    Map<String, String> paramsMap = getParams();
    try {
      // refresh the local timeline first.
      this.timeline = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
      return executeRequest(REFRESH_TABLE, paramsMap, new TypeReference<Boolean>() {}, RequestMethod.POST);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    Map<String, String> paramsMap = getParams();
    try {
      List<CompactionOpDTO> dtos = executeRequest(PENDING_COMPACTION_OPS, paramsMap,
          new TypeReference<List<CompactionOpDTO>>() {}, RequestMethod.GET);
      return dtos.stream().map(CompactionOpDTO::toCompactionOperation);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public Stream<Pair<HoodieFileGroupId, HoodieInstant>> getFileGroupsInPendingClustering() {
    Map<String, String> paramsMap = getParams();
    try {
      List<ClusteringOpDTO> dtos = executeRequest(PENDING_CLUSTERING_FILEGROUPS, paramsMap,
          new TypeReference<List<ClusteringOpDTO>>() {}, RequestMethod.GET);
      return dtos.stream().map(ClusteringOpDTO::toClusteringOperation);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public void reset() {
    refresh();
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    Map<String, String> paramsMap = getParams();
    try {
      List<InstantDTO> instants =
          executeRequest(LAST_INSTANT, paramsMap, new TypeReference<List<InstantDTO>>() {}, RequestMethod.GET);
      return Option.fromJavaOptional(instants.stream().map(InstantDTO::toInstant).findFirst());
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public HoodieTimeline getTimeline() {
    Map<String, String> paramsMap = getParams();
    try {
      TimelineDTO timeline =
          executeRequest(TIMELINE, paramsMap, new TypeReference<TimelineDTO>() {}, RequestMethod.GET);
      return TimelineDTO.toTimeline(timeline, metaClient);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  @Override
  public void sync() {
    refresh();
  }

  @Override
  public Option<HoodieBaseFile> getLatestBaseFile(String partitionPath, String fileId) {
    Map<String, String> paramsMap = getParamsWithAdditionalParam(partitionPath, FILEID_PARAM, fileId);
    try {
      List<BaseFileDTO> dataFiles = executeRequest(LATEST_PARTITION_DATA_FILE_URL, paramsMap,
          new TypeReference<List<BaseFileDTO>>() {}, RequestMethod.GET);
      return Option.fromJavaOptional(dataFiles.stream().map(BaseFileDTO::toHoodieBaseFile).findFirst());
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }
}
