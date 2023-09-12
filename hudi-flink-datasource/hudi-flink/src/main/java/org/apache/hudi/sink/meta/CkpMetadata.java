/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.meta;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.dto.CkpMetadataDTO;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.timeline.service.handlers.FlinkCkpMetadataHandler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The checkpoint metadata for bookkeeping the checkpoint messages.
 *
 * <p>Each time the driver starts a new instant, it writes a commit message into the metadata, the write tasks
 * then consume the message and unblock the data flushing.
 *
 * <p>Why we use the DFS based message queue instead of sending
 * the {@link org.apache.flink.runtime.operators.coordination.OperatorEvent} ?
 * The writer task thread handles the operator event using the main mailbox executor which has the lowest priority for mails,
 * it is also used to process the inputs. When the writer task blocks and waits for the operator event to ack the valid instant to write,
 * it actually blocks all the subsequent events in the mailbox, the operator event would never be consumed then it causes deadlock.
 *
 * <p>The checkpoint metadata is also more lightweight than the active timeline.
 *
 * <p>NOTE: should be removed in the future if we have good manner to handle the async notifications from driver.
 */
public class CkpMetadata implements Serializable, AutoCloseable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CkpMetadata.class);

  // 1 is actually enough for fetching the latest pending instant,
  // keep 3 instants here for purpose of debugging.
  private static final int MAX_RETAIN_CKP_NUM = 3;

  // the ckp metadata directory
  private static final String CKP_META = "ckp_meta";

  private final FileSystem fs;
  protected final Path path;

  private HoodieWriteConfig writeConfig;
  private ObjectMapper mapper;
  private boolean timelineServerBased = false;

  private List<CkpMessage> messages;
  private List<String> instantCache;

  private CkpMetadata(Configuration config, HoodieWriteConfig writeConfig) {
    this(FSUtils.getFs(config.getString(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(config)),
        config.getString(FlinkOptions.PATH), config.getString(FlinkOptions.WRITE_CLIENT_ID), writeConfig);
  }

  private CkpMetadata(FileSystem fs, String basePath, String uniqueId, HoodieWriteConfig writeConfig) {
    this.fs = fs;
    this.path = new Path(ckpMetaPath(basePath, uniqueId));
    this.writeConfig = writeConfig;
    if (writeConfig != null && writeConfig.isTimelineServerBasedFlinkCkpMetadataEnabled()) {
      LOG.info("Timeline server based CkpMetadata enabled");
      this.timelineServerBased = true;
      mapper = new ObjectMapper();
    }
  }

  public void close() {
    this.instantCache = null;
  }

  // -------------------------------------------------------------------------
  //  WRITE METHODS
  // -------------------------------------------------------------------------

  /**
   * Initialize the message bus, would clean all the messages
   *
   * <p>This expects to be called by the driver.
   */
  public void bootstrap() throws IOException {
    fs.delete(path, true);
    fs.mkdirs(path);
  }

  public void startInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.INFLIGHT));
    try {
      fs.createNewFile(path);
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint start metadata for instant: " + instant, e);
    }
    // cache the instant
    cache(instant);
    // cleaning
    clean();
    sendRefreshRequest();
  }

  private void cache(String newInstant) {
    if (this.instantCache == null) {
      this.instantCache = new ArrayList<>();
    }
    this.instantCache.add(newInstant);
  }

  private void clean() {
    if (instantCache.size() > MAX_RETAIN_CKP_NUM) {
      final String instant = instantCache.get(0);
      boolean[] error = new boolean[1];
      CkpMessage.getAllFileNames(instant).stream().map(this::fullPath).forEach(path -> {
        try {
          fs.delete(path, false);
        } catch (IOException e) {
          error[0] = true;
          LOG.warn("Exception while cleaning the checkpoint meta file: " + path);
        }
      });
      if (!error[0]) {
        instantCache.remove(0);
      }
    }
  }

  /**
   * Add a checkpoint commit message.
   *
   * @param instant The committed instant
   */
  public void commitInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.COMPLETED));
    try {
      fs.createNewFile(path);
      sendRefreshRequest();
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint commit metadata for instant: " + instant, e);
    }
  }

  /**
   * Add an aborted checkpoint message.
   */
  public void abortInstant(String instant) {
    Path path = fullPath(CkpMessage.getFileName(instant, CkpMessage.State.ABORTED));
    try {
      fs.createNewFile(path);
      sendRefreshRequest();
    } catch (IOException e) {
      throw new HoodieException("Exception while adding checkpoint abort metadata for instant: " + instant);
    }
  }

  // -------------------------------------------------------------------------
  //  READ METHODS
  // -------------------------------------------------------------------------

  private void load() {
    try {
      this.messages = scanCkpMetadata(this.path);
    } catch (IOException e) {
      throw new HoodieException("Exception while scanning the checkpoint meta files under path: " + this.path, e);
    }
  }

  @Nullable
  public String lastPendingInstant() {
    load();
    if (this.messages.size() > 0) {
      CkpMessage ckpMsg = this.messages.get(this.messages.size() - 1);
      // consider 'aborted' as pending too to reuse the instant
      if (!ckpMsg.isComplete()) {
        return ckpMsg.getInstant();
      }
    }
    return null;
  }

  public List<CkpMessage> getMessages() {
    load();
    return messages;
  }

  public boolean isAborted(String instant) {
    ValidationUtils.checkState(this.messages != null, "The checkpoint metadata should #load first");
    return this.messages.stream().anyMatch(ckpMsg -> instant.equals(ckpMsg.getInstant()) && ckpMsg.isAborted());
  }

  @VisibleForTesting
  public List<String> getInstantCache() {
    return this.instantCache;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  public static CkpMetadata getInstance(Configuration config, HoodieWriteConfig writeConfig) {
    return new CkpMetadata(config, writeConfig);
  }

  public static CkpMetadata getInstance(HoodieTableMetaClient metaClient, String uniqueId, HoodieWriteConfig writeConfig) {
    return new CkpMetadata(metaClient.getFs(), metaClient.getBasePath(), uniqueId, writeConfig);
  }

  public static CkpMetadata getInstance(FileSystem fs, String basePath, String uniqueId, HoodieWriteConfig writeConfig) {
    return new CkpMetadata(fs, basePath, uniqueId, writeConfig);
  }

  protected static String ckpMetaPath(String basePath, String uniqueId) {
    // .hoodie/.aux/ckp_meta
    String metaPath = basePath + Path.SEPARATOR + HoodieTableMetaClient.AUXILIARYFOLDER_NAME + Path.SEPARATOR + CKP_META;
    return StringUtils.isNullOrEmpty(uniqueId) ? metaPath : metaPath + "_" + uniqueId;
  }

  private Path fullPath(String fileName) {
    return new Path(path, fileName);
  }

  private List<CkpMessage> scanCkpMetadata(Path ckpMetaPath) throws IOException {
    // This is required when the storage is minio
    if (!this.fs.exists(ckpMetaPath)) {
      return new ArrayList<>();
    }

    Stream<CkpMessage> ckpMessageStream = null;
    if (timelineServerBased) {
      // Read ckp messages from timeline server
      try {
        List<CkpMetadataDTO> ckpMetadataDTOList = executeRequestToTimelineServerWithRetry(
            FlinkCkpMetadataHandler.ALL_CKP_METADATA_URL, getRequestParams(ckpMetaPath.toString()),
            new TypeReference<List<CkpMetadataDTO>>() {
            }, RequestMethod.GET);
        ckpMessageStream = ckpMetadataDTOList.stream().map(c -> new CkpMessage(c.getInstant(), c.getState()));
      } catch (HoodieException e) {
        LOG.error("Failed to execute scan ckp metadata", e);
      }
    }

    if (ckpMessageStream == null) {
      // If timelineServerBased is not enabled, or we failed to request timeline server, read ckp messages from file system directly.
      ckpMessageStream = Arrays.stream(this.fs.listStatus(ckpMetaPath)).map(CkpMessage::new);
    }

    return ckpMessageStream
        .collect(Collectors.groupingBy(CkpMessage::getInstant)).values().stream()
        .map(messages -> messages.stream().reduce((x, y) -> {
          // Pick the one with the highest state
          if (x.getState().compareTo(y.getState()) >= 0) {
            return x;
          }
          return y;
        }).get())
        .sorted().collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Timeline based checkpoint metadata
  // -------------------------------------------------------------------------
  private Map<String, String> getRequestParams(String dirPath) {
    return Collections.singletonMap(FlinkCkpMetadataHandler.CKP_METADATA_DIR_PATH_PARAM, dirPath);
  }

  /**
   * Refresh the ckp messages cached in timeline server.
   */
  private void sendRefreshRequest() {
    if (!timelineServerBased) {
      return;
    }
    try {
      boolean success = executeRequestToTimelineServerWithRetry(
          FlinkCkpMetadataHandler.REFRESH_CKP_METADATA, getRequestParams(path.toString()),
          new TypeReference<Boolean>() {}, RequestMethod.POST);
      if (!success) {
        LOG.warn("Timeline server responses with failed refresh");
      }
    } catch (Exception e) {
      // Do not propagate the exception because the server will also do auto refresh
      LOG.error("Failed to execute refresh", e);
    }
  }

  private <T> T executeRequestToTimelineServerWithRetry(String requestPath, Map<String, String> queryParameters,
                                                        TypeReference reference, CkpMetadata.RequestMethod method) {
    int retry = 5;
    while (--retry >= 0) {
      long start = System.currentTimeMillis();
      try {
        return executeRequestToTimelineServer(requestPath, queryParameters, reference, method);
      } catch (IOException e) {
        LOG.warn("Failed to execute ckp request (" + requestPath + ") to timeline server", e);
      } finally {
        LOG.info("Execute request : (" + requestPath + "), costs: " + (System.currentTimeMillis() - start) + " ms");
      }
    }
    throw new HoodieException("Failed to execute ckp request (" + requestPath + ")");
  }

  private <T> T executeRequestToTimelineServer(String requestPath, Map<String, String> queryParameters,
                                               TypeReference reference, CkpMetadata.RequestMethod method) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(this.writeConfig.getViewStorageConfig().getRemoteViewServerHost())
            .setPort(this.writeConfig.getViewStorageConfig().getRemoteViewServerPort())
            .setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    int timeout = this.writeConfig.getViewStorageConfig().getRemoteTimelineClientTimeoutSecs() * 1000;
    Response response;
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

  private enum RequestMethod {
    GET, POST
  }
}
