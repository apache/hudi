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

package org.apache.hudi.metaserver.client;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetaServerConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.HoodieMetaServer;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaServer;
import org.apache.hudi.metaserver.util.EntryConvertor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * HoodieMetaServerClientImp based on thrift.
 */
public class HoodieMetaServerClientImp implements HoodieMetaServerClient, Serializable {

  private static final Logger LOG =  LogManager.getLogger(HoodieMetaServerClientImp.class);
  private final HoodieMetaServerConfig config;
  private final int retryLimit;
  private final int retryDelaySeconds;
  private boolean isConnected;
  private boolean isLocal;
  private ThriftHoodieMetaServer.Iface client;
  private TTransport transport;

  public HoodieMetaServerClientImp(HoodieMetaServerConfig config) {
    this.config = config;
    this.retryLimit = config.getConnectionRetryLimit();
    this.retryDelaySeconds = config.getConnectionRetryDelay();
    String uri = config.getMetaServerUris();
    if (isLocalEmbeddedMetaServer(uri)) {
      this.client = HoodieMetaServer.getEmbeddedMetaServer();
      this.isConnected = true;
      this.isLocal = true;
    } else {
      open();
    }
  }

  private void open() {
    String uri = config.getMetaServerUris();
    TTransportException exception = null;
    for (int i = 0; !isConnected && i < retryLimit; i++) {
      try {
        URI msUri = new URI(uri);
        this.transport = new TSocket(msUri.getHost(), msUri.getPort());
        this.client = new ThriftHoodieMetaServer.Client(new TBinaryProtocol(transport));
        transport.open();
        this.isConnected = true;
        LOG.info("Connected to meta server: " + msUri);
      } catch (URISyntaxException e) {
        throw new HoodieException("Invalid meta server uri: " + uri, e);
      } catch (TTransportException e) {
        exception = e;
        LOG.warn("Fail to connect to the meta server.", e);
      }
    }
    if (!isConnected) {
      throw new HoodieException("Fail to connect to the meta server.", exception);
    }
  }

  private boolean isLocalEmbeddedMetaServer(String uri) {
    return uri == null ? true : uri.trim().isEmpty();
  }

  @Override
  public Table getTable(String db, String tb) {
    return exceptionWrapper(() -> this.client.get_table(db, tb)).get();
  }

  @Override
  public void createTable(Table table) {
    try {
      this.client.create_table(table);
    } catch (TException e) {
      throw new HoodieException(e);
    }
  }

  public List<HoodieInstant> listInstants(String db, String tb, int commitNum) {
    return exceptionWrapper(() -> this.client.list_instants(db, tb, commitNum).stream()
        .map(EntryConvertor::fromTHoodieInstant)
        .collect(Collectors.toList())).get();
  }

  public Option<byte[]> getInstantMeta(String db, String tb, HoodieInstant instant) {
    ByteBuffer bytes = exceptionWrapper(() -> this.client.get_instant_meta(db, tb, EntryConvertor.toTHoodieInstant(instant))).get();
    Option<byte[]> res = bytes.capacity() == 0 ? Option.empty() : Option.of(bytes.array());
    return res;
  }

  public String createNewTimestamp(String db, String tb) {
    return exceptionWrapper(() -> this.client.create_new_instant_time(db, tb)).get();
  }

  public void createNewInstant(String db, String tb, HoodieInstant instant, Option<byte[]> content) {
    exceptionWrapper(() -> this.client.create_new_instant_with_time(db, tb, EntryConvertor.toTHoodieInstant(instant), getByteBuffer(content))).get();
  }

  public void transitionInstantState(String db, String tb, HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> content) {
    exceptionWrapper(() -> this.client.transition_instant_state(db, tb,
        EntryConvertor.toTHoodieInstant(fromInstant),
        EntryConvertor.toTHoodieInstant(toInstant),
        getByteBuffer(content))).get();
  }

  public void deleteInstant(String db, String tb, HoodieInstant instant) {
    exceptionWrapper(() -> this.client.delete_instant(db, tb, EntryConvertor.toTHoodieInstant(instant))).get();
  }

  // TODO: support snapshot creation
  public FileStatus[] listFilesInPartition(String db, String tb, String partition, String timestamp) {
    return null;
  }

  public List<String> listAllPartitions(String db, String tb) {
    return exceptionWrapper(() -> this.client.list_all_partitions(db, tb)).get();
  }

  private ByteBuffer getByteBuffer(Option<byte[]> content) {
    ByteBuffer byteBuffer;
    if (content.isPresent()) {
      byteBuffer = ByteBuffer.wrap(content.get());
    } else {
      byteBuffer = ByteBuffer.allocate(0);
    }
    return byteBuffer;
  }

  // used for test
  public boolean isLocal() {
    return isLocal;
  }

  public boolean isConnected() {
    return isConnected;
  }

  public void close() {
    isConnected = false;
    if (transport != null && transport.isOpen()) {
      transport.close();
    }
  }

  interface FunctionWithTException<R, E extends TException> {
    R get() throws E;
  }

  private <R, E extends TException> Supplier<R> exceptionWrapper(FunctionWithTException<R, E> f) {
    return () -> {
      try {
        return f.get();
      } catch (TException e) {
        throw new HoodieException(e);
      }
    };
  }
}
