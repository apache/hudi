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

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaserver;
import org.apache.hudi.metaserver.util.EntityConversions;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * HoodieMetaserverClientImp based on thrift.
 */
public class HoodieMetaserverClientImp implements HoodieMetaserverClient {

  private static final Logger LOG =  LoggerFactory.getLogger(HoodieMetaserverClientImp.class);
  private final HoodieMetaserverConfig config;
  private final int retryLimit;
  private final long retryDelayMs;
  private boolean isConnected;
  private boolean isLocal;
  private final ThriftHoodieMetaserver.Iface client;
  private TTransport transport;
  private final DefaultInstantGenerator instantGenerator = new DefaultInstantGenerator();

  public HoodieMetaserverClientImp(HoodieMetaserverConfig config) {
    this.config = config;
    this.retryLimit = config.getConnectionRetryLimit();
    this.retryDelayMs = config.getConnectionRetryDelay() * 1000L;
    String uri = config.getMetaserverUris();
    if (isLocalEmbeddedMetaserver(uri)) {
      try {
        this.client = (ThriftHoodieMetaserver.Iface) ReflectionUtils.invokeStaticMethod("org.apache.hudi.metaserver.HoodieMetaserver",
            "getEmbeddedMetaserver", new Object[]{}, new Class[]{});
      } catch (HoodieException e) {
        throw new HoodieException("Please check the server uri has ever been set. Empty uri is used for local unit test", e);
      }
      this.isConnected = true;
      this.isLocal = true;
    } else {
      URI msUri = URI.create(uri);
      this.transport = new TSocket(msUri.getHost(), msUri.getPort());
      this.client = new ThriftHoodieMetaserver.Client(new TBinaryProtocol(transport));
      try {
        new RetryHelper<Void, TTransportException>(retryDelayMs, retryLimit, retryDelayMs, TTransportException.class.getName())
            .tryWith(() -> {
              transport.open();
              this.isConnected = true;
              LOG.info("Connected to meta server: " + msUri);
              return null;
            }).start();
      } catch (TTransportException e) {
        throw new HoodieException("Fail to connect to the metaserver.", e);
      }
    }
  }

  private boolean isLocalEmbeddedMetaserver(String uri) {
    return uri == null || uri.trim().isEmpty();
  }

  @Override
  public Table getTable(String db, String tb) {
    return exceptionWrapper(() -> this.client.getTable(db, tb)).get();
  }

  @Override
  public void createTable(Table table) {
    try {
      this.client.createTable(table);
    } catch (TException e) {
      throw new HoodieException(e);
    }
  }

  @Override
  public List<HoodieInstant> listInstants(String db, String tb, int commitNum) {
    return exceptionWrapper(() -> this.client.listInstants(db, tb, commitNum).stream()
        .map(instant -> EntityConversions.fromTHoodieInstant(instant, instantGenerator))
        .sorted(Comparator.comparing(HoodieInstant::requestedTime).reversed())
        .collect(Collectors.toList())).get();
  }

  @Override
  public Option<byte[]> getInstantMetadata(String db, String tb, HoodieInstant instant) {
    ByteBuffer byteBuffer = exceptionWrapper(() -> this.client.getInstantMetadata(db, tb, EntityConversions.toTHoodieInstant(instant))).get();
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes.length > 0 ? Option.of(bytes) : Option.empty();
  }

  @Override
  public String createNewTimestamp(String db, String tb) {
    return exceptionWrapper(() -> this.client.createNewInstantTime(db, tb)).get();
  }

  @Override
  public void createNewInstant(String db, String tb, HoodieInstant instant, Option<byte[]> content) {
    exceptionWrapper(() -> this.client.createNewInstantWithTime(db, tb, EntityConversions.toTHoodieInstant(instant), getByteBuffer(content))).get();
  }

  @Override
  public HoodieInstant transitionInstantState(String db, String tb, HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> content) {
    exceptionWrapper(() -> this.client.transitionInstantState(db, tb,
        EntityConversions.toTHoodieInstant(fromInstant),
        EntityConversions.toTHoodieInstant(toInstant),
        getByteBuffer(content))).get();
    return toInstant;
  }

  @Override
  public void deleteInstant(String db, String tb, HoodieInstant instant) {
    exceptionWrapper(() -> this.client.deleteInstant(db, tb, EntityConversions.toTHoodieInstant(instant))).get();
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
  @Override
  public boolean isLocal() {
    return isLocal;
  }

  @Override
  public boolean isConnected() {
    return isConnected;
  }

  @Override
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
