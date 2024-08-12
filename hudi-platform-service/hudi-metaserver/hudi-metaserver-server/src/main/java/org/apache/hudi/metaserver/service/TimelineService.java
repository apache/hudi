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

package org.apache.hudi.metaserver.service;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.metaserver.store.MetaserverStorage;
import org.apache.hudi.metaserver.thrift.HoodieInstantChangeResult;
import org.apache.hudi.metaserver.thrift.MetaserverException;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.TAction;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;
import org.apache.hudi.metaserver.util.MetaserverTableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Handle all timeline / instant / instant meta related requests.
 */
public class TimelineService implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineService.class);
  private static final List<TAction> ALL_ACTIONS = Arrays.asList(TAction.COMMIT, TAction.DELTACOMMIT,
      TAction.CLEAN, TAction.ROLLBACK, TAction.SAVEPOINT, TAction.REPLACECOMMIT, TAction.COMPACTION, TAction.RESTORE);
  private static final List<TState> PENDING_STATES = Arrays.asList(TState.REQUESTED, TState.INFLIGHT);

  private final MetaserverStorage store;

  public TimelineService(MetaserverStorage metaserverStorage) {
    this.store = metaserverStorage;
  }

  public List<THoodieInstant> listInstants(String db, String tb, int num) throws MetaserverStorageException, NoSuchObjectException {
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    List<THoodieInstant> completeds = store.scanInstants(tableId, TState.COMPLETED, num);
    List<THoodieInstant> pendings = store.scanInstants(tableId, PENDING_STATES, -1);
    completeds.addAll(pendings);
    return completeds;
  }

  public ByteBuffer getInstantMetadata(String db, String tb, THoodieInstant instant) throws MetaserverStorageException, NoSuchObjectException {
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    Option<byte[]> instantMetadata = store.getInstantMetadata(tableId, instant);
    return ByteBuffer.wrap(instantMetadata.orElse(new byte[0]));
  }

  public String createNewInstantTime(String db, String tb) throws MetaserverStorageException, NoSuchObjectException {
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    return store.createNewTimestamp(tableId);
  }

  public HoodieInstantChangeResult createNewInstantWithTime(String db, String tb, THoodieInstant instant, ByteBuffer content) throws MetaserverStorageException, NoSuchObjectException {
    checkArgument(instant.getState().equals(TState.REQUESTED));
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    HoodieInstantChangeResult result = new HoodieInstantChangeResult();
    result.setInstant(instant);
    if (store.instantExists(tableId, instant)) {
      result.setSuccess(true);
      return result;
    }
    store.saveInstantMetadata(tableId, instant, content.array());
    result.setSuccess(store.createInstant(tableId, instant));
    return result;
  }

  public HoodieInstantChangeResult transitionInstantState(String db, String tb, THoodieInstant fromInstant, THoodieInstant toInstant, ByteBuffer metadata)
      throws MetaserverStorageException, NoSuchObjectException, MetaserverException {
    switch (fromInstant.getState()) {
      case REQUESTED:
        return transitionRequestedToInflight(db, tb, fromInstant, toInstant, metadata);
      case INFLIGHT:
        return transitionInflightToCompleted(db, tb, fromInstant, toInstant, metadata);
      default:
        throw new MetaserverException("Unsupported state " + fromInstant.getState() + " when do the state transition.");
    }
  }

  private HoodieInstantChangeResult transitionRequestedToInflight(String db, String tb, THoodieInstant fromInstant, THoodieInstant toInstant, ByteBuffer metadata)
      throws MetaserverStorageException, NoSuchObjectException {
    checkArgument(fromInstant.getState().equals(TState.REQUESTED));
    checkArgument(toInstant.getState().equals(TState.INFLIGHT));
    HoodieInstantChangeResult result = new HoodieInstantChangeResult();
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    if (store.instantExists(tableId, toInstant)) {
      LOG.info("Instant " + toInstant + " has been already changed to");
      result.setSuccess(true);
      return result;
    }
    // todo: add conflict check for inflight
    store.saveInstantMetadata(tableId, toInstant, metadata.array());
    result.setSuccess(store.updateInstant(tableId, fromInstant, toInstant));
    return result;
  }

  private HoodieInstantChangeResult transitionInflightToCompleted(String db, String tb, THoodieInstant fromInstant, THoodieInstant toInstant, ByteBuffer metadata)
      throws MetaserverStorageException, NoSuchObjectException {
    checkArgument(fromInstant.getState().equals(TState.INFLIGHT));
    checkArgument(toInstant.getState().equals(TState.COMPLETED));
    HoodieInstantChangeResult result = new HoodieInstantChangeResult();
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    if (store.instantExists(tableId, toInstant)) {
      LOG.info("Instant " + toInstant + " has been already changed to");
      result.setSuccess(true);
      return result;
    }
    // todo: add conflict check for completed
    store.saveInstantMetadata(tableId, toInstant, metadata.array());
    // todo: sync snapshot
    result.setSuccess(store.updateInstant(tableId, fromInstant, toInstant));
    return result;
  }

  public HoodieInstantChangeResult deleteInstant(String db, String tb, THoodieInstant instant) throws MetaserverStorageException, NoSuchObjectException {
    Long tableId = MetaserverTableUtils.getTableId(store, db, tb);
    HoodieInstantChangeResult result = new HoodieInstantChangeResult();
    if (store.instantExists(tableId, instant)) {
      if (instant.getState() == TState.COMPLETED) {
        store.deleteInstantAllMeta(tableId, instant.getTimestamp());
      }
      store.deleteInstant(tableId, instant);
    } else {
      LOG.info("Instant " + instant + " has been already deleted");
    }
    result.setSuccess(true);
    return result;
  }
}
