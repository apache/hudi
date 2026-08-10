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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metaserver.thrift.Table;

import java.io.Serializable;
import java.util.List;

/**
 * Hoodie meta server client, is to get/put instants, instant meta, snapshot from/to hoodie meta server.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface HoodieMetaserverClient extends Serializable, AutoCloseable {

  Table getTable(String db, String tb);

  void createTable(Table table);

  List<HoodieInstant> listInstants(String db, String tb, int commitNum);

  Option<byte[]> getInstantMetadata(String db, String tb, HoodieInstant instant);

  String createNewTimestamp(String db, String tb);

  void createNewInstant(String db, String tb, HoodieInstant instant, Option<byte[]> content);

  HoodieInstant transitionInstantState(String db, String tb, HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> content);

  void deleteInstant(String db, String tb, HoodieInstant instant);

  boolean isLocal();

  boolean isConnected();
}
