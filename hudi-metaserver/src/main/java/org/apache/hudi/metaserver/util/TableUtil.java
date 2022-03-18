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

package org.apache.hudi.metaserver.util;

import org.apache.hudi.metaserver.store.MetadataStore;
import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;

public class TableUtil {
  public static Long getTableId(String db, String tb, MetadataStore store) throws NoSuchObjectException, MetaStoreException {
    Long tableId = store.getTableId(db, tb);
    if (tableId == null) {
      throw new NoSuchObjectException(db + "." + tb + " does not exist");
    }
    return tableId;
  }
}
