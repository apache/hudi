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

import org.apache.hudi.metaserver.store.MetaserverStorage;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;

public class MetaserverTableUtils {

  public static Long getTableId(MetaserverStorage store, String databaseName, String tableName) throws NoSuchObjectException, MetaserverStorageException {
    Long tableId = store.getTableId(databaseName, tableName);
    if (tableId == null) {
      throw new NoSuchObjectException(databaseName + "." + tableName + " does not exist");
    }
    return tableId;
  }
}
