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

package org.apache.hudi.metaserver.store.jdbc;

import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.ibatis.exceptions.PersistenceException;

import java.util.List;

/**
 * A wrapper class to handle exception thrown when do operations to database store.
 */
public class WrapperDao extends BasicDao {

  private final String namespace;

  public WrapperDao(String namespace) {
    this.namespace = namespace;
  }

  public <T> List<T> queryForListBySql(String sqlID, Object parameter) throws MetaStoreException {
    try {
      return queryForListBySql(namespace, sqlID, parameter);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public <T> T queryForObjectBySql(String sqlID, Object parameter) throws MetaStoreException {
    try {
      return queryForObjectBySql(namespace, sqlID, parameter);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public int insertBySql(String sqlID, Object parameter) throws MetaStoreException {
    try {
      return insertBySql(namespace, sqlID, parameter);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public int deleteBySql(String sqlID, Object parameter) throws MetaStoreException {
    try {
      return deleteBySql(namespace, sqlID, parameter);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public int updateBySql(String sqlID, Object parameter) throws MetaStoreException {
    try {
      return updateBySql(namespace, sqlID, parameter);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public void batchOperateBySql(List<BatchDaoOperation> batchDaoOperations) throws MetaStoreException {
    try {
      batchDaoOperations.forEach(x -> {
        if (x.getNamespace() == null) {
          x.setNamespace(namespace);
        }
      });
      super.batchOperateBySql(batchDaoOperations);
    } catch (PersistenceException e) {
      throw new MetaStoreException(e.getMessage());
    }
  }

  public static class TableDao extends WrapperDao {
    public TableDao() {
      super("TableMapper");
    }
  }

  public static class PartitionDao extends WrapperDao {
    public PartitionDao() {
      super("PartitionMapper");
    }
  }

  public static class TimelineDao extends WrapperDao {
    public TimelineDao() {
      super("TimelineMapper");
    }
  }

  public static class FileDao extends WrapperDao {
    public FileDao() {
      super("FileMapper");
    }
  }
}
