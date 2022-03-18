package org.apache.hudi.metaserver.store.jdbc;

import org.apache.hudi.metaserver.thrift.MetaStoreException;
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

import org.apache.ibatis.session.SqlSession;

import java.io.Serializable;
import java.util.List;

/**
 * A basic class provides the public method for DAO.
 */
public class BasicDao implements Serializable {
  public <T> List<T> queryForListBySql(String namespace, String sqlID, Object parameter) {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      return session.selectList(statement(namespace, sqlID), parameter);
    }
  }

  public <T> T queryForObjectBySql(String namespace, String sqlID, Object parameter) {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      return session.selectOne(statement(namespace, sqlID), parameter);
    }
  }

  public int insertBySql(String namespace, String sqlID, Object parameter) {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      int res = session.insert(statement(namespace, sqlID), parameter);
      session.commit();
      return res;
    }
  }

  public int deleteBySql(String namespace, String sqlID, Object parameter) {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      int res = session.delete(statement(namespace, sqlID), parameter);
      session.commit();
      return res;
    }
  }

  public int updateBySql(String namespace, String sqlID, Object parameter) {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      int res = session.update(statement(namespace, sqlID), parameter);
      session.commit();
      return res;
    }
  }

  public void batchOperateBySql(List<BatchDaoOperation> batchDaoOperations) throws MetaStoreException {
    try (SqlSession session = SqlSessionFactoryUtil.openSqlSession()) {
      for (BatchDaoOperation batchDaoOperation: batchDaoOperations) {
        switch (batchDaoOperation.getOperationType()) {
          case BatchDaoOperation.OPERATION_TYPE_INSERT:
            session.insert(statement(batchDaoOperation.getNamespace(), batchDaoOperation.getSqlID()), batchDaoOperation.getParameter());
            break;
          case BatchDaoOperation.OPERATION_TYPE_UPDATE:
            session.update(statement(batchDaoOperation.getNamespace(), batchDaoOperation.getSqlID()), batchDaoOperation.getParameter());
            break;
          case BatchDaoOperation.OPERATION_TYPE_DELETE:
            session.delete(statement(batchDaoOperation.getNamespace(), batchDaoOperation.getSqlID()), batchDaoOperation.getParameter());
            break;
          default:
            throw new MetaStoreException("Unsupported type: " + batchDaoOperation.getOperationType());
        }
      }
      session.commit();
    }
  }

  private String statement(String namespace, String sqlID) {
    return namespace + "." + sqlID;
  }
}
