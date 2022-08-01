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

package org.apache.hudi.table.service.manager.store.jdbc;

import java.util.List;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

public class JdbcMapper {

  public int saveObject(String sqlId, Object object) {
    try (SqlSession sqlSession = SqlSessionFactoryUtil.openSqlSession()) {
      int res = sqlSession.insert(sqlId, object);
      sqlSession.commit();
      return res;
    }
  }

  public int updateObject(String sqlId, Object params) {
    try (SqlSession sqlSession = SqlSessionFactoryUtil.openSqlSession()) {
      int res = sqlSession.update(sqlId, params);
      sqlSession.commit();
      return res;
    }
  }

  public <T> T getObject(String sqlId, Object params) {
    try (SqlSession sqlSession = SqlSessionFactoryUtil.openSqlSession()) {
      return sqlSession.selectOne(sqlId, params);
    }
  }

  public <T> List<T> getObjects(String sqlId, Object params) {
    try (SqlSession sqlSession = SqlSessionFactoryUtil.openSqlSession()) {
      return sqlSession.selectList(sqlId, params);
    }
  }

  public <T> List<T> getObjects(String sqlId, Object params, RowBounds rowBounds) {
    try (SqlSession sqlSession = SqlSessionFactoryUtil.openSqlSession()) {
      return sqlSession.selectList(sqlId, params, rowBounds);
    }
  }
}
