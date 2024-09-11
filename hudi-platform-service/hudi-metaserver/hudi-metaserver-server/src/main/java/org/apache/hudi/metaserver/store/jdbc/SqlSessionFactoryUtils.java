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

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utils for sql session's life cycle.
 */
public class SqlSessionFactoryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessionFactoryUtils.class);
  private static final String CONFIG_PATH = "mybatis-config.xml";
  private static volatile SqlSessionFactory sqlSessionFactory;

  private SqlSessionFactoryUtils() {

  }

  private static void initSqlSessionFactory() {
    if (sqlSessionFactory == null) {
      synchronized (SqlSessionFactoryUtils.class) {
        if (sqlSessionFactory == null) {
          try (InputStream inputStream = Resources.getResourceAsStream(CONFIG_PATH)) {
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
          } catch (IOException e) {
            LOG.error("Failed to init SQL session.", e);
          }
        }
      }
    }
  }

  public static SqlSession openSqlSession() {
    initSqlSessionFactory();
    return sqlSessionFactory.openSession();
  }
}
