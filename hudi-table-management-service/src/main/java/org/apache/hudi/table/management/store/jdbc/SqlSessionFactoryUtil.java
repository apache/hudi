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

package org.apache.hudi.table.management.store.jdbc;

import org.apache.hudi.table.management.exception.HoodieTableManagementException;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.util.stream.Collectors;

public class SqlSessionFactoryUtil {

  private static final String CONFIG_PATH = "mybatis-config.xml";

  private static SqlSessionFactory sqlSessionFactory;
  private static final Class<?> CLASS_LOCK = SqlSessionFactoryUtil.class;

  private SqlSessionFactoryUtil() {

  }

  public static void initSqlSessionFactory() {
    try (InputStream inputStream = Resources.getResourceAsStream(CONFIG_PATH)) {
      synchronized (CLASS_LOCK) {
        if (sqlSessionFactory == null) {
          sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static SqlSession openSqlSession() {
    if (sqlSessionFactory == null) {
      initSqlSessionFactory();
      init();
    }
    return sqlSessionFactory.openSession();
  }

  public static void init() {
    try {
      String[] ddls = org.apache.commons.io.IOUtils.readLines(
              SqlSessionFactoryUtil.class.getResourceAsStream("/table-management-service.sql"))
          .stream().filter(e -> !e.startsWith("--"))
          .collect(Collectors.joining(""))
          .split(";");
      for (String ddl : ddls) {
        try (PreparedStatement statement = SqlSessionFactoryUtil.openSqlSession().getConnection()
            .prepareStatement(ddl)) {
          statement.execute();
        }
      }
    } catch (Exception e) {
      throw new HoodieTableManagementException("Unable to read init ddl file", e);
    }
  }

}
