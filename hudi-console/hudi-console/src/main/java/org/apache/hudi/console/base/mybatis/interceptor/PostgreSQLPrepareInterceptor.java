/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.mybatis.interceptor;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Properties;

/** the mybatis interceptor for update/insert/delete. */
@Intercepts({
  @Signature(
      type = StatementHandler.class,
      method = "prepare",
      args = {Connection.class, Integer.class})
})
public class PostgreSQLPrepareInterceptor implements Interceptor {
  @Override
  public Object intercept(final Invocation invocation) throws Throwable {
    StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
    BoundSql boundSql = statementHandler.getBoundSql();
    Field field = boundSql.getClass().getDeclaredField("sql");
    field.setAccessible(true);
    field.set(boundSql, boundSql.getSql().replace("`", "\"").toLowerCase());
    return invocation.proceed();
  }

  @Override
  public Object plugin(final Object target) {
    return Plugin.wrap(target, this);
  }

  @Override
  public void setProperties(final Properties properties) {}
}
