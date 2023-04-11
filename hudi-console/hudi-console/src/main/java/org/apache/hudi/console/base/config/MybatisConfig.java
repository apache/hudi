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

package org.apache.hudi.console.base.config;

import org.apache.hudi.console.base.mybatis.interceptor.PostgreSQLPrepareInterceptor;
import org.apache.hudi.console.base.mybatis.interceptor.PostgreSQLQueryInterceptor;

import org.apache.ibatis.type.JdbcType;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusPropertiesCustomizer;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.core.toolkit.GlobalConfigUtils;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** for MyBatis Configure management. */
@Configuration
@MapperScan(value = {"org.apache.hudi.console.*.mapper"})
public class MybatisConfig {

  @Bean
  public MybatisPlusInterceptor mybatisPlusInterceptor() {
    MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
    interceptor.addInnerInterceptor(new PaginationInnerInterceptor());
    return interceptor;
  }

  /**
   * Add the plugin to the MyBatis plugin interceptor chain.
   *
   * @return {@linkplain PostgreSQLQueryInterceptor}
   */
  @Bean
  @ConditionalOnProperty(name = "spring.profiles.active", havingValue = "pgsql")
  public PostgreSQLQueryInterceptor postgreSQLQueryInterceptor() {
    return new PostgreSQLQueryInterceptor();
  }

  /**
   * Add the plugin to the MyBatis plugin interceptor chain.
   *
   * @return {@linkplain PostgreSQLPrepareInterceptor}
   */
  @Bean
  @ConditionalOnProperty(name = "spring.profiles.active", havingValue = "pgsql")
  public PostgreSQLPrepareInterceptor postgreSQLPrepareInterceptor() {
    return new PostgreSQLPrepareInterceptor();
  }

  /**
   * mybatis plus setting
   *
   * @return MybatisPlusPropertiesCustomizer
   */
  @Bean
  public MybatisPlusPropertiesCustomizer mybatisPlusPropertiesCustomizer() {
    return properties -> {
      properties.setTypeAliasesPackage("org.apache.hudi.console.*.entity");
      properties.setMapperLocations(new String[] {"classpath:mapper/*/*.xml"});
      MybatisConfiguration mybatisConfiguration = new MybatisConfiguration();
      mybatisConfiguration.setJdbcTypeForNull(JdbcType.NULL);
      properties.setConfiguration(mybatisConfiguration);
      GlobalConfig globalConfig = GlobalConfigUtils.getGlobalConfig(mybatisConfiguration);
      GlobalConfig.DbConfig dbConfig = globalConfig.getDbConfig();
      dbConfig.setIdType(IdType.AUTO);
      // close mybatis-plus banner
      globalConfig.setBanner(false);
      properties.setGlobalConfig(globalConfig);
    };
  }
}
