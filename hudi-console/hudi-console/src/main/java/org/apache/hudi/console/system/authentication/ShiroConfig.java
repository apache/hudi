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

package org.apache.hudi.console.system.authentication;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.spring.security.interceptor.AuthorizationAttributeSourceAdvisor;
import org.apache.shiro.spring.web.ShiroFilterFactoryBean;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;

import java.util.LinkedHashMap;

@Configuration
public class ShiroConfig {

  @Bean
  public ShiroFilterFactoryBean shiroFilterFactoryBean(SecurityManager securityManager) {
    ShiroFilterFactoryBean shiroFilterFactoryBean = new ShiroFilterFactoryBean();
    shiroFilterFactoryBean.setSecurityManager(securityManager);

    LinkedHashMap<String, Filter> filters = new LinkedHashMap<>();
    filters.put("jwt", new JWTFilter());
    shiroFilterFactoryBean.setFilters(filters);

    LinkedHashMap<String, String> filterChainDefinitionMap = new LinkedHashMap<>();
    filterChainDefinitionMap.put("/actuator/**", "anon");

    filterChainDefinitionMap.put("/doc.html", "anon");
    filterChainDefinitionMap.put("/swagger-ui/**", "anon");
    filterChainDefinitionMap.put("/swagger-resources", "anon");
    filterChainDefinitionMap.put("/swagger-resources/configuration/security", "anon");
    filterChainDefinitionMap.put("/swagger-resources/configuration/ui", "anon");
    filterChainDefinitionMap.put("/v3/api-docs", "anon");
    filterChainDefinitionMap.put("/webjars/**", "anon");

    filterChainDefinitionMap.put("/passport/**", "anon");
    filterChainDefinitionMap.put("/systemName", "anon");
    filterChainDefinitionMap.put("/permissions/teams", "anon");
    filterChainDefinitionMap.put("/user/check/**", "anon");
    filterChainDefinitionMap.put("/user/initTeam", "anon");
    filterChainDefinitionMap.put("/websocket/**", "anon");
    filterChainDefinitionMap.put("/metrics/**", "anon");

    filterChainDefinitionMap.put("/index.html", "anon");
    filterChainDefinitionMap.put("/assets/**", "anon");
    filterChainDefinitionMap.put("/resource/**/**", "anon");
    filterChainDefinitionMap.put("/css/**", "anon");
    filterChainDefinitionMap.put("/fonts/**", "anon");
    filterChainDefinitionMap.put("/img/**", "anon");
    filterChainDefinitionMap.put("/js/**", "anon");
    filterChainDefinitionMap.put("/loading/**", "anon");
    filterChainDefinitionMap.put("/*.js", "anon");
    filterChainDefinitionMap.put("/*.png", "anon");
    filterChainDefinitionMap.put("/*.jpg", "anon");
    filterChainDefinitionMap.put("/*.less", "anon");
    filterChainDefinitionMap.put("/*.ico", "anon");
    filterChainDefinitionMap.put("/", "anon");
    filterChainDefinitionMap.put("/**", "jwt");

    shiroFilterFactoryBean.setFilterChainDefinitionMap(filterChainDefinitionMap);

    return shiroFilterFactoryBean;
  }

  @Bean
  public SecurityManager securityManager() {
    DefaultWebSecurityManager securityManager = new DefaultWebSecurityManager();
    securityManager.setRealm(shiroRealm());
    return securityManager;
  }

  @Bean
  public ShiroRealm shiroRealm() {
    return new ShiroRealm();
  }

  @Bean
  public AuthorizationAttributeSourceAdvisor authorizationAttributeSourceAdvisor(
      SecurityManager securityManager) {
    AuthorizationAttributeSourceAdvisor authorizationAttributeSourceAdvisor =
        new AuthorizationAttributeSourceAdvisor();
    authorizationAttributeSourceAdvisor.setSecurityManager(securityManager);
    return authorizationAttributeSourceAdvisor;
  }
}
