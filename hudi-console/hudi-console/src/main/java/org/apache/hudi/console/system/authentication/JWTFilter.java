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

import org.apache.hudi.console.base.properties.ShiroProperties;
import org.apache.hudi.console.base.util.SpringContextUtils;
import org.apache.hudi.console.base.util.WebUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.UnauthorizedException;
import org.apache.shiro.web.filter.authc.BasicHttpAuthenticationFilter;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Slf4j
public class JWTFilter extends BasicHttpAuthenticationFilter {

  private static final String TOKEN = "Authorization";

  private final AntPathMatcher pathMatcher = new AntPathMatcher();

  @Override
  protected boolean isAccessAllowed(
      ServletRequest request, ServletResponse response, Object mappedValue)
      throws UnauthorizedException {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    ShiroProperties properties = SpringContextUtils.getBean(ShiroProperties.class);
    String[] anonUrl =
        StringUtils.splitByWholeSeparatorPreserveAllTokens(
            properties.getAnonUrl(), StringPool.COMMA);

    for (String u : anonUrl) {
      if (pathMatcher.match(u.trim(), httpServletRequest.getRequestURI())) {
        return true;
      }
    }

    if (isLoginAttempt(request, response)) {
      return executeLogin(request, response);
    }
    return false;
  }

  @Override
  protected boolean isLoginAttempt(ServletRequest request, ServletResponse response) {
    HttpServletRequest req = (HttpServletRequest) request;
    String token = req.getHeader(TOKEN);
    return token != null;
  }

  @Override
  protected boolean executeLogin(ServletRequest request, ServletResponse response) {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    String token = httpServletRequest.getHeader(TOKEN);
    JWTToken jwtToken = new JWTToken(WebUtils.decryptToken(token));
    try {
      getSubject(request, response).login(jwtToken);
      return true;
    } catch (Exception e) {
      log.error("Error in executeLogin, token {}, jwtToken {}", token, jwtToken, e);
      return false;
    }
  }

  /** cross-domain support */
  @Override
  protected boolean preHandle(ServletRequest request, ServletResponse response) throws Exception {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    HttpServletResponse httpServletResponse = (HttpServletResponse) response;
    httpServletResponse.setHeader(
        "Access-control-Allow-Origin", httpServletRequest.getHeader("Origin"));
    httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS,PUT,DELETE");
    httpServletResponse.setHeader(
        "Access-Control-Allow-Headers",
        httpServletRequest.getHeader("Access-Control-Request-Headers"));
    if (httpServletRequest.getMethod().equals(RequestMethod.OPTIONS.name())) {
      httpServletResponse.setStatus(HttpStatus.OK.value());
      return false;
    }
    boolean preHandleResult = super.preHandle(request, response);
    int httpStatus = httpServletResponse.getStatus();
    // avoid the browser to automatically pop up the authentication box when http status=401
    if (!preHandleResult && httpStatus == 401) {
      httpServletResponse.setHeader("WWW-Authenticate", null);
    }
    return preHandleResult;
  }
}
