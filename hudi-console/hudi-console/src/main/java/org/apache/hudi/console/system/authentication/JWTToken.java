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

import org.apache.shiro.authc.AuthenticationToken;

import lombok.Data;

/** JSON Web Token */
@Data
public class JWTToken implements AuthenticationToken {

  private static final long serialVersionUID = 1282057025599826155L;

  private String token;

  private String expireAt;

  public JWTToken(String token) {
    this.token = token;
  }

  public JWTToken(String token, String expireAt) {
    this.token = token;
    this.expireAt = expireAt;
  }

  @Override
  public Object getPrincipal() {
    return token;
  }

  @Override
  public Object getCredentials() {
    return token;
  }
}
