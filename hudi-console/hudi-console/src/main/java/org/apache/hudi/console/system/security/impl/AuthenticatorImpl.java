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

package org.apache.hudi.console.system.security.impl;

import org.apache.hudi.console.base.util.ShaHashUtils;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.security.Authenticator;
import org.apache.hudi.console.system.service.UserService;

import org.apache.commons.lang3.StringUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthenticatorImpl implements Authenticator {
  @Autowired private UserService usersService;

  @Override
  public User authenticate(String username, String password) {
    User user = usersService.findByName(username);
    if (user == null) {
      return null;
    }
    String salt = user.getSalt();
    password = ShaHashUtils.encrypt(salt, password);
    if (!StringUtils.equals(user.getPassword(), password)) {
      return null;
    }
    return user;
  }
}
