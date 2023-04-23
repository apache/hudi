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

package org.apache.hudi.console.system.service.impl;

import org.apache.hudi.console.system.authentication.JWTUtil;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.service.CommonService;
import org.apache.hudi.console.system.service.UserService;

import org.apache.shiro.SecurityUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CommonServiceImpl implements CommonService {

  @Autowired private UserService userService;

  @Override
  public User getCurrentUser() {
    String token = (String) SecurityUtils.getSubject().getPrincipal();
    Long userId = JWTUtil.getUserId(token);
    return userService.getById(userId);
  }

  @Override
  public Long getUserId() {
    User user = getCurrentUser();
    if (user != null) {
      return user.getUserId();
    }
    return null;
  }
}
