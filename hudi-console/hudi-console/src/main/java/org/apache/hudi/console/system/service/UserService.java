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

package org.apache.hudi.console.system.service;

import org.apache.hudi.console.base.domain.RestRequest;
import org.apache.hudi.console.system.authentication.JWTToken;
import org.apache.hudi.console.system.entity.User;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface UserService extends IService<User> {

  /**
   * find user by name
   *
   * @param username username
   * @return user
   */
  User findByName(String username);

  /**
   * find uer detail, contains basic info, role, department
   *
   * @param user user
   * @param restRequest queryRequest
   * @return IPage
   */
  IPage<User> findUserDetail(User user, RestRequest restRequest);

  /**
   * update login time
   *
   * @param username username
   */
  void updateLoginTime(String username) throws Exception;

  /**
   * create user
   *
   * @param user user
   */
  void createUser(User user) throws Exception;

  /**
   * update user
   *
   * @param user user
   */
  void updateUser(User user) throws Exception;

  /**
   * delete user
   *
   * @param userId user id
   */
  void deleteUser(Long userId) throws Exception;

  /**
   * update user
   *
   * @param user user
   */
  void updateProfile(User user) throws Exception;

  /**
   * update user avatar
   *
   * @param username name
   * @param avatar avatar
   */
  void updateAvatar(String username, String avatar) throws Exception;

  /**
   * update password
   *
   * @param user
   * @throws Exception
   */
  void updatePassword(User user) throws Exception;

  /**
   * reset password
   *
   * @param usernames user list
   */
  void resetPassword(String[] usernames) throws Exception;

  /**
   * Get the permissions of current userId.
   *
   * @param userId the user Id
   * @return permissions
   */
  Set<String> getPermissions(Long userId);

  List<User> getNoTokenUser();

  Map<String, Object> generateFrontendUserInfo(User user, JWTToken token);
}
