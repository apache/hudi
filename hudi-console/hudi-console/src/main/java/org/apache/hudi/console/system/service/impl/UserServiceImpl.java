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

import org.apache.hudi.console.base.domain.RestRequest;
import org.apache.hudi.console.base.exception.ApiAlertException;
import org.apache.hudi.console.base.util.CommonUtils;
import org.apache.hudi.console.base.util.ShaHashUtils;
import org.apache.hudi.console.system.authentication.JWTToken;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.mapper.UserMapper;
import org.apache.hudi.console.system.service.MenuService;
import org.apache.hudi.console.system.service.PermissionsService;
import org.apache.hudi.console.system.service.UserService;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {

  @Autowired private PermissionsService permissionsService;

  @Autowired private MenuService menuService;

  @Override
  public User findByName(String username) {
    LambdaQueryWrapper<User> queryWrapper =
        new LambdaQueryWrapper<User>().eq(User::getUsername, username);
    return baseMapper.selectOne(queryWrapper);
  }

  @Override
  public IPage<User> findUserDetail(User user, RestRequest request) {
    Page<User> page = new Page<>();
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());
    IPage<User> resPage = this.baseMapper.findUserDetail(page, user);

    CommonUtils.notNull(resPage);
    if (resPage.getTotal() == 0) {
      resPage.setRecords(Collections.emptyList());
    }
    return resPage;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updateLoginTime(String username) {
    User user = new User();
    user.setLastLoginTime(new Date());
    LambdaQueryWrapper<User> queryWrapper =
        new LambdaQueryWrapper<User>().eq(User::getUsername, username);
    this.baseMapper.update(user, queryWrapper);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void createUser(User user) {
    user.setCreateTime(new Date());
    user.setAvatar(User.DEFAULT_AVATAR);
    String salt = ShaHashUtils.getRandomSalt();
    String password = ShaHashUtils.encrypt(salt, user.getPassword());
    user.setSalt(salt);
    user.setPassword(password);
    save(user);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updateUser(User user) {
    user.setPassword(null);
    user.setModifyTime(new Date());
    updateById(user);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteUser(Long userId) {
    removeById(userId);
    this.permissionsService.deleteByUserId(userId);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updateProfile(User user) {
    updateById(user);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updateAvatar(String username, String avatar) {
    User user = new User();
    user.setAvatar(avatar);
    LambdaQueryWrapper<User> queryWrapper =
        new LambdaQueryWrapper<User>().eq(User::getUsername, username);
    this.baseMapper.update(user, queryWrapper);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updatePassword(User userParam) {
    User user = getById(userParam.getUserId());
    ApiAlertException.throwIfNull(user, "User is null. Update password failed.");

    String saltPassword = ShaHashUtils.encrypt(user.getSalt(), userParam.getOldPassword());
    ApiAlertException.throwIfFalse(
        StringUtils.equals(user.getPassword(), saltPassword),
        "Old password error. Update password failed.");

    String salt = ShaHashUtils.getRandomSalt();
    String password = ShaHashUtils.encrypt(salt, userParam.getPassword());
    user.setSalt(salt);
    user.setPassword(password);
    LambdaQueryWrapper<User> queryWrapper =
        new LambdaQueryWrapper<User>().eq(User::getUserId, user.getUserId());
    this.baseMapper.update(user, queryWrapper);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void resetPassword(String[] usernames) {
    for (String username : usernames) {
      User user = new User();
      String salt = ShaHashUtils.getRandomSalt();
      String password = ShaHashUtils.encrypt(salt, User.DEFAULT_PASSWORD);
      user.setSalt(salt);
      user.setPassword(password);
      LambdaQueryWrapper<User> queryWrapper =
          new LambdaQueryWrapper<User>().eq(User::getUsername, username);
      this.baseMapper.update(user, queryWrapper);
    }
  }

  @Override
  public Set<String> getPermissions(Long userId) {
    List<String> userPermissions = this.menuService.findUserPermissions(userId);
    return new HashSet<>(userPermissions);
  }

  @Override
  public List<User> getNoTokenUser() {
    List<User> users = this.baseMapper.getNoTokenUser();
    if (!users.isEmpty()) {
      users.forEach(User::dataMasking);
    }
    return users;
  }

  /**
   * generate user info, contains: 1.token, 2.vue router, 3.role, 4.permission, 5.personalized
   * config info of frontend
   *
   * @param user user
   * @return UserInfo
   */
  @Override
  public Map<String, Object> generateFrontendUserInfo(User user, JWTToken token) {
    Map<String, Object> userInfo = new HashMap<>(8);

    // 1) token & expire
    if (token != null) {
      userInfo.put("token", token.getToken());
      userInfo.put("expire", token.getExpireAt());
    }

    // 2) user
    user.dataMasking();
    userInfo.put("user", user);

    // 3) permissions
    Set<String> permissions = this.getPermissions(user.getUserId());
    userInfo.put("permissions", permissions);

    return userInfo;
  }
}
