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
import org.apache.hudi.console.system.entity.Permissions;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.mapper.PermissionsMapper;
import org.apache.hudi.console.system.service.PermissionsService;
import org.apache.hudi.console.system.service.RoleService;
import org.apache.hudi.console.system.service.UserService;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class PermissionsServiceImpl extends ServiceImpl<PermissionsMapper, Permissions>
    implements PermissionsService {

  @Autowired private UserService userService;

  @Autowired private RoleService roleService;

  @Override
  @Transactional
  public void deleteByUserId(Long userId) {
    baseMapper.deleteByUserId(userId);
  }

  @Override
  public IPage<Permissions> findUsers(Permissions permissions, RestRequest request) {
    Page<Permissions> page = new Page<>();
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());
    return baseMapper.findUsers(page, permissions);
  }

  @Override
  public Permissions findByUserName(String userName) {
    User user = userService.findByName(userName);
    if (user == null) {
      return null;
    }
    return findByUserId(user.getUserId());
  }

  private Permissions findByUserId(Long userId) {
    LambdaQueryWrapper<Permissions> queryWrapper =
        new LambdaQueryWrapper<Permissions>().eq(Permissions::getUserId, userId);
    return baseMapper.selectOne(queryWrapper);
  }

  @Override
  public List<Long> findUserIdsByRoleId(Long roleId) {
    LambdaQueryWrapper<Permissions> queryWrapper =
        new LambdaQueryWrapper<Permissions>().eq(Permissions::getRoleId, roleId);
    List<Permissions> list = baseMapper.selectList(queryWrapper);
    return list.stream().map(Permissions::getUserId).collect(Collectors.toList());
  }

  @Override
  public void createPermissions(Permissions permissions) {
    User user =
        Optional.ofNullable(userService.findByName(permissions.getUserName()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The username [%s] not found", permissions.getUserName())));
    Optional.ofNullable(roleService.getById(permissions.getRoleId()))
        .orElseThrow(
            () ->
                new ApiAlertException(
                    String.format("The roleId [%s] not found", permissions.getRoleId())));

    permissions.setId(null);
    permissions.setUserId(user.getUserId());
    permissions.setCreateTime(new Date());
    this.save(permissions);
  }

  @Override
  public void deletePermissions(Permissions permissionsArg) {
    Permissions permissions =
        Optional.ofNullable(this.getById(permissionsArg.getId()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format(
                            "The permissions [id=%s] not found", permissionsArg.getId())));
    this.removeById(permissions);
  }

  @Override
  public void updatePermissions(Permissions permissions) {
    Permissions oldPermissions =
        Optional.ofNullable(this.getById(permissions.getId()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The permissions [id=%s] not found", permissions.getId())));
    CommonUtils.required(
        oldPermissions.getUserId().equals(permissions.getUserId()), "User id cannot be changed.");
    Optional.ofNullable(roleService.getById(permissions.getRoleId()))
        .orElseThrow(
            () ->
                new ApiAlertException(
                    String.format("The roleId [%s] not found", permissions.getRoleId())));
    oldPermissions.setRoleId(permissions.getRoleId());
    updateById(oldPermissions);
  }

  @Override
  public List<User> users() {
    return userService.list();
  }
}
