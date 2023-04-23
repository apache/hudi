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

import org.apache.hudi.console.base.domain.Constant;
import org.apache.hudi.console.base.domain.RestRequest;
import org.apache.hudi.console.base.exception.ApiAlertException;
import org.apache.hudi.console.system.entity.Role;
import org.apache.hudi.console.system.entity.RoleMenu;
import org.apache.hudi.console.system.mapper.RoleMapper;
import org.apache.hudi.console.system.mapper.RoleMenuMapper;
import org.apache.hudi.console.system.service.PermissionsService;
import org.apache.hudi.console.system.service.RoleMenuService;
import org.apache.hudi.console.system.service.RoleService;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class RoleServiceImpl extends ServiceImpl<RoleMapper, Role> implements RoleService {

  @Autowired private RoleMenuMapper roleMenuMapper;

  @Autowired private PermissionsService permissionsService;

  @Autowired private RoleMenuService roleMenuService;

  @Override
  public IPage<Role> findRoles(Role role, RestRequest request) {
    Page<Role> page = new Page<>();
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());
    return this.baseMapper.findRole(page, role);
  }

  @Override
  public Role findByName(String roleName) {
    return baseMapper.selectOne(new LambdaQueryWrapper<Role>().eq(Role::getRoleName, roleName));
  }

  @Override
  public void createRole(Role role) {
    role.setCreateTime(new Date());
    this.save(role);

    String[] menuIds = role.getMenuId().split(StringPool.COMMA);
    setRoleMenus(role, menuIds);
  }

  @Override
  public void deleteRole(Long roleId) {
    Role role =
        Optional.ofNullable(this.getById(roleId))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("Role id [%s] not found. Delete role failed.", roleId)));
    List<Long> userIdsByRoleId = permissionsService.findUserIdsByRoleId(roleId);
    ApiAlertException.throwIfFalse(
        userIdsByRoleId == null || userIdsByRoleId.isEmpty(),
        String.format(
            "There are some users of role %s, delete role failed, please unbind it first.",
            role.getRoleName()));
    this.removeById(roleId);
    this.roleMenuService.deleteByRoleId(roleId);
  }

  @Override
  public void updateRole(Role role) {
    role.setModifyTime(new Date());
    baseMapper.updateById(role);
    LambdaQueryWrapper<RoleMenu> queryWrapper =
        new LambdaQueryWrapper<RoleMenu>().eq(RoleMenu::getRoleId, role.getRoleId());
    roleMenuMapper.delete(queryWrapper);

    String menuId = role.getMenuId();
    if (StringUtils.contains(menuId, Constant.APP_DETAIL_MENU_ID)
        && !StringUtils.contains(menuId, Constant.APP_MENU_ID)) {
      menuId = menuId + StringPool.COMMA + Constant.APP_MENU_ID;
    }
    String[] menuIds = menuId.split(StringPool.COMMA);
    setRoleMenus(role, menuIds);
  }

  private void setRoleMenus(Role role, String[] menuIds) {
    Arrays.stream(menuIds)
        .forEach(
            menuId -> {
              RoleMenu rm = new RoleMenu();
              rm.setMenuId(Long.valueOf(menuId));
              rm.setRoleId(role.getRoleId());
              this.roleMenuMapper.insert(rm);
            });
  }
}
