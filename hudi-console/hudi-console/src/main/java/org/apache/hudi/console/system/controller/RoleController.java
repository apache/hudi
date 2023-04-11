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

package org.apache.hudi.console.system.controller;

import org.apache.hudi.console.base.domain.RestRequest;
import org.apache.hudi.console.base.domain.RestResponse;
import org.apache.hudi.console.system.entity.Role;
import org.apache.hudi.console.system.entity.RoleMenu;
import org.apache.hudi.console.system.service.RoleMenuService;
import org.apache.hudi.console.system.service.RoleService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Validated
@RestController
@RequestMapping("role")
public class RoleController {

  @Autowired private RoleService roleService;
  @Autowired private RoleMenuService roleMenuService;

  @PostMapping("list")
  @RequiresPermissions("role:view")
  public RestResponse roleList(RestRequest restRequest, Role role) {
    IPage<Role> roleList = roleService.findRoles(role, restRequest);
    return RestResponse.success(roleList);
  }

  @PostMapping("check/name")
  public RestResponse checkRoleName(@NotBlank(message = "{required}") String roleName) {
    Role result = this.roleService.findByName(roleName);
    return RestResponse.success(result == null);
  }

  @PostMapping("menu")
  public RestResponse getRoleMenus(@NotBlank(message = "{required}") String roleId) {
    List<RoleMenu> list = this.roleMenuService.getByRoleId(roleId);
    List<String> roleMenus =
        list.stream()
            .map(roleMenu -> String.valueOf(roleMenu.getMenuId()))
            .collect(Collectors.toList());
    return RestResponse.success(roleMenus);
  }

  @PostMapping("post")
  @RequiresPermissions("role:add")
  public RestResponse addRole(@Valid Role role) {
    this.roleService.createRole(role);
    return RestResponse.success();
  }

  @DeleteMapping("delete")
  @RequiresPermissions("role:delete")
  public RestResponse deleteRole(Long roleId) {
    this.roleService.deleteRole(roleId);
    return RestResponse.success();
  }

  @PutMapping("update")
  @RequiresPermissions("role:update")
  public RestResponse updateRole(Role role) throws Exception {
    this.roleService.updateRole(role);
    return RestResponse.success();
  }
}
