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
import org.apache.hudi.console.system.entity.Permissions;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.service.PermissionsService;

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

@Slf4j
@Validated
@RestController
@RequestMapping("permissions")
public class PermissionsController {

  @Autowired private PermissionsService permissionsService;

  @PostMapping("list")
  public RestResponse permissionsList(RestRequest restRequest, Permissions permissions) {
    IPage<Permissions> userList = permissionsService.findUsers(permissions, restRequest);
    return RestResponse.success(userList);
  }

  @PostMapping("users")
  public RestResponse candidateUsers() {
    List<User> userList = permissionsService.users();
    return RestResponse.success(userList);
  }

  @PostMapping("check/user")
  public RestResponse check(@NotBlank(message = "{required}") String userName) {
    Permissions result = this.permissionsService.findByUserName(userName);
    return RestResponse.success(result == null);
  }

  @PostMapping("post")
  @RequiresPermissions("permissions:add")
  public RestResponse create(@Valid Permissions permissions) {
    this.permissionsService.createPermissions(permissions);
    return RestResponse.success();
  }

  @DeleteMapping("delete")
  @RequiresPermissions("permissions:delete")
  public RestResponse delete(Permissions permissions) {
    this.permissionsService.deletePermissions(permissions);
    return RestResponse.success();
  }

  @PutMapping("update")
  @RequiresPermissions("permissions:update")
  public RestResponse update(Permissions permissions) {
    this.permissionsService.updatePermissions(permissions);
    return RestResponse.success();
  }
}
