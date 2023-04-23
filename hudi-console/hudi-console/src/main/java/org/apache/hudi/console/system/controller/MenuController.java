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

import org.apache.hudi.console.base.domain.RestResponse;
import org.apache.hudi.console.base.domain.router.VueRouter;
import org.apache.hudi.console.system.entity.Menu;
import org.apache.hudi.console.system.service.CommonService;
import org.apache.hudi.console.system.service.MenuService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
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

import java.util.ArrayList;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("/menu")
public class MenuController {

  @Autowired private MenuService menuService;

  @Autowired private CommonService commonService;

  @PostMapping("router")
  public RestResponse getUserRouters() {
    ArrayList<VueRouter<Menu>> routers = this.menuService.getUserRouters(commonService.getUserId());
    return RestResponse.success(routers);
  }

  @PostMapping("list")
  @RequiresPermissions("menu:view")
  public RestResponse menuList(Menu menu) {
    Map<String, Object> maps = this.menuService.findMenus(menu);
    return RestResponse.success(maps);
  }

  @PostMapping("post")
  @RequiresPermissions("menu:add")
  public RestResponse addMenu(@Valid Menu menu) {
    this.menuService.createMenu(menu);
    return RestResponse.success();
  }

  @DeleteMapping("delete")
  @RequiresPermissions("menu:delete")
  public RestResponse deleteMenus(@NotBlank(message = "{required}") String menuIds)
      throws Exception {
    String[] ids = menuIds.split(StringPool.COMMA);
    this.menuService.deleteMenus(ids);
    return RestResponse.success();
  }

  @PutMapping("update")
  @RequiresPermissions("menu:update")
  public RestResponse updateMenu(@Valid Menu menu) throws Exception {
    this.menuService.updateMenu(menu);
    return RestResponse.success();
  }
}
