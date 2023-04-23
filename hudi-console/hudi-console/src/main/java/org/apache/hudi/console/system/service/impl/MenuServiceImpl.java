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

import org.apache.hudi.console.base.domain.router.RouterMeta;
import org.apache.hudi.console.base.domain.router.RouterTree;
import org.apache.hudi.console.base.domain.router.VueRouter;
import org.apache.hudi.console.base.emuns.UserType;
import org.apache.hudi.console.base.util.VueRouterUtils;
import org.apache.hudi.console.system.entity.Menu;
import org.apache.hudi.console.system.entity.User;
import org.apache.hudi.console.system.mapper.MenuMapper;
import org.apache.hudi.console.system.service.MenuService;
import org.apache.hudi.console.system.service.RoleMenuService;
import org.apache.hudi.console.system.service.UserService;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class MenuServiceImpl extends ServiceImpl<MenuMapper, Menu> implements MenuService {

  @Autowired private UserService userService;

  @Autowired private RoleMenuService roleMenuService;

  @Override
  public List<String> findUserPermissions(Long userId) {
    User user =
        Optional.ofNullable(userService.getById(userId))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("The userId [%s] not found", userId)));
    // Admin has the permission for all menus.
    if (UserType.ADMIN.equals(user.getUserType())) {
      return this.list().stream().map(Menu::getPerms).collect(Collectors.toList());
    }
    return this.baseMapper.findUserPermissions(userId);
  }

  @Override
  public List<Menu> findUserMenus(Long userId) {
    User user =
        Optional.ofNullable(userService.getById(userId))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("The userId:[%s] not found", userId)));
    // Admin has the permission for all menus.
    if (UserType.ADMIN.equals(user.getUserType())) {
      LambdaQueryWrapper<Menu> queryWrapper =
          new LambdaQueryWrapper<Menu>().eq(Menu::getType, "0").orderByAsc(Menu::getOrderNum);
      return this.list(queryWrapper);
    }
    return this.baseMapper.findUserMenus(userId);
  }

  @Override
  public Map<String, Object> findMenus(Menu menu) {
    Map<String, Object> result = new HashMap<>(16);
    try {
      LambdaQueryWrapper<Menu> queryWrapper = new LambdaQueryWrapper<>();
      if (StringUtils.isNotBlank(menu.getMenuName())) {
        queryWrapper.eq(Menu::getMenuName, menu.getMenuName());
      }
      if (StringUtils.isNotBlank(menu.getCreateTimeFrom())
          && StringUtils.isNotBlank(menu.getCreateTimeTo())) {
        queryWrapper
            .ge(Menu::getCreateTime, menu.getCreateTimeFrom())
            .le(Menu::getCreateTime, menu.getCreateTimeTo());
      }
      List<Menu> menus = baseMapper.selectList(queryWrapper);

      List<RouterTree<Menu>> trees = new ArrayList<>();
      List<String> ids = new ArrayList<>();

      menus.forEach(
          m -> {
            ids.add(m.getMenuId().toString());
            trees.add(new RouterTree(m));
          });
      result.put("ids", ids);
      result.put("total", menus.size());
      RouterTree<Menu> routerTree = VueRouterUtils.buildRouterTree(trees);
      result.put("rows", routerTree);
    } catch (Exception e) {
      log.error("Failed to query menu", e);
      result.put("rows", null);
      result.put("total", 0);
    }
    return result;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void createMenu(Menu menu) {
    menu.setCreateTime(new Date());
    setMenu(menu);
    this.save(menu);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void updateMenu(Menu menu) throws Exception {
    menu.setModifyTime(new Date());
    setMenu(menu);
    baseMapper.updateById(menu);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteMenus(String[] menuIds) throws Exception {
    // Find users associated with these menus/buttons
    this.roleMenuService.deleteByMenuId(menuIds);
    // Recursively delete these menus/buttons
    this.removeByIds(Arrays.asList(menuIds));
  }

  @Override
  public ArrayList<VueRouter<Menu>> getUserRouters(Long userId) {
    List<VueRouter<Menu>> routes = new ArrayList<>();
    // The query type is the menu type
    List<Menu> menus = this.findUserMenus(userId);
    menus.forEach(
        menu -> {
          VueRouter<Menu> route = new VueRouter<>();
          route.setId(menu.getMenuId().toString());
          route.setParentId(menu.getParentId().toString());
          route.setPath(menu.getPath());
          route.setComponent(menu.getComponent());
          route.setName(menu.getMenuName());
          route.setMeta(new RouterMeta(true, !menu.isDisplay(), true, menu.getIcon()));
          routes.add(route);
        });
    return VueRouterUtils.buildVueRouter(routes);
  }

  private void setMenu(Menu menu) {
    if (menu.getParentId() == null) {
      menu.setParentId(0L);
    }
    if (Menu.TYPE_BUTTON.equals(menu.getType())) {
      menu.setPath(null);
      menu.setIcon(null);
      menu.setComponent(null);
    }
  }
}
