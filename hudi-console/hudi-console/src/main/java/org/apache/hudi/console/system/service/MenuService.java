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

import org.apache.hudi.console.base.domain.router.VueRouter;
import org.apache.hudi.console.system.entity.Menu;

import com.baomidou.mybatisplus.extension.service.IService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface MenuService extends IService<Menu> {

  /**
   * Get the permissions of current userId.
   *
   * @param userId the user Id
   * @return permissions
   */
  List<String> findUserPermissions(Long userId);

  List<Menu> findUserMenus(Long userId);

  Map<String, Object> findMenus(Menu menu);

  void createMenu(Menu menu);

  void updateMenu(Menu menu) throws Exception;

  /**
   * Recursively delete menu buttons
   *
   * @param menuIds menuIds
   */
  void deleteMenus(String[] menuIds) throws Exception;

  ArrayList<VueRouter<Menu>> getUserRouters(Long userId);
}
