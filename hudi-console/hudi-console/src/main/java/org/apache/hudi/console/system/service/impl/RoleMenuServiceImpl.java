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

import org.apache.hudi.console.system.entity.RoleMenu;
import org.apache.hudi.console.system.mapper.RoleMenuMapper;
import org.apache.hudi.console.system.service.RoleMenuService;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class RoleMenuServiceImpl extends ServiceImpl<RoleMenuMapper, RoleMenu>
    implements RoleMenuService {

  @Override
  @Transactional
  public void deleteByRoleId(Long roleId) {
    LambdaQueryWrapper<RoleMenu> queryWrapper =
        new LambdaQueryWrapper<RoleMenu>().eq(RoleMenu::getRoleId, roleId);
    baseMapper.delete(queryWrapper);
  }

  @Override
  @Transactional
  public void deleteByMenuId(String[] menuIds) {
    List<String> list = Arrays.asList(menuIds);
    LambdaQueryWrapper<RoleMenu> queryWrapper =
        new LambdaQueryWrapper<RoleMenu>().in(RoleMenu::getMenuId, list);
    baseMapper.delete(queryWrapper);
  }

  @Override
  public List<RoleMenu> getByRoleId(String roleId) {
    LambdaQueryWrapper<RoleMenu> queryWrapper =
        new LambdaQueryWrapper<RoleMenu>().eq(RoleMenu::getRoleId, roleId);
    return baseMapper.selectList(queryWrapper);
  }
}
