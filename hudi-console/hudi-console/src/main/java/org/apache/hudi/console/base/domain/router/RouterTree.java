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

package org.apache.hudi.console.base.domain.router;

import org.apache.hudi.console.system.entity.Menu;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RouterTree<T> {

  private String id;

  private String key;

  private String icon;

  private String title;

  private String value;

  private String text;

  private String permission;

  private String type;

  private boolean display;

  private Double order;

  private String path;

  private String component;

  private List<RouterTree<T>> children;

  private String parentId;

  private boolean hasParent = false;

  private boolean hasChildren = false;

  private Date createTime;

  private Date modifyTime;

  public void initChildren() {
    this.children = new ArrayList<>();
  }

  public RouterTree() {}

  public RouterTree(Menu menu) {
    this.setId(menu.getMenuId().toString());
    this.setKey(this.getId());
    this.setParentId(menu.getParentId().toString());
    this.setText(menu.getMenuName());
    this.setTitle(menu.getMenuName());
    this.setIcon(menu.getIcon());
    this.setComponent(menu.getComponent());
    this.setCreateTime(menu.getCreateTime());
    this.setModifyTime(menu.getModifyTime());
    this.setPath(menu.getPath());
    this.setOrder(menu.getOrderNum());
    this.setPermission(menu.getPerms());
    this.setType(menu.getType());
    this.setDisplay(menu.isDisplay());
  }
}
