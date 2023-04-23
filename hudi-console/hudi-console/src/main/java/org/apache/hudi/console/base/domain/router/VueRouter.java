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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Build Vue Router */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VueRouter<T> implements Serializable {

  private static final long serialVersionUID = -3327478146308500708L;

  @JsonIgnore private String id;

  @JsonIgnore private String parentId;

  private String path;

  private String name;

  private String component;

  private String redirect;

  private RouterMeta meta;

  private List<VueRouter<T>> children;

  @JsonIgnore private boolean hasParent = false;

  @JsonIgnore private boolean hasChildren = false;

  public void initChildren() {
    this.children = new ArrayList<>();
  }
}
