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

package org.apache.hudi.console.system.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

import java.io.Serializable;
import java.util.Date;

@Data
@TableName("t_menu")
public class Menu implements Serializable {

  private static final long serialVersionUID = 7187628714679791771L;

  public static final String TYPE_MENU = "0";

  public static final String TYPE_BUTTON = "1";

  @TableId(type = IdType.AUTO)
  private Long menuId;

  private Long parentId;

  @NotBlank(message = "{required}")
  @Size(max = 20, message = "{noMoreThan}")
  private String menuName;

  @Size(max = 50, message = "{noMoreThan}")
  private String path;

  @Size(max = 100, message = "{noMoreThan}")
  private String component;

  @Size(max = 50, message = "{noMoreThan}")
  private String perms;

  private String icon;

  @NotBlank(message = "{required}")
  private String type;

  private Double orderNum;

  private Date createTime;

  private Date modifyTime;

  private boolean display;

  private transient String createTimeFrom;
  private transient String createTimeTo;
}
