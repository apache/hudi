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
@TableName("t_role")
public class Role implements Serializable {

  private static final long serialVersionUID = -1714476694755654924L;

  @TableId(type = IdType.AUTO)
  private Long roleId;

  @NotBlank(message = "{required}")
  @Size(max = 10, message = "{noMoreThan}")
  private String roleName;

  private String roleCode;

  @Size(max = 50, message = "{noMoreThan}")
  private String remark;

  private Date createTime;

  private Date modifyTime;

  private transient String sortField;

  private transient String sortOrder;

  private transient String createTimeFrom;
  private transient String createTimeTo;
  private transient String menuId;
}
