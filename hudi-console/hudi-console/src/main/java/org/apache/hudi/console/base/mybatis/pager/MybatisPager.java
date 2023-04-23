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

package org.apache.hudi.console.base.mybatis.pager;

import org.apache.hudi.console.base.domain.Constant;
import org.apache.hudi.console.base.domain.RestRequest;
import org.apache.hudi.console.base.util.WebUtils;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public final class MybatisPager<T> {

  public Page<T> getDefaultPage(RestRequest request) {
    return getPage(request, Constant.DEFAULT_SORT_FIELD, Constant.ORDER_DESC);
  }

  public Page<T> getPage(RestRequest request, String defaultSort, String defaultOrder) {
    Page<T> page = new Page<>();
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());

    List<OrderItem> orderItems = new ArrayList<>(0);
    if (StringUtils.isNotBlank(request.getSortField())
        && StringUtils.isNotBlank(request.getSortOrder())) {
      String sortField = WebUtils.camelToUnderscore(request.getSortField());
      if (StringUtils.equals(request.getSortOrder(), Constant.ORDER_DESC)) {
        orderItems.add(OrderItem.desc(sortField));
      } else {
        orderItems.add(OrderItem.asc(sortField));
      }
    } else {
      if (StringUtils.isNotBlank(defaultSort)) {
        if (StringUtils.equals(defaultOrder, Constant.ORDER_DESC)) {
          orderItems.add(OrderItem.desc(defaultSort));
        } else {
          orderItems.add(OrderItem.asc(defaultSort));
        }
      }
    }
    if (!orderItems.isEmpty()) {
      page.setOrders(orderItems);
    }
    return page;
  }
}
