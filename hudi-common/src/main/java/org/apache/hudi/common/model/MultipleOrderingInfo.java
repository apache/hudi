/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MultipleOrderingVal2ColsInfo
 * let each sub source has its own ordering field,
 * for example:
 * _ts1:name1,price1;_ts2:name2,price2
 * then every sub source won't lose its ordering value in target table
 */
public class MultipleOrderingInfo {
  private List<OrderingVal2ColsInfo> orderingVal2ColsInfoList = new ArrayList<>();
  private GenericRecord record;

  protected MultipleOrderingInfo(String multipleOrderingFieldsWithColsText) {
    this(multipleOrderingFieldsWithColsText, null);
  }

  protected MultipleOrderingInfo(String multipleOrderingFieldsWithColsText, GenericRecord record) {
    this.record = record;
    for (String orderingFieldWithColsText : multipleOrderingFieldsWithColsText.split(";")) {
      if (orderingFieldWithColsText == null || orderingFieldWithColsText.isEmpty()) {
        continue;
      }
      OrderingVal2ColsInfo orderingVal2ColsInfo = new OrderingVal2ColsInfo(orderingFieldWithColsText);
      orderingVal2ColsInfoList.add(orderingVal2ColsInfo);
    }
  }

  public static MultipleOrderingInfo.Builder newBuilder() {
    return new Builder();
  }

  public List<OrderingVal2ColsInfo> getOrderingVal2ColsInfoList() {
    return orderingVal2ColsInfoList;
  }

  public class OrderingVal2ColsInfo {
    private String orderingField;
    private Comparable orderingValue;
    private List<String> columnNames;

    public OrderingVal2ColsInfo(String orderingFieldWithColsText) {
      String[] orderInfo2ColsArr = orderingFieldWithColsText.split(":");
      String[] columnArr = orderInfo2ColsArr[1].split(",");
      this.orderingField = orderInfo2ColsArr[0];
      if (record != null) {
        this.orderingValue = (Comparable) HoodieAvroUtils.getNestedFieldVal(record, this.orderingField, true, false); //Long.parseLong(orderingField2Value[1]);
      }
      this.columnNames = Arrays.asList(columnArr);
    }

    public String getOrderingField() {
      return orderingField;
    }

    public Comparable getOrderingValue() {
      return orderingValue;
    }

    public void setOrderingValue(Comparable value) {
      this.orderingValue = value;
    }

    public List<String> getColumnNames() {
      return columnNames;
    }
  }

  public static class Builder {
    private String multipleOrderingConfig;
    private GenericRecord record;

    public Builder withMultipleOrderingConfig(String multipleOrderingConfig) {
      this.multipleOrderingConfig = multipleOrderingConfig;
      return this;
    }

    public Builder withGenericRecord(GenericRecord record) {
      this.record = record;
      return this;
    }

    public MultipleOrderingInfo build() {
      return new MultipleOrderingInfo(this.multipleOrderingConfig, this.record);
    }
  }
}
