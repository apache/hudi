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

package org.apache.hudi.metaserver.store.jdbc;

public class BatchDaoOperation {

  public static final String OPERATION_TYPE_INSERT = "INSERT";
  public static final String OPERATION_TYPE_UPDATE = "UPDATE";
  public static final String OPERATION_TYPE_DELETE = "DELETE";

  private String namespace;
  private String sqlID;
  private Object parameter;
  private String operationType;

  public BatchDaoOperation(String namespace, String sqlID, Object parameter, String operationType) {
    this.namespace = namespace;
    this.sqlID = sqlID;
    this.parameter = parameter;
    this.operationType = operationType;
  }

  public BatchDaoOperation(String sqlID, Object parameter, String operationType) {
    this(null, sqlID, parameter, operationType);
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getSqlID() {
    return sqlID;
  }

  public void setSqlID(String sqlID) {
    this.sqlID = sqlID;
  }

  public Object getParameter() {
    return parameter;
  }

  public void setParameter(Object parameter) {
    this.parameter = parameter;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }
}
