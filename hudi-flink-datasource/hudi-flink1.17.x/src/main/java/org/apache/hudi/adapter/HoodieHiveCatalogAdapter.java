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

package org.apache.hudi.adapter;

import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter clazz for {@code HoodieHiveCatalog}.
 */
public interface HoodieHiveCatalogAdapter extends Catalog, TableSchemaSupplier, FlinkTypeConverter {

  default void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, List<TableChange> tableChanges,
      boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(newCatalogTable, "New catalog table cannot be null");

    if (!isUpdatePermissible(tablePath, newCatalogTable, ignoreIfNotExists)) {
      return;
    }
    InternalSchema oldSchema = getInternalSchema(tablePath);
    InternalSchema newSchema = oldSchema;
    for (TableChange tableChange : tableChanges) {
      newSchema = Utils.applyTableChange(newSchema, tableChange, this);
    }
    if (!oldSchema.equals(newSchema)) {
      alterHoodieTableSchema(tablePath, newSchema);
    }
    refreshHMSTable(tablePath, newCatalogTable);
  }

  boolean isUpdatePermissible(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists) throws TableNotExistException;

  void refreshHMSTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable);

  void alterHoodieTableSchema(ObjectPath tablePath, InternalSchema newSchema) throws TableNotExistException, CatalogException;
}
