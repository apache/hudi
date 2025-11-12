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

package org.apache.hudi.table;

import org.apache.hudi.table.catalog.HoodieCatalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * IT cases for schema evolution by alter table SQL using {@link HoodieCatalog}.
 */
public class ITTestSchemaEvolutionBySQLWithDFSCatalog extends ITTestSchemaEvolutionBySQL {

  @TempDir
  File tempFile;

  @Override
  protected Catalog createCatalog() {
    Map<String, String> catalogOptions = new HashMap<>();
    assertThrows(
        ValidationException.class,
        () -> catalog = new HoodieCatalog(CATALOG_NAME, Configuration.fromMap(catalogOptions)));
    String catalogPathStr = tempFile.getAbsolutePath();
    catalogOptions.put(CATALOG_PATH.key(), catalogPathStr);
    catalogOptions.put(DEFAULT_DATABASE.key(), DB_NAME);
    return new HoodieCatalog(DB_NAME, Configuration.fromMap(catalogOptions));
  }
}
