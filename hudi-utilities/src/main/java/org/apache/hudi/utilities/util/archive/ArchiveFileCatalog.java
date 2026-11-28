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

package org.apache.hudi.utilities.util.archive;

import java.io.IOException;
import java.util.List;

/**
 * Supplies the archive files a search walks, newest first.
 *
 * <p>Split out from the search itself so the scheduler can be driven with synthetic file sizes -- a
 * three-gigabyte file under a two-gigabyte budget is a case no real fixture can produce.
 */
public interface ArchiveFileCatalog {

  /**
   * @return the table's archive files ordered newest to oldest, each carrying its size on disk
   * @throws IOException when the archive directory cannot be listed. Callers treat this as fatal:
   *         a search that does not know which files exist cannot report absence.
   */
  List<ArchiveFile> list() throws IOException;
}
