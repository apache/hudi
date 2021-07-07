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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public abstract class MarkerFiles implements Serializable {

  private static final Logger LOG = LogManager.getLogger(MarkerFiles.class);

  protected final String basePath;
  protected final transient Path markerDirPath;
  protected final String instantTime;

  public MarkerFiles(String basePath, String markerFolderPath, String instantTime) {
    this.basePath = basePath;
    this.markerDirPath = new Path(markerFolderPath);
    this.instantTime = instantTime;
  }

  /**
   * The marker path will be <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename.marker.writeIOType.
   */
  public Path create(String partitionPath, String dataFileName, IOType type) {
    return create(partitionPath, dataFileName, type, false);
  }

  /**
   * The marker path will be <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename.marker.writeIOType.
   *
   * @return the path of the marker file if it is created successfully,
   * null if it already exists
   */
  public Path createIfNotExists(String partitionPath, String dataFileName, IOType type) {
    return create(partitionPath, dataFileName, type, true);
  }

  public void quietDeleteMarkerDir(HoodieEngineContext context, int parallelism) {
    try {
      deleteMarkerDir(context, parallelism);
    } catch (HoodieIOException ioe) {
      LOG.warn("Error deleting marker directory for instant " + instantTime, ioe);
    }
  }

  public static String stripMarkerSuffix(String path) {
    return path.substring(0, path.indexOf(HoodieTableMetaClient.MARKER_EXTN));
  }

  protected String getMarkerFileName(String dataFileName, IOType type) {
    return String.format("%s%s.%s", dataFileName, HoodieTableMetaClient.MARKER_EXTN, type.name());
  }

  /**
   * Returns the marker path. Would create the partition path first if not exists.
   *
   * @param partitionPath The partition path
   * @param dataFileName  The data file name
   * @param type          The IO type
   * @return path of the marker file
   */
  protected Path getMarkerPath(String partitionPath, String dataFileName, IOType type) {
    Path path = FSUtils.getPartitionPath(markerDirPath, partitionPath);
    String markerFileName = getMarkerFileName(dataFileName, type);
    return new Path(path, markerFileName);
  }

  protected String stripMarkerFolderPrefix(String fullMarkerPath) {
    ValidationUtils.checkArgument(fullMarkerPath.contains(HoodieTableMetaClient.MARKER_EXTN));
    String markerRootPath = Path.getPathWithoutSchemeAndAuthority(
        new Path(String.format("%s/%s/%s", basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime))).toString();
    int begin =
        fullMarkerPath.indexOf(markerRootPath);
    ValidationUtils.checkArgument(begin >= 0,
        "Not in marker dir. Marker Path=" + fullMarkerPath + ", Expected Marker Root=" + markerRootPath);
    return fullMarkerPath.substring(begin + markerRootPath.length() + 1);
  }

  public abstract boolean deleteMarkerDir(HoodieEngineContext context, int parallelism);

  public abstract boolean doesMarkerDirExist() throws IOException;

  public abstract Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException;

  public abstract Set<String> allMarkerFilePaths() throws IOException;

  abstract Path create(String partitionPath, String dataFileName, IOType type, boolean checkIfExists);
}
