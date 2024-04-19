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

package org.apache.hudi.common.bootstrap;

import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.StorageLocationInfo;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * Helper functions around FileStatus and StorageLocationInfo.
 */
public class FileStatusUtils {

  public static Path toPath(HoodiePath path) {
    if (null == path) {
      return null;
    }
    return new Path(path.getUri());
  }

  public static HoodiePath fromPath(Path path) {
    if (null == path) {
      return null;
    }
    return HoodiePath.newBuilder().setUri(path.toString()).build();
  }
  
  public static FsPermission toFSPermission(HoodieFSPermission fsPermission) {
    if (null == fsPermission) {
      return null;
    }
    FsAction userAction = fsPermission.getUserAction() != null ? FsAction.valueOf(fsPermission.getUserAction()) : null;
    FsAction grpAction = fsPermission.getGroupAction() != null ? FsAction.valueOf(fsPermission.getGroupAction()) : null;
    FsAction otherAction =
        fsPermission.getOtherAction() != null ? FsAction.valueOf(fsPermission.getOtherAction()) : null;
    boolean stickyBit = fsPermission.getStickyBit() != null ? fsPermission.getStickyBit() : false;
    return new FsPermission(userAction, grpAction, otherAction, stickyBit);
  }

  public static HoodieFSPermission fromFSPermission(FsPermission fsPermission) {
    if (null == fsPermission) {
      return null;
    }
    String userAction = fsPermission.getUserAction() != null ? fsPermission.getUserAction().name() : null;
    String grpAction = fsPermission.getGroupAction() != null ? fsPermission.getGroupAction().name() : null;
    String otherAction = fsPermission.getOtherAction() != null ? fsPermission.getOtherAction().name() : null;
    return HoodieFSPermission.newBuilder().setUserAction(userAction).setGroupAction(grpAction)
        .setOtherAction(otherAction).setStickyBit(fsPermission.getStickyBit()).build();
  }

  public static StoragePathInfo toStoragePathInfo(StorageLocationInfo storageLocationInfo) {
    if (null == storageLocationInfo) {
      return null;
    }

    return new StoragePathInfo(
        new StoragePath(storageLocationInfo.getPath().getUri()), storageLocationInfo.getLength(),
            storageLocationInfo.getIsDir() == null ? false : storageLocationInfo.getIsDir(),
            storageLocationInfo.getBlockReplication().shortValue(), storageLocationInfo.getBlockSize(), storageLocationInfo.getModificationTime());
  }

  public static StorageLocationInfo fromFileStatus(FileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    StorageLocationInfo storageLocationInfo = new StorageLocationInfo();
    try {
      storageLocationInfo.setPath(fromPath(fileStatus.getPath()));
      storageLocationInfo.setLength(fileStatus.getLen());
      storageLocationInfo.setIsDir(fileStatus.isDirectory());
      storageLocationInfo.setBlockReplication((int) fileStatus.getReplication());
      storageLocationInfo.setBlockSize(fileStatus.getBlockSize());
      storageLocationInfo.setModificationTime(fileStatus.getModificationTime());
      storageLocationInfo.setAccessTime(fileStatus.getModificationTime());
      storageLocationInfo.setSymlink(fileStatus.isSymlink() ? fromPath(fileStatus.getSymlink()) : null);
      safeReadAndSetMetadata(storageLocationInfo, fileStatus);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return storageLocationInfo;
  }

  /**
   * Used to safely handle FileStatus calls which might fail on some FileSystem implementation.
   * (DeprecatedLocalFileSystem)
   */
  private static void safeReadAndSetMetadata(StorageLocationInfo storageLocationInfo, FileStatus fileStatus) {
    try {
      storageLocationInfo.setOwner(fileStatus.getOwner());
      storageLocationInfo.setGroup(fileStatus.getGroup());
      storageLocationInfo.setPermission(fromFSPermission(fileStatus.getPermission()));
    } catch (IllegalArgumentException ie) {
      // Deprecated File System (testing) does not work well with this call
      // skipping
    }
  }

}
