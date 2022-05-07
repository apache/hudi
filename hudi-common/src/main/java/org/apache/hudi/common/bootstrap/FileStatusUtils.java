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
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * Helper functions around FileStatus and HoodieFileStatus.
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
  
  public static FileStatus toFileStatus(HoodieFileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    return new FileStatus(fileStatus.getLength(), fileStatus.getIsDir() == null ? false : fileStatus.getIsDir(),
        fileStatus.getBlockReplication(), fileStatus.getBlockSize(), fileStatus.getModificationTime(),
        fileStatus.getAccessTime(), toFSPermission(fileStatus.getPermission()), fileStatus.getOwner(),
        fileStatus.getGroup(), toPath(fileStatus.getSymlink()), toPath(fileStatus.getPath()));
  }

  public static HoodieFileStatus fromFileStatus(FileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    HoodieFileStatus fStatus = new HoodieFileStatus();
    try {
      fStatus.setPath(fromPath(fileStatus.getPath()));
      fStatus.setLength(fileStatus.getLen());
      fStatus.setIsDir(fileStatus.isDirectory());
      fStatus.setBlockReplication((int) fileStatus.getReplication());
      fStatus.setBlockSize(fileStatus.getBlockSize());
      fStatus.setModificationTime(fileStatus.getModificationTime());
      fStatus.setAccessTime(fileStatus.getModificationTime());
      fStatus.setSymlink(fileStatus.isSymlink() ? fromPath(fileStatus.getSymlink()) : null);
      safeReadAndSetMetadata(fStatus, fileStatus);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return fStatus;
  }

  /**
   * Used to safely handle FileStatus calls which might fail on some FileSystem implementation.
   * (DeprecatedLocalFileSystem)
   */
  private static void safeReadAndSetMetadata(HoodieFileStatus fStatus, FileStatus fileStatus) {
    try {
      fStatus.setOwner(fileStatus.getOwner());
      fStatus.setGroup(fileStatus.getGroup());
      fStatus.setPermission(fromFSPermission(fileStatus.getPermission()));
    } catch (IllegalArgumentException ie) {
      // Deprecated File System (testing) does not work well with this call
      // skipping
    }
  }

}
