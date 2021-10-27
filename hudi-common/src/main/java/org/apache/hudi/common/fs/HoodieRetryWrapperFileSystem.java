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
package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hudi.common.util.RetryHelper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class HoodieRetryWrapperFileSystem extends FileSystem {

    private FileSystem fileSystem;
    private RetryHelper retryHelper;

    public HoodieRetryWrapperFileSystem(FileSystem fileSystem, RetryHelper retryHelper) {
        this.fileSystem = fileSystem;
        this.retryHelper = retryHelper;
    }

    @Override
    public URI getUri() {
        return fileSystem.getUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return (FSDataInputStream) retryHelper.tryWith(() -> fileSystem.open(f, bufferSize)).start();
    }

    @Override
    public FSDataOutputStream create(Path f,
                                     FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progress) throws IOException {
        return (FSDataOutputStream) retryHelper.tryWith(() -> fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)).start();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return (FSDataOutputStream) retryHelper.tryWith(() -> fileSystem.append(f, bufferSize, progress)).start();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return (boolean) retryHelper.tryWith(() -> fileSystem.rename(src, dst)).start();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return (boolean) retryHelper.tryWith(() -> fileSystem.delete(f, recursive)).start();
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return (FileStatus[]) retryHelper.tryWith(() -> fileSystem.listStatus(f)).start();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        fileSystem.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
        return fileSystem.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return (boolean) retryHelper.tryWith(() -> fileSystem.mkdirs(f, permission)).start();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return (FileStatus) retryHelper.tryWith(() -> fileSystem.getFileStatus(f)).start();
    }
}
