/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * ReadOptimized view which includes only the ROStorageformat files
 */
public class ReadOptimizedTableView extends AbstractTableFileSystemView {
    public ReadOptimizedTableView(FileSystem fs, HoodieTableMetaClient metaClient) {
        super(fs, metaClient);
    }

    protected FileStatus[] listDataFilesInPartition(String partitionPathStr) {
        Path partitionPath = new Path(metaClient.getBasePath(), partitionPathStr);
        try {
            return fs.listStatus(partitionPath, path -> path.getName()
                .contains(metaClient.getTableConfig().getROFileFormat().getFileExtension()));
        } catch (IOException e) {
            throw new HoodieIOException(
                "Failed to list data files in partition " + partitionPathStr, e);
        }
    }


}
