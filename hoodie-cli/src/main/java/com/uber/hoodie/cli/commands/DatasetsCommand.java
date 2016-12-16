/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DatasetsCommand implements CommandMarker {
    @CliCommand(value = "connect", help = "Connect to a hoodie dataset")
    public String connect(
            @CliOption(key = {"path"}, mandatory = true, help = "Base Path of the dataset")
            final String path) throws IOException {
        boolean initialized = HoodieCLI.initConf();
        HoodieCLI.initFS(initialized);
        HoodieCLI.setTableMetadata(new HoodieTableMetadata(HoodieCLI.fs, path));
        HoodieCLI.state = HoodieCLI.CLIState.DATASET;
        return "Metadata for table " + HoodieCLI.tableMetadata.getTableName() + " loaded";
    }
}
