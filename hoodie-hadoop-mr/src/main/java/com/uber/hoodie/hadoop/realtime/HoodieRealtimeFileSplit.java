/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import org.apache.hadoop.mapred.FileSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Filesplit that wraps the base split and a list of log files to merge deltas from.
 */
public class HoodieRealtimeFileSplit extends FileSplit {

    private List<String> deltaFilePaths;

    private String maxCommitTime;


    public HoodieRealtimeFileSplit() {
        super();
    }

    public HoodieRealtimeFileSplit(FileSplit baseSplit, List<String> deltaLogFiles, String maxCommitTime) throws IOException {
        super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(), baseSplit.getLocations());
        this.deltaFilePaths = deltaLogFiles;
        this.maxCommitTime = maxCommitTime;
    }

    public List<String> getDeltaFilePaths() {
        return deltaFilePaths;
    }

    public String getMaxCommitTime() {
        return maxCommitTime;
    }

    private static void writeString(String str, DataOutput out) throws IOException {
        byte[] pathBytes = str.getBytes(StandardCharsets.UTF_8);
        out.writeInt(pathBytes.length);
        out.write(pathBytes);
    }

    private static String readString(DataInput in) throws IOException {
        byte[] pathBytes = new byte[in.readInt()];
        in.readFully(pathBytes);
        return new String(pathBytes, StandardCharsets.UTF_8);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        writeString(maxCommitTime, out);
        out.writeInt(deltaFilePaths.size());
        for (String logFilePath: deltaFilePaths) {
            writeString(logFilePath, out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        maxCommitTime = readString(in);
        int totalLogFiles = in.readInt();
        deltaFilePaths = new ArrayList<>(totalLogFiles);
        for (int i=0; i < totalLogFiles; i++) {
            deltaFilePaths.add(readString(in));
        }
    }
}
