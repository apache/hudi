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

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.FSUtils;

import com.uber.hoodie.exception.DatasetNotFoundException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.InvalidDatasetException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Manages all file system level interactions for the Hoodie tables.
 */
public class HoodieTableMetadata implements Serializable {
    public static final String MAX_COMMIT_TS = String.valueOf(Long.MAX_VALUE);
    public static final String HOODIE_TABLE_NAME_PROP_NAME = "hoodie.table.name";
    public static final String HOODIE_TABLE_TYPE_PROP_NAME = "hoodie.table.type";
    public static final HoodieTableType DEFAULT_TABLE_TYPE = HoodieTableType.COPY_ON_WRITE;

    public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";
    private static final String HOODIE_HDRONE_PROFILE_DEFAULT_VALUE = "HOODIE";
    private static final java.lang.String HOODIE_HDRONE_PROFILE_PROP_NAME =
        "hoodie.hdrone.dataset.profile";

    private static Logger log = LogManager.getLogger(HoodieTableMetadata.class);
    private transient final FileSystem fs;
    private transient final Path metadataFolder;
    private final Properties properties;
    private HoodieCommits commits;
    private List<String> inflightCommits;
    private String basePath;

    public static final String METAFOLDER_NAME = ".hoodie";
    public static final String COMMIT_FILE_SUFFIX = ".commit";
    public static final String INFLIGHT_FILE_SUFFIX = ".inflight";

    /**
     * Constructor which initializes the hoodie table metadata. It will initialize the meta-data if not already present.
     *
     * @param fs
     * @param basePath
     * @param tableName
     */
    public HoodieTableMetadata(FileSystem fs, String basePath, String tableName) {
        this(fs, basePath, tableName, true);
    }

    /**
     * Constructor which loads the hoodie table metadata, It requires the meta-data to be present already
     * @param fs
     * @param basePath
     */
    public HoodieTableMetadata(FileSystem fs, String basePath) {
        this(fs, basePath, null, false);
    }

    private HoodieTableMetadata(FileSystem fs, String basePath, String tableName,
        boolean initOnMissing) {
        this.fs = fs;
        this.basePath = basePath;

        try {
            Path basePathDir = new Path(this.basePath);
            if (!fs.exists(basePathDir)) {
                if (initOnMissing) {
                    fs.mkdirs(basePathDir);
                } else {
                    throw new DatasetNotFoundException(this.basePath);
                }
            }

            if (!fs.isDirectory(new Path(basePath))) {
                throw new DatasetNotFoundException(this.basePath);
            }

            this.metadataFolder = new Path(this.basePath, METAFOLDER_NAME);
            Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
            if (!fs.exists(propertyPath)) {
                if (initOnMissing) {
                    // create .hoodie folder if it does not exist.
                    createHoodieProperties(metadataFolder, tableName);
                } else {
                    throw new InvalidDatasetException(this.basePath);
                }
            }

            // Load meta data
            this.commits = new HoodieCommits(scanCommits(COMMIT_FILE_SUFFIX));
            this.inflightCommits = scanCommits(INFLIGHT_FILE_SUFFIX);
            this.properties = readHoodieProperties();
            log.info("Found " + commits.getNumCommits() + " commits in total.");
            if (log.isDebugEnabled()) {
                log.debug("All commits :" + commits);
            }
        } catch (IOException e) {
            throw new HoodieIOException("Could not load HoodieMetadata from path " + basePath, e);
        }
    }

    /**
     * Returns all the commit metadata for this table. Reads all the commit files from HDFS.
     * Expensive operation, use with caution.
     *
     * @return SortedMap of CommitTime,HoodieCommitMetadata
     */
    public SortedMap<String, HoodieCommitMetadata> getAllCommitMetadata() {
        try {
            TreeMap<String, HoodieCommitMetadata> metadataMap = new TreeMap<>();
            for (String commitTs : commits.getCommitList()) {
                metadataMap.put(commitTs, getCommitMetadata(commitTs));
            }
            return Collections.unmodifiableSortedMap(metadataMap);
        } catch (IOException e) {
            throw new HoodieIOException("Could not load all commits for table " + getTableName(),
                e);
        }
    }

    public HoodieCommitMetadata getCommitMetadata(String commitTime) throws IOException {
        FSDataInputStream is = fs.open(new Path(metadataFolder, FSUtils.makeCommitFileName(commitTime)));
        try {
            String jsonStr = IOUtils.toString(is);
            return HoodieCommitMetadata.fromJsonString(jsonStr);
        } finally {
            is.close();
        }
    }

    public HoodieTableType getTableType() {
        return HoodieTableType.valueOf(properties.getProperty(HOODIE_TABLE_TYPE_PROP_NAME));
    }

    /**
     * Lookup the file name for specified <code>HoodieRecord</code>
     *
     * TODO(vc): This metadata needs to be cached in each executor, statically, and used across, if
     * we need to be nicer to the NameNode
     */
    public String getFilenameForRecord(FileSystem fs, final HoodieRecord record) {
        String fileId = record.getCurrentLocation().getFileId();
        return getFilenameForRecord(fs, record, fileId);
    }


    public String getFilenameForRecord(FileSystem fs, final HoodieRecord record, String fileId) {
        try {
            FileStatus[] files = fs.listStatus(new Path(basePath, record.getPartitionPath()));
            Map<String, List<FileStatus>> fileIdToVersions =
                    groupFilesByFileId(files, commits.lastCommit());
            // If the record is not found
            if(!fileIdToVersions.containsKey(fileId)) {
                throw new FileNotFoundException("Cannot find valid versions for fileId " + fileId);
            }

            List<FileStatus> statuses = fileIdToVersions.get(fileId);
            return statuses.get(0).getPath().getName();
        } catch (IOException e) {
            throw new HoodieIOException(
                    "Could not get Filename for record " + record, e);
        }
    }



    /**
     * Get only the latest file in the partition with precondition commitTime(file) lt maxCommitTime
     *
     * @param fs
     * @param partitionPathStr
     * @param maxCommitTime
     * @return
     */
    public FileStatus[] getLatestVersionInPartition(FileSystem fs, String partitionPathStr,
        String maxCommitTime) {
        try {
            Path partitionPath = new Path(basePath, partitionPathStr);
            if(!fs.exists(partitionPath)) {
                return new FileStatus[0];
            }
            FileStatus[] files = fs.listStatus(partitionPath);
            Map<String, List<FileStatus>> fileIdToVersions =
                groupFilesByFileId(files, commits.lastCommit());
            HashMap<String, FileStatus> validFiles = new HashMap<>();
            for (String fileId : fileIdToVersions.keySet()) {
                List<FileStatus> versions = fileIdToVersions.get(fileId);
                for (FileStatus file : versions) {
                    String filename = file.getPath().getName();
                    String commitTime = FSUtils.getCommitTime(filename);
                    if (HoodieCommits.isCommit1BeforeOrOn(commitTime, maxCommitTime)) {
                        validFiles.put(fileId, file);
                        break;
                    }
                }
            }
            return validFiles.values().toArray(new FileStatus[validFiles.size()]);
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not get latest versions in Partition " + partitionPathStr, e);
        }
    }

    /**
     * Get ALL the data files in partition grouped by fileId and sorted by the commitTime
     * Given a partition path, provide all the files with a list of their commits, sorted by commit time.
     */
    public Map<String, List<FileStatus>> getAllVersionsInPartition(FileSystem fs, String partitionPath) {
        try {
            FileStatus[] files = fs.listStatus(new Path(basePath, partitionPath));
            return groupFilesByFileId(files, commits.lastCommit());
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not load all file versions in partition " + partitionPath, e);
        }
    }

    /**
     * Get all the versions of files, within the commit range provided.
     *
     * @param commitsToReturn - commits to include
     */
    public FileStatus[] getLatestVersionInRange(FileStatus[] fileStatuses, List<String> commitsToReturn) {
        if (commitsToReturn.isEmpty()) {
            return new FileStatus[0];
        }
        try {
            Map<String, List<FileStatus>> fileIdToVersions =
                    groupFilesByFileId(fileStatuses, commits.lastCommit());

            List<FileStatus> statuses = new ArrayList<>();
            for (List<FileStatus> entry : fileIdToVersions.values()) {
                for (FileStatus status : entry) {
                    String commitTime = FSUtils.getCommitTime(status.getPath().getName());
                    if (commitsToReturn.contains(commitTime)) {
                        statuses.add(status);
                        break;
                    }
                }
            }
            return statuses.toArray(new FileStatus[statuses.size()]);
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files from commits " + commitsToReturn, e);
        }
    }

    /**
     *
     * Get the latest versions of all the files.
     *
     * @param fileStatuses
     * @return
     */
    public FileStatus[] getLatestVersions(FileStatus[] fileStatuses) {
        try {
            Map<String, List<FileStatus>> fileIdToVersions =
                    groupFilesByFileId(fileStatuses, commits.lastCommit());

            List<FileStatus> statuses = new ArrayList<>();
            for(List<FileStatus> entry:fileIdToVersions.values()) {
                // first file is the latest one
                statuses.add(entry.get(0));
            }
            return statuses.toArray(new FileStatus[statuses.size()]);
        } catch (IOException e) {
            throw new HoodieIOException("Could not filter files for latest version ", e);
        }
    }


    /**
     * Get the base path for the Hoodie Table
     *
     * @return
     */
    public String getBasePath() {
        return basePath;
    }


    public boolean isCommitsEmpty() {
        return commits.isEmpty();
    }

    public boolean isCommitTsSafe(String commitTs) {
        return !isCommitsEmpty() && (commits.isCommitBeforeEarliestCommit(commitTs) || commits
            .contains(commitTs));
    }

    public List<String> findCommitsSinceTs(String startTs) {
        return commits.findCommitsInRange(startTs, MAX_COMMIT_TS);
    }

    public List<String> findCommitsInRange(String startTs, String endTs) {
        return commits.findCommitsInRange(startTs, endTs);
    }

    public List<String> findCommitsAfter(String startTs, Integer maxCommits) {
        return commits.findCommitsAfter(startTs, maxCommits);
    }

    public HoodieCommits getAllCommits() {
        return commits;
    }

    public List<String> getAllInflightCommits() {
        return inflightCommits;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieTableMetadata{");
        sb.append("commits=").append(commits);
        sb.append('}');
        return sb.toString();
    }

    public String getTableName() {
        return properties.getProperty(HOODIE_TABLE_NAME_PROP_NAME);
    }

    public String getHDroneDatasetProfile() {
        return properties.getProperty(HOODIE_HDRONE_PROFILE_PROP_NAME, HOODIE_HDRONE_PROFILE_DEFAULT_VALUE);
    }

    /**
     * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties)
     *
     * @param metadataFolder
     * @param tableName
     * @throws IOException
     */
    private void createHoodieProperties(Path metadataFolder, String tableName) throws IOException {
        if (!fs.exists(metadataFolder)) {
            fs.mkdirs(metadataFolder);
        }
        Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
        FSDataOutputStream outputStream = fs.create(propertyPath);
        try {
            Properties props = new Properties();
            props.setProperty(HOODIE_TABLE_NAME_PROP_NAME, tableName);
            props.setProperty(HOODIE_TABLE_TYPE_PROP_NAME, DEFAULT_TABLE_TYPE.name());
            props
                .store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        } finally {
            outputStream.close();
        }
    }

    /**
     * Loads the hoodie table properties from the hoodie.properties file under the .hoodie path
     */
    private Properties readHoodieProperties() throws IOException {
        Properties props = new Properties();
        Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
        FSDataInputStream inputStream = fs.open(propertyPath);
        try {
            props.load(inputStream);
        } finally {
            inputStream.close();
        }
        return props;
    }

    /**
     * Scan the commit times (only choosing commit file with the given suffix)
     */
    private List<String> scanCommits(final String commitFileSuffix) throws IOException {
        log.info("Attempting to load the commits under " + metadataFolder + " with suffix " + commitFileSuffix);
        final List<String> commitFiles = new ArrayList<>();
        fs.listStatus(metadataFolder, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if (path.getName().endsWith(commitFileSuffix)) {
                    commitFiles.add(path.getName().split("\\.")[0]);
                    return true;
                }
                return false;
            }
        });
        return commitFiles;
    }

    /**
     * Takes a bunch of file versions, and returns a map keyed by fileId, with the necessary
     * version safety checking. Returns a map of commitTime and Sorted list of FileStats
     * ( by reverse commit time )
     *
     * @param maxCommitTime maximum permissible commit time
     *
     * @return
     */
    private Map<String, List<FileStatus>> groupFilesByFileId(FileStatus[] files,
        String maxCommitTime) throws IOException {
        HashMap<String, List<FileStatus>> fileIdtoVersions = new HashMap<>();
        for (FileStatus file : files) {
            String filename = file.getPath().getName();
            String fileId = FSUtils.getFileId(filename);
            String commitTime = FSUtils.getCommitTime(filename);
            if (isCommitTsSafe(commitTime) && HoodieCommits
                .isCommit1BeforeOrOn(commitTime, maxCommitTime)) {
                if (!fileIdtoVersions.containsKey(fileId)) {
                    fileIdtoVersions.put(fileId, new ArrayList<FileStatus>());
                }
                fileIdtoVersions.get(fileId).add(file);
            }
        }
        for (Map.Entry<String, List<FileStatus>> entry : fileIdtoVersions.entrySet()) {
            Collections.sort(fileIdtoVersions.get(entry.getKey()), new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus o1, FileStatus o2) {
                    String o1CommitTime = FSUtils.getCommitTime(o1.getPath().getName());
                    String o2CommitTime = FSUtils.getCommitTime(o2.getPath().getName());
                    // Reverse the order
                    return o2CommitTime.compareTo(o1CommitTime);
                }
            });
        }
        return fileIdtoVersions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        HoodieTableMetadata metadata = (HoodieTableMetadata) o;

        if (commits != null ? !commits.equals(metadata.commits) : metadata.commits != null)
            return false;
        return basePath != null ? basePath.equals(metadata.basePath) : metadata.basePath == null;

    }

    @Override
    public int hashCode() {
        int result = commits != null ? commits.hashCode() : 0;
        result = 31 * result + (basePath != null ? basePath.hashCode() : 0);
        return result;
    }

}

