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
        return fileSystem.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return fileSystem.append(f, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileSystem.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return fileSystem.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return fileSystem.listStatus(f);
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
        return fileSystem.mkdirs(f, permission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return fileSystem.getFileStatus(f);
    }
}
