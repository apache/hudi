package org.apache.hudi.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Paths

class TestPathUtils {

  @Test
  def testGlobPaths(@TempDir tempDir: File): Unit = {
    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata", "file5").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata", "file6").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    var paths = Seq(tempDir.getAbsolutePath + "/*")
    var globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(folders.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/*/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(0), files(1)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(2), files(3)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*", tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))
  }


}
