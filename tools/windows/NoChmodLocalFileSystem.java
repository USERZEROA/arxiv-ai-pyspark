// Windows-only helper for running local Spark without winutils.exe/hadoop.dll.
//
// On Windows, Hadoop's RawLocalFileSystem shells out to `winutils` to set or
// read POSIX permissions, and calls native hadoop.dll code to check whether a
// directory is readable. Neither binary ships with PySpark. Windows ACLs do not
// use POSIX permission bits, so this subclass:
//   * skips setPermission / setOwner,
//   * lists directories with java.io.File,
//   * builds FileStatus objects from java.io.File with a default permission.
// It is only enabled by spark_session.py when os.name == "nt"; Linux/macOS runs
// (including CI) use Spark's normal local file system.
package local.shim;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

public class NoChmodLocalFileSystem extends LocalFileSystem {
  public NoChmodLocalFileSystem() {
    super(new Raw());
  }

  public static class Raw extends RawLocalFileSystem {
    private static final FsPermission DIR_PERM = new FsPermission((short) 0755);
    private static final FsPermission FILE_PERM = new FsPermission((short) 0644);

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
      // no-op on Windows
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
      // no-op on Windows
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      File file = pathToFile(f);
      if (!file.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      boolean dir = file.isDirectory();
      return new FileStatus(
          dir ? 0L : file.length(),
          dir,
          1,
          getDefaultBlockSize(f),
          file.lastModified(),
          0L,
          dir ? DIR_PERM : FILE_PERM,
          System.getProperty("user.name"),
          "",
          f.makeQualified(getUri(), getWorkingDirectory()));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
      return getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      File local = pathToFile(f);
      if (!local.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      if (!local.isDirectory()) {
        return new FileStatus[] {getFileStatus(f)};
      }
      String[] names = local.list();
      if (names == null) {
        throw new IOException("Cannot list directory " + f);
      }
      List<FileStatus> out = new ArrayList<>(names.length);
      for (String name : names) {
        try {
          out.add(getFileStatus(new Path(f, name)));
        } catch (FileNotFoundException e) {
          // file removed between list() and stat(); same behaviour as Hadoop
        }
      }
      return out.toArray(new FileStatus[0]);
    }
  }
}
