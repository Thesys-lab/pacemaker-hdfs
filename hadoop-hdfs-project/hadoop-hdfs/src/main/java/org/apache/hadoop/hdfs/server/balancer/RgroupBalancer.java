package org.apache.hadoop.hdfs.server.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;

import static com.google.common.base.Preconditions.checkArgument;

@InterfaceAudience.Private
public class RgroupBalancer {
  static final Logger LOG = LoggerFactory.getLogger(RgroupBalancer.class);

  private static final String USAGE = "Usage: hdfs rgroupbalancer"
      + "\n\t[-bandwidth <bandwidth>]\tMax bandwidth for balancing";

  private static final String WIP_SUFFIX = "._BALANCING_";

  private static DistributedFileSystem getDFS(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri() + " is not an HDFS file system");
    }
    return (DistributedFileSystem) fs;
  }

  private static String encodeRgroup(String path, String rgroup) {
    return path + "%" + rgroup;
  }

  @VisibleForTesting
  static void moveFile(DistributedFileSystem dfs, Configuration conf, String source, String targetRgroup, int bandwidth)
      throws IOException {
    Path sourcePath = new Path(source);
    Path workingPath = new Path(source + WIP_SUFFIX);
    Path encodedWorkingPath = new Path(encodeRgroup(source + WIP_SUFFIX, targetRgroup));

    FSDataInputStream in = dfs.open(sourcePath);
    FSDataOutputStream out = dfs.create(encodedWorkingPath);
    OutputStream throttled_out = new ThrottledOutputStream(out, bandwidth);
    IOUtils.copyBytes(in, throttled_out, conf, true);

    dfs.delete(sourcePath, false);
    dfs.rename(workingPath, sourcePath);
  }

  public static class Options {
    public int bandwidth = 0;  // 0 disables the throttling
  }

  private static void printOptions(Options options) {
    LOG.info("Running rgroupbalancer with options:");
    LOG.info("\tbandwidth = " + options.bandwidth);
  }

  // the main entry function for RgroupBalancer
  public static int run(Configuration conf, Options options) throws IOException, InterruptedException {
    DistributedFileSystem dfs = getDFS(conf);
    printOptions(options);

    // TODO: 1. start balancer, initialization

    // TODO: 2. get task(s) from NameNode

    // TODO: 3. iterate the task list and call `moveFile`

    return ExitStatus.SUCCESS.getExitCode();
  }

  @VisibleForTesting
  static class Cli extends Configured implements Tool {
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      try {
        return RgroupBalancer.run(conf, parse(args));
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Rgroup balancing took " +
            DurationFormatUtils.formatDuration(Time.monotonicNow() - startTime, "HH:mm:ss.S"));
      }
    }

    @VisibleForTesting
    static Options parse(String[] args) {
      Options options = new Options();
      if (args != null) {
        try {
          for (int i = 0; i < args.length; i++) {
            if ("-bandwidth".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "Bandwidth value is missing: args = " + Arrays.toString(args));
              try {
                int bandwidth = Integer.parseInt(args[i]);
                if (bandwidth < 0) {
                  throw new IllegalArgumentException("Number must be non-negative: bandwidth = " + bandwidth);
                }
                LOG.info("Using a bandwidth of " + bandwidth);
                options.bandwidth = bandwidth;
              } catch (IllegalArgumentException e) {
                System.err.println("Expecting a non-negative integer: " + args[i]);
                throw e;
              }
            } else {
              throw new IllegalArgumentException("args = " + Arrays.toString(args));
            }
          }
        } catch (RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      return options;
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }
    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new RgroupBalancer.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting rgroupbalancer due to an exception", e);
      System.exit(-1);
    }
  }
}
