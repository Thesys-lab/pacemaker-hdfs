package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.hdfs.util.DataTransferThrottler;

import java.io.IOException;
import java.io.OutputStream;

public class ThrottledOutputStream extends OutputStream {
  private OutputStream out;
  private DataTransferThrottler throttler;

  public ThrottledOutputStream(OutputStream out, long bandwidth) {
    this.out = out;
    if (bandwidth == 0) {
      this.throttler = null;
    } else {
      this.throttler = new DataTransferThrottler(bandwidth);
    }
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
    if (throttler != null) {
      throttler.throttle(1);
    }
  }

  @Override
  public void write(byte b[]) throws IOException {
    out.write(b);
    if (throttler != null) {
      throttler.throttle(b.length);
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    out.write(b, off, len);
    if (throttler != null) {
      throttler.throttle(len);
    }
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}