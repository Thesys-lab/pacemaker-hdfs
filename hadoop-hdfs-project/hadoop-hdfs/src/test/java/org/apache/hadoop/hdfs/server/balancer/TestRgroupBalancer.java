package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;

class MockInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
  MockInputStream(byte[] buf) {
    super(buf);
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0 || count < pos) {
      throw new IOException("Seek position out of range.");
    }
    this.pos = (int) pos;
  }

  @Override
  public long getPos() throws IOException {
    return this.pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    if (targetPos < 0 || this.count < targetPos) {
      throw new IOException("Seek position out of range.");
    }
    this.pos = (int) targetPos;
    return false;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (buffer == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > buffer.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    if (position < 0 || this.count < position) {
      throw new IOException("Position out of range.");
    }
    if (position < this.count) {
      int n = (int) Math.min(length, this.count - position);
      System.arraycopy(this.buf, (int) position, buffer, offset, n);
      return n;
    }
    return -1;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (buffer == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > buffer.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return;
    }
    if (position < 0 || this.count < position) {
      throw new IOException("Cannot read after EOF.");
    }
    if (this.count < position + length) {
      throw new EOFException("Reach the end of stream.");
    }
    System.arraycopy(this.buf, (int) position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}

public class TestRgroupBalancer {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseWithIncorrectArgs1() {
    expectedException.expect(IllegalArgumentException.class);
    String[] args = {"-unknown"};
    RgroupBalancer.Cli.parse(args);
  }

  @Test
  public void testParseWithIncorrectArgs2() {
    expectedException.expect(IllegalArgumentException.class);
    String[] args = {"-bandwidth", "abc"};
    RgroupBalancer.Cli.parse(args);
  }

  @Test
  public void testParseWithIncorrectArgs3() {
    expectedException.expect(IllegalArgumentException.class);
    String[] args = {"-bandwidth", "-1"};
    RgroupBalancer.Cli.parse(args);
  }

  @Test
  public void testParseWithCorrectArgs1() {
    String[] args = {};
    RgroupBalancer.Options options = RgroupBalancer.Cli.parse(args);
    assertEquals(0, options.bandwidth);
  }

  @Test
  public void testParseWithCorrectArgs2() {
    String[] args = {"-bandwidth", "100"};
    RgroupBalancer.Options options = RgroupBalancer.Cli.parse(args);
    assertEquals(100, options.bandwidth);
  }

  @Test
  public void testMoveFile() throws IOException {
    // test data
    String source = "/tmp/src";
    String targetRgroup = "RS-6-3-1024K";
    String data = "Hello World!";

    // mock objects
    DistributedFileSystem mockDfs = mock(DistributedFileSystem.class);
    Configuration conf = new Configuration();
    FSDataInputStream mockIn = new FSDataInputStream(new MockInputStream(data.getBytes()));
    FSDataOutputStream mockOut = mock(FSDataOutputStream.class);

    // mock behaviors
    when(mockDfs.open(any(Path.class))).thenReturn(mockIn);
    when(mockDfs.create(any())).thenReturn(mockOut);
    when(mockDfs.delete(any(), anyBoolean())).thenReturn(true);
    when(mockDfs.rename(any(), any())).thenReturn(true);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    doAnswer(invocation -> {
      byte[] buffer = (byte[]) invocation.getArguments()[0];
      int len = (int) invocation.getArguments()[2];
      output.write(buffer, 0, len);
      return null;
    }).when(mockOut).write(any(), anyInt(), anyInt());

    // do action
    RgroupBalancer.moveFile(mockDfs, conf, source, targetRgroup, 2);

    // verify
    String workingPath = source + "._BALANCING_";
    String encodedPath = source + "._BALANCING_%" + targetRgroup;

    ArgumentCaptor<Path> openPath = ArgumentCaptor.forClass(Path.class);
    verify(mockDfs, times(1)).open(openPath.capture());
    assertEquals(source, openPath.getValue().toString());

    ArgumentCaptor<Path> createPath = ArgumentCaptor.forClass(Path.class);
    verify(mockDfs, times(1)).create(createPath.capture());
    assertEquals(encodedPath, createPath.getValue().toString());

    ArgumentCaptor<Path> deletePath = ArgumentCaptor.forClass(Path.class);
    verify(mockDfs, times(1)).delete(deletePath.capture(), eq(false));
    assertEquals(source, deletePath.getValue().toString());

    ArgumentCaptor<Path> renameSrc = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Path> renameDst = ArgumentCaptor.forClass(Path.class);
    verify(mockDfs, times(1)).rename(renameSrc.capture(), renameDst.capture());
    assertEquals(workingPath, renameSrc.getValue().toString());
    assertEquals(source, renameDst.getValue().toString());

    verify(mockOut, atLeastOnce()).write(any(), eq(0), eq(data.length()));

    assertEquals(data, output.toString());
  }
}