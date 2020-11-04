package org.apache.hadoop.hdfs.server.balancer;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public class TestThrottledOutputStream {
  private static final int INPUT_DATA_SIZE = 524288 /* 512K */;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {4096 /* 4k */}, {8192 /* 8k */}, {16384 /* 16k */}, {32768 /* 32k */}, {65536 /* 64k */}, {131072 /* 128k */},
        {262144 /* 256k */}, {524288 /* 512k */}
    });
  }

  @Parameter
  public long bandwidth;

  @Test
  public void testPerformance() throws IOException {
    // Generate random data
    Random rand = new Random();
    byte[] input_data = new byte[INPUT_DATA_SIZE];
    rand.nextBytes(input_data);

    // Prepare input/output streams
    ByteArrayInputStream in = new ByteArrayInputStream(input_data);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ThrottledOutputStream throttled = new ThrottledOutputStream(out, this.bandwidth);

    // Copy data
    long start = System.currentTimeMillis();
    IOUtils.copy(in, throttled);
    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
    System.out.println("Benchmark test case: bandwidth = " + bandwidth + ", elapsed = " + elapsed + " seconds.");

    // Check result
    assertArrayEquals(input_data, out.toByteArray());
  }
}