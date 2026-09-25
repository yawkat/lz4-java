package net.jpountz.lz4;

/*
 * Copyright 2020 Charles Allen and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.jpountz.xxhash.XXHashFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.SequenceInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 *
 */
@RunWith(Parameterized.class)
public class LZ4FrameIOStreamTest {
  private static void copy(InputStream in, OutputStream out) throws IOException {
    final byte[] buffer = new byte[1 << 10];
    int inSize = in.read(buffer);
    while (inSize >= 0) {
      out.write(buffer, 0, inSize);
      inSize = in.read(buffer);
    }
    out.flush();
  }

  private static void copyWithPerByteReadWrite(InputStream in, OutputStream out) throws IOException {
    int readByte;
    while ((readByte = in.read()) != -1) {
      out.write(readByte);
    }
    out.flush();
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    final List<Object[]> retval = new LinkedList<>(
        Arrays.asList(
            new Object[]{0},
            new Object[]{1},
            new Object[]{1 << 10},
            new Object[]{(1 << 10) + 1},
            new Object[]{1 << 16},
            new Object[]{1 << 17},
            new Object[]{1 << 20}
        ));
    final Random rnd = new Random(78370789134L); // Chosen by lightly  smashing my keyboard a few times.
    for (int i = 0; i < 10; ++i) {
      retval.add(new Object[]{Math.abs(rnd.nextInt()) % (1 << 22)});
    }
    return retval;
  }

  private final int testSize;

  public LZ4FrameIOStreamTest(int testSize) {
    this.testSize = testSize;
  }

  File tmpFile = null;

  @Before
  public void setUp() throws IOException {
    tmpFile = Files.createTempFile("lz4ioTest", ".dat").toFile();
    final Random rnd = new Random(5378L);
    int sizeRemaining = testSize;
    try (OutputStream os = Files.newOutputStream(tmpFile.toPath())) {
      while (sizeRemaining > 0) {
        final byte[] buff = new byte[Math.min(sizeRemaining, 1 << 10)];
        rnd.nextBytes(buff);
        os.write(buff);
        sizeRemaining -= buff.length;
      }
    }
    Assert.assertEquals(testSize, tmpFile.length());
  }

  @After
  public void tearDown() {
    if (tmpFile != null && tmpFile.exists() && !tmpFile.delete()) {
      Assert.fail(String.format("Could not delete file [%s]", tmpFile.getAbsolutePath()));
    }
  }

  /**
   * Whether the native LZ4 CLI is available; can be used for comparing this library with the expected native LZ4 behavior
   */
  private static boolean hasLz4CLI = false;

  @BeforeClass
  public static void checkLz4CLI() {
    try {
      ProcessBuilder checkBuilder = new ProcessBuilder().command("lz4", "-V").redirectErrorStream(true);
      Process checkProcess = checkBuilder.start();
      hasLz4CLI = checkProcess.waitFor() == 0;
    } catch (IOException | InterruptedException e) {
      // lz4 CLI not available or failed to execute; treat as unavailable to allow test skip
      hasLz4CLI = false;
    }

    // Check if this is running in CI (env CI=true), see https://docs.github.com/en/actions/reference/workflows-and-actions/variables#default-environment-variables
    if (!hasLz4CLI && "true".equals(System.getenv("CI"))) {
      Assert.fail("LZ4 CLI is not available, but should be for CI run");
    }
  }

  private void fillBuffer(final byte[] buffer, final InputStream is) throws IOException {
    int offset = 0;
    while (offset < buffer.length) {
      final int myLength = is.read(buffer, offset, buffer.length - offset);
      if (myLength < 0) {
        throw new EOFException("End of stream");
      }
      offset += myLength;
    }
  }

  private void validateStreamEquals(InputStream is, File file) throws IOException {
    int size = (int) file.length();
    try (InputStream fis = new FileInputStream(file)) {
      while (size > 0) {
        final byte[] buffer0 = new byte[Math.min(size, 1 << 10)];
        final byte[] buffer1 = new byte[Math.min(size, 1 << 10)];
        fillBuffer(buffer1, fis);
        fillBuffer(buffer0, is);
        for (int i = 0; i < buffer0.length; ++i) {
          Assert.assertEquals(buffer0[i], buffer1[i]);
        }
        size -= buffer0.length;
      }
    }
  }

  private void validateStreamEqualsWithPerByteRead(InputStream is, File file) throws IOException {
    try (InputStream fis = new FileInputStream(file)) {
      for (int size = (int) file.length(); size > 0; size--) {
        int byte0 = is.read();
        int byte1 = fis.read();
        Assert.assertEquals(byte0, byte1);
        if (byte0 == -1) {
          throw new EOFException("End of stream");
        }
        if (byte1 == -1) {
          throw new EOFException("End of stream");
        }
      }
    }
  }

  @Test
  public void testValidator() throws IOException {
    try (InputStream is = new FileInputStream(tmpFile)) {
      validateStreamEquals(is, tmpFile);
    }
    final File file = Files.createTempFile("copyTmp", ".dat").toFile();
    try {
      try (InputStream is = new FileInputStream(tmpFile)) {
        try (OutputStream os = new FileOutputStream(file)) {
          copy(is, os);
        }
      }
      try (InputStream is = new FileInputStream(file)) {
        validateStreamEquals(is, file);
      }
    } finally {
      file.delete();
    }
  }

  @Test
  public void testOutputSimple() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      final ByteBuffer buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
      try (FileChannel channel = FileChannel.open(lz4File.toPath())) {
        channel.read(buffer);
      }
      buffer.rewind();
      Assert.assertEquals(LZ4FrameOutputStream.MAGIC, buffer.getInt());
      final BitSet b = BitSet.valueOf(new byte[]{buffer.get()});
      Assert.assertFalse(b.get(0));
      Assert.assertFalse(b.get(1));
      Assert.assertFalse(b.get(2));
      Assert.assertFalse(b.get(3));
      Assert.assertFalse(b.get(4));
      Assert.assertTrue(b.get(5));
      LZ4FrameOutputStream.BD bd = LZ4FrameOutputStream.BD.fromByte(buffer.get());
      Assert.assertEquals(LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB.getIndicator() << 4, bd.toByte());
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testInputOutputSimple() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testInputOutputWithPerByteReadWrite() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copyWithPerByteReadWrite(is, os);
        }
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEqualsWithPerByteRead(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testInputOutputSkipped() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (FileOutputStream fos = new FileOutputStream(lz4File)) {
        final int skipSize = 1 << 10;
        final ByteBuffer skipBuffer = ByteBuffer.allocate(skipSize + 8).order(ByteOrder.LITTLE_ENDIAN);
        skipBuffer.putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE | 0x00000007); // anything 00 through FF should work
        skipBuffer.putInt(skipSize);
        final byte[] skipRandom = new byte[skipSize];
        new Random(478278L).nextBytes(skipRandom);
        skipBuffer.put(skipRandom);
        fos.write(skipBuffer.array());
        try (OutputStream os = new LZ4FrameOutputStream(fos)) {
          try (InputStream is = new FileInputStream(tmpFile)) {
            copy(is, os);
          }
        }
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testSkippableOnly() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (FileOutputStream fos = new FileOutputStream(lz4File)) {
        final int skipSize = 1 << 10;
        final ByteBuffer skipBuffer = ByteBuffer.allocate(skipSize + 8).order(ByteOrder.LITTLE_ENDIAN);
        skipBuffer.putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE | 0x00000007); // anything 00 through FF should work
        skipBuffer.putInt(skipSize);
        final byte[] skipRandom = new byte[skipSize];
        new Random(478278L).nextBytes(skipRandom);
        skipBuffer.put(skipRandom);
        fos.write(skipBuffer.array());
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        Assert.assertEquals(0, is.available());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(0, is.available());
        Assert.assertEquals(0, is.skip(1));
        Assert.assertEquals(-1, is.read(new byte[1]));
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        Assert.assertEquals(-1, is.read(new byte[1]));
        Assert.assertEquals(-1, is.read(new byte[1]));
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        Assert.assertEquals(0, is.skip(1));
        Assert.assertEquals(0, is.skip(1));
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File), true)) {
        Assert.assertFalse(is.isExpectedContentSizeDefined());
        Assert.assertEquals(-1L, is.getExpectedContentSize());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(0, is.available());
        Assert.assertEquals(0, is.skip(1));
        Assert.assertFalse(is.isExpectedContentSizeDefined());
        Assert.assertEquals(-1L, is.getExpectedContentSize());
      }
      // Extra one byte at the tail
      try (InputStream is = new LZ4FrameInputStream(new SequenceInputStream(new FileInputStream(lz4File), new ByteArrayInputStream(new byte[1])))) {
        Assert.assertThrows(IOException.class, is::read);
      }
    } finally {
      lz4File.delete();
    }
  }

  /**
   * Stream consisting of a skippable frame header, a synthetic payload of the given length that
   * is generated on the fly (starting with the LZ4 frame magic, zeros otherwise), and a trailer.
   */
  private static final class SyntheticSkippableFrameStream extends InputStream {
    private final byte[] header;
    private final byte[] payloadPrefix;
    private final long payloadSize;
    private final byte[] trailer;
    private long pos;

    SyntheticSkippableFrameStream(long declaredSize, long payloadSize, byte[] trailer) {
      this.header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE)
          .putInt((int) declaredSize)
          .array();
      this.payloadPrefix = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(LZ4FrameOutputStream.MAGIC)
          .array();
      this.payloadSize = payloadSize;
      this.trailer = trailer;
    }

    long remaining() {
      return header.length + payloadSize + trailer.length - pos;
    }

    @Override
    public int read() throws IOException {
      final byte[] b = new byte[1];
      return read(b, 0, 1) == -1 ? -1 : b[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      final long payloadEnd = header.length + payloadSize;
      final int n;
      if (pos < header.length) {
        n = (int) Math.min(len, header.length - pos);
        System.arraycopy(header, (int) pos, b, off, n);
      } else if (pos < payloadEnd) {
        final long payloadPos = pos - header.length;
        n = (int) Math.min(len, payloadEnd - pos);
        Arrays.fill(b, off, off + n, (byte) 0);
        for (int i = 0; i < n && payloadPos + i < payloadPrefix.length; i++) {
          b[off + i] = payloadPrefix[(int) payloadPos + i];
        }
      } else if (pos < payloadEnd + trailer.length) {
        n = (int) Math.min(len, payloadEnd + trailer.length - pos);
        System.arraycopy(trailer, (int) (pos - payloadEnd), b, off, n);
      } else {
        return -1;
      }
      pos += n;
      return n;
    }
  }

  @Test
  public void testLargeSkippableFrame() throws IOException {
    // Skipping several GiB is slow-ish, only run this once
    Assume.assumeTrue(testSize == 1 << 10);
    final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (OutputStream os = new LZ4FrameOutputStream(compressed)) {
      try (InputStream is = new FileInputStream(tmpFile)) {
        copy(is, os);
      }
    }
    // Skippable frame size >= 2^31, which does not fit in a signed int
    final long skipSize = 0x80000000L;
    try (InputStream is = new LZ4FrameInputStream(new SyntheticSkippableFrameStream(skipSize, skipSize, compressed.toByteArray()))) {
      validateStreamEquals(is, tmpFile);
      Assert.assertEquals(-1, is.read());
    }
    // Maximum skippable frame size
    final long maxSkipSize = 0xFFFFFFFFL;
    try (InputStream is = new LZ4FrameInputStream(new SyntheticSkippableFrameStream(maxSkipSize, maxSkipSize, compressed.toByteArray()))) {
      validateStreamEquals(is, tmpFile);
      Assert.assertEquals(-1, is.read());
    }
  }

  @Test
  public void testTruncatedLargeSkippableFrame() throws IOException {
    // Skippable frame size >= 2^31, but the stream ends early: the skip must consume all that is available
    final SyntheticSkippableFrameStream truncated = new SyntheticSkippableFrameStream(0x80000000L, 64, new byte[0]);
    try (InputStream is = new LZ4FrameInputStream(truncated)) {
      final IOException e = Assert.assertThrows(IOException.class, is::read);
      Assert.assertEquals(LZ4FrameInputStream.PREMATURE_EOS, e.getMessage());
      Assert.assertEquals(0, truncated.remaining());
    }
  }

  @Test
  public void testEmptySkippableFrameOnly() throws IOException {
    final ByteBuffer frame = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    frame.putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE);
    frame.putInt(0);
    for (boolean readSingleFrame : new boolean[] {false, true}) {
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frame.array()), readSingleFrame)) {
        Assert.assertEquals(0, is.available());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(-1, is.read(new byte[1]));
        Assert.assertEquals(0, is.skip(1));
        Assert.assertEquals(0, is.available());
      }
    }
    try (LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frame.array()), true)) {
      Assert.assertEquals(-1L, is.getExpectedContentSize());
      Assert.assertFalse(is.isExpectedContentSizeDefined());
      Assert.assertEquals(-1, is.read());
    }
  }

  @Test
  public void testStreamWithContentSize() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      final long knownSize = tmpFile.length();
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File),
                                                      LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,
                                                      knownSize,
                                                      LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                                                      LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM,
                                                      LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE)) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File), true)) {
        Assert.assertEquals(knownSize, is.getExpectedContentSize());
        Assert.assertTrue(is.isExpectedContentSizeDefined());
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testStreamWithoutContentSize() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File),
              LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,
              LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
              LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM)) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File), true)) {
        Assert.assertEquals(-1L, is.getExpectedContentSize());
        Assert.assertFalse(is.isExpectedContentSizeDefined());
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testInputOutputWithBlockChecksum() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File),
                                                      LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
                                                      LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                                                      LZ4FrameOutputStream.FLG.Bits.BLOCK_CHECKSUM)) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testInputOutputMultipleFrames() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      final long oneLength = lz4File.length();
      try (OutputStream os = new FileOutputStream(lz4File, true)) {
        for (int i = 0; i < 3; ++i) {
          try (InputStream is = new FileInputStream(lz4File)) {
            long size = oneLength;
            while (size > 0) {
              final byte[] buff = new byte[Math.min((int) size, 1 << 10)];
              fillBuffer(buff, is);
              os.write(buff);
              size -= buff.length;
            }
          }
        }
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        Assert.assertThrows(UnsupportedOperationException.class, is::getExpectedContentSize);
        Assert.assertFalse(is.isExpectedContentSizeDefined());
        validateStreamEquals(is, tmpFile);
        validateStreamEquals(is, tmpFile);
        validateStreamEquals(is, tmpFile);
        validateStreamEquals(is, tmpFile);
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File), true)) {
        Assert.assertEquals(-1L, is.getExpectedContentSize());
        Assert.assertFalse(is.isExpectedContentSizeDefined());
        validateStreamEquals(is, tmpFile);
        Assert.assertEquals(-1, is.read());
        final byte[] tmpBuff = new byte[10];
        Assert.assertEquals(-1, is.read(tmpBuff, 0, 10));
        Assert.assertEquals(0, is.skip(1));
      }
    } finally {
      lz4File.delete();
    }
  }

  private static final int FLG_BLOCK_INDEPENDENCE = 0x60; // version 01, block independence
  private static final int FLG_CONTENT_CHECKSUM = 0x04;
  private static final int BD_64KB = 0x40;
  private static final int BD_4MB = 0x70;

  /**
   * Writes a frame with the given FLG and BD bytes, and the given data in a single block (no block if data is empty).
   */
  private static void writeFrame(ByteArrayOutputStream out, int flgByte, int bdByte, byte[] data, boolean compress) {
    final byte[] descriptor = {(byte) flgByte, (byte) bdByte};
    final int headerHash = (XXHashFactory.fastestInstance().hash32().hash(descriptor, 0, descriptor.length, 0) >> 8) & 0xFF;
    final ByteBuffer header = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(LZ4FrameOutputStream.MAGIC).put(descriptor).put((byte) headerHash);
    out.write(header.array(), 0, header.position());
    final ByteBuffer intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    if (data.length > 0) {
      if (compress) {
        final byte[] compressed = LZ4Factory.fastestJavaInstance().fastCompressor().compress(data);
        intBuffer.putInt(0, compressed.length);
        out.write(intBuffer.array(), 0, 4);
        out.write(compressed, 0, compressed.length);
      } else {
        intBuffer.putInt(0, data.length | LZ4FrameOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK);
        out.write(intBuffer.array(), 0, 4);
        out.write(data, 0, data.length);
      }
    }
    intBuffer.putInt(0, 0); // EndMark
    out.write(intBuffer.array(), 0, 4);
    if ((flgByte & FLG_CONTENT_CHECKSUM) != 0) {
      intBuffer.putInt(0, XXHashFactory.fastestInstance().hash32().hash(data, 0, data.length, 0));
      out.write(intBuffer.array(), 0, 4);
    }
  }

  private static void writeFrame(ByteArrayOutputStream out, int bdByte, byte[] data) {
    writeFrame(out, FLG_BLOCK_INDEPENDENCE, bdByte, data, true);
  }

  private static void writeEmptySkippableFrame(ByteArrayOutputStream out) {
    final ByteBuffer frame = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    frame.putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE).putInt(0);
    out.write(frame.array(), 0, frame.position());
  }

  /**
   * Decodes the given input and returns the number of bytes allocated by the current thread while doing so.
   */
  private static long allocatedBytesForDecode(byte[] input, long expectedLength) throws IOException {
    Assume.assumeTrue(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean);
    final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled());

    final byte[] out = new byte[1 << 10];
    final long threadId = Thread.currentThread().getId();
    long allocated = 0;
    // the first iteration is a warm-up, so that one-time allocations (e.g. class initialization) are not counted
    for (int i = 0; i < 2; i++) {
      final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
      long length = 0;
      try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(input))) {
        int n;
        while ((n = is.read(out)) != -1) {
          length += n;
        }
      }
      allocated = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
      Assert.assertEquals(expectedLength, length);
    }
    return allocated;
  }

  @Test
  public void testBlockBuffersReusedAcrossFrames() throws IOException {
    final byte[] data = new byte[] {1, 2, 3};
    final int frameCount = 2000;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < frameCount; i++) {
      writeFrame(baos, BD_4MB, new byte[0]);
      // the content checksum hash is reused across frames, so this also checks that it is reset
      writeFrame(baos, FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_CHECKSUM, BD_4MB, data, true);
    }
    final long allocated = allocatedBytesForDecode(baos.toByteArray(), (long) frameCount * data.length);
    // one raw and one compressed buffer of 4 MiB each, plus some slack
    Assert.assertTrue("Allocated " + allocated + " bytes", allocated < 3 * (8L << 20));
  }

  @Test
  public void testFramesWithoutBlocksAllocation() throws IOException {
    final int frameCount = 10000;
    final ByteArrayOutputStream checksumFrames = new ByteArrayOutputStream();
    final ByteArrayOutputStream skippableFrames = new ByteArrayOutputStream();
    for (int i = 0; i < frameCount; i++) {
      writeFrame(checksumFrames, FLG_BLOCK_INDEPENDENCE | FLG_CONTENT_CHECKSUM, BD_4MB, new byte[0], true);
      writeEmptySkippableFrame(skippableFrames);
    }
    final long checksumAllocated = allocatedBytesForDecode(checksumFrames.toByteArray(), 0);
    Assert.assertTrue("Allocated " + checksumAllocated + " bytes", checksumAllocated < frameCount * 1024L);
    final long skippableAllocated = allocatedBytesForDecode(skippableFrames.toByteArray(), 0);
    Assert.assertTrue("Allocated " + skippableAllocated + " bytes", skippableAllocated < frameCount * 64L);
  }

  @Test
  public void testBlockLargerThanFrameBlockSizeAfterLargerFrame() throws IOException {
    final byte[] first = new byte[] {1, 2, 3};
    final byte[] tooLarge = new byte[(64 << 10) + 1];
    for (boolean compress : new boolean[] {true, false}) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      writeFrame(baos, FLG_BLOCK_INDEPENDENCE, BD_4MB, first, true);
      // 64 KiB max block size, but a larger block
      writeFrame(baos, FLG_BLOCK_INDEPENDENCE, BD_64KB, tooLarge, compress);
      try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
        final byte[] out = new byte[first.length];
        fillBuffer(out, is);
        Assert.assertArrayEquals(first, out);
        Assert.assertThrows(IOException.class, is::read);
      }
    }
  }

  @Test
  public void testMultipleFramesWithDifferentBlockSizes() throws IOException {
    final LZ4FrameOutputStream.BLOCKSIZE[] blockSizes = {
        LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
        LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,
        LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB,
        LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
        LZ4FrameOutputStream.BLOCKSIZE.SIZE_1MB,
    };
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (LZ4FrameOutputStream.BLOCKSIZE blockSize : blockSizes) {
      try (OutputStream os = new LZ4FrameOutputStream(baos, blockSize)) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
    }
    final byte[] compressed = baos.toByteArray();
    try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(compressed))) {
      for (int i = 0; i < blockSizes.length; i++) {
        validateStreamEquals(is, tmpFile);
      }
      Assert.assertEquals(-1, is.read());
    }
  }

  @Test
  public void testNativeCompressIfAvailable() throws IOException, InterruptedException {
    Assume.assumeTrue(hasLz4CLI);
    nativeCompress();
    nativeCompress("--no-frame-crc");
  }

  private void nativeCompress(String... args) throws IOException, InterruptedException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    lz4File.delete();
    try {
      final ProcessBuilder builder = new ProcessBuilder();
      final ArrayList<String> cmd = new ArrayList<>();
      cmd.add("lz4");
      if (args != null) {
        cmd.addAll(Arrays.asList(args));
      }
      cmd.add(tmpFile.getAbsolutePath());
      cmd.add(lz4File.getAbsolutePath());
      builder.command(cmd.toArray(new String[0]));
      builder.inheritIO();
      Process process = builder.start();
      int retval = process.waitFor();
      Assert.assertEquals(0, retval);
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testUncompressableEnd() throws IOException {
    final byte data = (byte) 0xEE;
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final OutputStream os = new LZ4FrameOutputStream(baos, LZ4FrameOutputStream.BLOCKSIZE.SIZE_1MB)) {
        os.write(data);
      }
      final byte[] bytes = baos.toByteArray();
      try (final InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(bytes))) {
        Assert.assertEquals(data, (byte) is.read());
      }

      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      // Make sure final "block" is a zero length block, then set it to an incompressible zero length block.
      Assert.assertEquals(0, buffer.getInt(bytes.length - (Integer.SIZE >> 3)));
      buffer.putInt(bytes.length - (Integer.SIZE >> 3), LZ4FrameOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK);
      try (final InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(bytes))) {
        Assert.assertEquals(data, (byte) is.read());
      }
    }
  }

  @Test
  public void testNativeDecompressIfAvailable() throws IOException, InterruptedException {
    Assume.assumeTrue(hasLz4CLI);
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    final File unCompressedFile = Files.createTempFile("lz4raw", ".dat").toFile();
    unCompressedFile.delete();
    lz4File.delete();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File),
                                                      LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,
                                                      tmpFile.length(),
                                                      LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                                                      LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE,
                                                      LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM)) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        validateStreamEquals(is, tmpFile);
      }

      final ProcessBuilder builder = new ProcessBuilder();
      builder.command("lz4", "-d", "-vvvvvvv", lz4File.getAbsolutePath(), unCompressedFile.getAbsolutePath()).inheritIO();
      Process process = builder.start();
      int retval = process.waitFor();
      Assert.assertEquals(0, retval);
      try (InputStream is = new FileInputStream(unCompressedFile)) {
        validateStreamEquals(is, tmpFile);
      }
    } finally {
      lz4File.delete();
      unCompressedFile.delete();
    }
  }

  @Test
  public void testEmptyLZ4Input() throws IOException {
    try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(new byte[0]))) {
      Assert.assertThrows(IOException.class, is::read);
    }
  }

  @Test
  public void testPrematureMagicNb() throws IOException {
    try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(new byte[1]))) {
      Assert.assertThrows(IOException.class, is::read);
    }

    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }
      // Extra one byte at the tail
      try (InputStream is = new LZ4FrameInputStream(new SequenceInputStream(new FileInputStream(lz4File), new ByteArrayInputStream(new byte[1])))) {
        validateStreamEquals(is, tmpFile);
        Assert.assertThrows(IOException.class, is::read);
      }
    } finally {
      lz4File.delete();
    }
  }

  @Test
  public void testAvailable() throws IOException {
    final File lz4File = Files.createTempFile("lz4test", ".lz4").toFile();
    try {
      try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(lz4File))) {
        try (InputStream is = new FileInputStream(tmpFile)) {
          copy(is, os);
        }
      }

      try (InputStream is = new LZ4FrameInputStream(new FileInputStream(lz4File))) {
        Assert.assertEquals("available() should be 0 before first read", 0, is.available());

        if (is.read() != -1 && testSize > 1) {
          Assert.assertTrue(
            "After reading 1 byte, available() should report > 0 bytes ready in the buffer",
            is.available() > 0
          );
        }
      }
    } finally {
      lz4File.delete();
    }
  }

  private static byte[] compressFrame(byte[] data, LZ4FrameOutputStream.FLG.Bits... bits) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream os = new LZ4FrameOutputStream(baos, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, bits)) {
      os.write(data);
    }
    return baos.toByteArray();
  }

  private static byte[] concat(byte[]... arrays) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (byte[] a : arrays) {
      baos.write(a, 0, a.length);
    }
    return baos.toByteArray();
  }

  private static void assertFailedStream(LZ4FrameInputStream is) throws IOException {
    Assert.assertThrows(IOException.class, is::read);
    Assert.assertThrows(IOException.class, () -> is.read(new byte[16]));
    Assert.assertThrows(IOException.class, () -> is.skip(1));
    Assert.assertThrows(IOException.class, is::readAllBytes);
    Assert.assertThrows(IOException.class, () -> is.readNBytes(16));
    Assert.assertEquals(0, is.available());
    is.close();
  }

  @Test
  public void testFailureIsStickyAfterDescriptorHashMismatch() throws IOException {
    final byte[] frame1 = compressFrame("hello".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
    final byte[] frame2 = compressFrame("world".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
    // magic (4), FLG, BD, then the header checksum
    frame2[6] ^= 1;

    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(concat(frame1, frame2)));
    final byte[] first = new byte[5];
    Assert.assertEquals(5, is.readNBytes(first, 0, 5));
    Assert.assertArrayEquals("hello".getBytes("UTF-8"), first);
    final IOException e = Assert.assertThrows(IOException.class, is::read);
    Assert.assertEquals(LZ4FrameInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
    // must not go on to return the data of the rejected frame
    assertFailedStream(is);
  }

  @Test
  public void testFailureIsStickyAfterSkippableFrameAndCorruptHeader() throws IOException {
    final byte[] skippable = new byte[8];
    ByteBuffer.wrap(skippable).order(ByteOrder.LITTLE_ENDIAN).putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE).putInt(0);
    final byte[] frame = compressFrame("hello".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
    frame[6] ^= 1;

    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(concat(skippable, frame)));
    Assert.assertThrows(IOException.class, is::read);
    assertFailedStream(is);
  }

  @Test
  public void testFailureIsStickyAfterBlockChecksumMismatch() throws IOException {
    final byte[] frame = compressFrame("hello".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
        LZ4FrameOutputStream.FLG.Bits.BLOCK_CHECKSUM);
    // the frame ends with the block checksum (4) and the end mark (4)
    frame[frame.length - 5] ^= 1;

    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frame));
    final IOException e = Assert.assertThrows(IOException.class, is::read);
    Assert.assertEquals(LZ4FrameInputStream.BLOCK_HASH_MISMATCH, e.getMessage());
    // must not silently drop the block and continue
    assertFailedStream(is);
  }

  @Test
  public void testMalformedFirstHeaderThrowsIOException() throws IOException {
    final byte[] frame = compressFrame("hello".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
    // set the reserved bit 0 in FLG
    frame[4] |= 1;

    // the header is read lazily, so construction must not fail
    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frame));
    Assert.assertThrows(IOException.class, is::read);
    assertFailedStream(is);
  }

  @Test
  public void testLinkedBlocksFirstHeaderThrowsIOException() throws IOException {
    final byte[] frame = compressFrame("hello".getBytes("UTF-8"), LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
    // version 01, BLOCK_INDEPENDENCE cleared (linked blocks), with a valid descriptor checksum
    frame[4] = 0x40;
    frame[6] = (byte) ((XXHashFactory.fastestInstance().hash32().hash(frame, 4, 2, 0) >> 8) & 0xFF);

    // the header is read lazily, so construction must not fail
    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frame), true);
    assertInvalidDescriptor(Assert.assertThrows(IOException.class, is::isExpectedContentSizeDefined));
    Assert.assertThrows(IOException.class, is::getExpectedContentSize);
    assertFailedStream(is);
  }

  @Test
  public void testFailureIsStickyForExpectedContentSize() throws IOException {
    final byte[] skippable = new byte[8];
    ByteBuffer.wrap(skippable).order(ByteOrder.LITTLE_ENDIAN).putInt(LZ4FrameInputStream.MAGIC_SKIPPABLE_BASE).putInt(0);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream os = new LZ4FrameOutputStream(baos, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, 5L,
        LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE, LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE)) {
      os.write("hello".getBytes("UTF-8"));
    }
    final byte[] frame = baos.toByteArray();
    // magic (4), FLG, BD, content size (8), then the header checksum
    frame[14] ^= 1;

    final LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(concat(skippable, frame)), true);
    final IOException e = Assert.assertThrows(IOException.class, is::getExpectedContentSize);
    Assert.assertEquals(LZ4FrameInputStream.DESCRIPTOR_HASH_MISMATCH, e.getMessage());
    // must not report the content size of the rejected frame
    Assert.assertThrows(IOException.class, is::isExpectedContentSizeDefined);
    assertFailedStream(is);
  }

  private static byte[] frameHeader(int flg, int bd) {
    final byte[] descriptor = new byte[]{(byte) flg, (byte) bd};
    final byte hc = (byte) ((XXHashFactory.fastestInstance().hash32().hash(descriptor, 0, descriptor.length, 0) >> 8) & 0xFF);
    final ByteBuffer header = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(LZ4FrameOutputStream.MAGIC);
    header.put(descriptor);
    header.put(hc);
    return header.array();
  }

  private static void assertInvalidDescriptor(IOException e) {
    Assert.assertEquals(LZ4FrameInputStream.INVALID_DESCRIPTOR, e.getMessage());
  }

  @Test
  public void testInvalidOrUnsupportedFrameDescriptor() throws IOException {
    Assume.assumeTrue(testSize == 0); // does not depend on the test size
    final int[][] descriptors = {
        {0x40, 0x70}, // dependent blocks
        {0x61, 0x70}, // dictionary ID
        {0x62, 0x70}, // reserved FLG bit
        {0x20, 0x70}, // unsupported version
        {0x60, 0x30}, // block size indicator 3
        {0x60, 0x41}, // reserved BD bit
    };
    for (final int[] d : descriptors) {
      final byte[] header = frameHeader(d[0], d[1]);
      try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(header))) {
        assertInvalidDescriptor(Assert.assertThrows(IOException.class, is::read));
      }
      try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(header))) {
        assertInvalidDescriptor(Assert.assertThrows(IOException.class, () -> is.skip(1)));
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(header), true)) {
        assertInvalidDescriptor(Assert.assertThrows(IOException.class, is::getExpectedContentSize));
      }
      try (LZ4FrameInputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(header), true)) {
        assertInvalidDescriptor(Assert.assertThrows(IOException.class, is::isExpectedContentSizeDefined));
      }
    }
  }
}
