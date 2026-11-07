package net.jpountz.lz4;

/*
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
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

import static net.jpountz.lz4.Instances.COMPRESSORS;
import static net.jpountz.lz4.Instances.FAST_DECOMPRESSORS;
import static net.jpountz.lz4.Instances.SAFE_DECOMPRESSORS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.*;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.carrotsearch.randomizedtesting.annotations.Seeds;

public class LZ4Test extends AbstractLZ4Test {

  @Test
  @Repeat(iterations = 50)
  public void testMaxCompressedLength() {
    final int len = randomBoolean() ? randomInt(16) : randomInt(1 << 30);
    for (LZ4Compressor compressor : COMPRESSORS) {
      assertEquals(LZ4JNI.LZ4_compressBound(len), compressor.maxCompressedLength(len));
    }
  }

  private static byte[] getCompressedWorstCase(byte[] decompressed) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int len = decompressed.length;
    if (len >= LZ4Constants.RUN_MASK) {
      baos.write(LZ4Constants.RUN_MASK << LZ4Constants.ML_BITS);
      len -= LZ4Constants.RUN_MASK;
      while (len >= 255) {
        baos.write(255);
        len -= 255;
      }
      baos.write(len);
    } else {
      baos.write(len << LZ4Constants.ML_BITS);
    }
    try {
      baos.write(decompressed);
    } catch (IOException e) {
      throw new AssertionError();
    }
    return baos.toByteArray();
  }

  @Test
  public void testEmpty() {
    testRoundTrip(new byte[0]);
  }

  public void testUncompressWorstCase(LZ4FastDecompressor decompressor) {
    final int len = randomInt(100 * 1024);
    final int max = randomIntBetween(1, 255);
    byte[] decompressed = randomArray(len, max);
    byte[] compressed = getCompressedWorstCase(decompressed);
    byte[] restored = new byte[decompressed.length];
    int cpLen = decompressor.decompress(compressed, 0, restored, 0, decompressed.length);
    assertEquals(compressed.length, cpLen);
    assertArrayEquals(decompressed, restored);
  }

  @Test
  public void testUncompressWorstCase() {
    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      testUncompressWorstCase(decompressor);
    }
  }

  public void testUncompressWorstCase(LZ4SafeDecompressor decompressor) {
    final int len = randomInt(100 * 1024);
    final int max = randomIntBetween(1, 256);
    byte[] decompressed = randomArray(len, max);
    byte[] compressed = getCompressedWorstCase(decompressed);
    byte[] restored = new byte[decompressed.length];
    int uncpLen = decompressor.decompress(compressed, 0, compressed.length, restored, 0);
    assertEquals(decompressed.length, uncpLen);
    assertArrayEquals(decompressed, restored);
  }

  @Seeds({
    @Seed("69CCC65267F3C588"),  // Seed that generates len < 16
    @Seed()
  })
  @Test
  public void testUncompressSafeWorstCase() {
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      testUncompressWorstCase(decompressor);
    }
  }

  public void testRoundTrip(byte[] data, int off, int len,
      LZ4Compressor compressor,
      LZ4FastDecompressor decompressor,
      LZ4SafeDecompressor decompressor2) {
    for (Tester<?> tester : Arrays.asList(Tester.BYTE_ARRAY, Tester.BYTE_BUFFER, Tester.BYTE_ARRAY_WITH_LENGTH, Tester.BYTE_BUFFER_WITH_LENGTH)) {
      testRoundTrip(tester, data, off, len, compressor, decompressor, decompressor2);
    }
    if (data.length == len && off == 0) {
      for (SrcDestTester<?> tester : Arrays.asList(SrcDestTester.BYTE_ARRAY, SrcDestTester.BYTE_BUFFER, SrcDestTester.BYTE_ARRAY_WITH_LENGTH, SrcDestTester.BYTE_BUFFER_WITH_LENGTH)) {
        testRoundTrip(tester, data, compressor, decompressor, decompressor2);
      }
    }
  }

  public <T> void testRoundTrip(
      Tester<T> tester,
      byte[] data, int off, int len,
      LZ4Compressor compressor,
      LZ4FastDecompressor decompressor,
      LZ4SafeDecompressor decompressor2) {
    final int maxCompressedLength = tester.maxCompressedLength(len);
    // "maxCompressedLength + 1" for the over-estimated compressed length test below
    final T compressed = tester.allocate(maxCompressedLength + 1);
    final int compressedLen = tester.compress(compressor,
        tester.copyOf(data), off, len,
        compressed, 0, maxCompressedLength);

    // test decompression
    final T restored = tester.allocate(len);
    assertEquals(compressedLen, tester.decompress(decompressor, compressed, 0, restored, 0, len));
    assertArrayEquals(Arrays.copyOfRange(data, off, off + len), tester.copyOf(restored, 0, len));

    // make sure it fails if the compression dest is not large enough
    tester.fill(restored, randomByte());
    final T compressed2 = tester.allocate(compressedLen - 1);
    try {
      final int compressedLen2 = tester.compress(compressor,
          tester.copyOf(data), off, len,
          compressed2, 0, compressedLen - 1);
      // Compression can succeed even with the smaller dest
      // because the compressor is allowed to return different compression results
      // even when it is invoked with the same input data.
      // In this case, just make sure the compressed data can be successfully decompressed.
      assertEquals(compressedLen2, tester.decompress(decompressor, compressed2, 0, restored, 0, len));
      assertArrayEquals(Arrays.copyOfRange(data, off, off + len), tester.copyOf(restored, 0, len));
    } catch (LZ4Exception e) {
      // OK
    }

    if (tester != Tester.BYTE_ARRAY_WITH_LENGTH && tester != Tester.BYTE_BUFFER_WITH_LENGTH) {
      // LZ4DecompressorWithLength will succeed in decompression
      // because it ignores destLen.

      if (len > 0) {
        // decompression dest is too small
        try {
          tester.decompress(decompressor, compressed, 0, restored, 0, len - 1);
          fail();
        } catch (LZ4Exception e) {
          // OK
        }
      }

      // decompression dest is too large
      final T restored2 = tester.allocate(len + 1);
      try {
        final int cpLen = tester.decompress(decompressor, compressed, 0, restored2, 0, len + 1);
        fail("compressedLen=" + cpLen);
      } catch (LZ4Exception e) {
        // OK
      }
    }

    // try decompression when only the size of the compressed buffer is known
    if (len > 0) {
      tester.fill(restored, randomByte());
      assertEquals(len, tester.decompress(decompressor2, compressed, 0, compressedLen, restored, 0, len));
      assertArrayEquals(Arrays.copyOfRange(data, off, off + len), tester.copyOf(restored, 0, len));
      tester.fill(restored, randomByte());
    } else {
      assertEquals(0, tester.decompress(decompressor2, compressed, 0, compressedLen, tester.allocate(1), 0, 1));
    }

    // over-estimated compressed length
    try {
      final int decompressedLen = tester.decompress(decompressor2, compressed, 0, compressedLen + 1, tester.allocate(len + 100), 0, len + 100);
      fail("decompressedLen=" + decompressedLen);
    } catch (LZ4Exception e) {
      // OK
    }

    // under-estimated compressed length
    try {
      final int decompressedLen = tester.decompress(decompressor2, compressed, 0, compressedLen - 1, tester.allocate(len + 100), 0, len + 100);
      if (!(decompressor2 instanceof LZ4JNISafeDecompressor)) {
        fail("decompressedLen=" + decompressedLen);
      }
    } catch (LZ4Exception e) {
      // OK
    }
  }

  public <T> void testRoundTrip(SrcDestTester<T> tester,
                                byte[] data,
                                LZ4Compressor compressor,
                                LZ4FastDecompressor decompressor,
                                LZ4SafeDecompressor decompressor2) {
    final T original = tester.copyOf(data);
    final int maxCompressedLength = tester.maxCompressedLength(data.length);
    final T compressed = tester.allocate(maxCompressedLength);
    final int compressedLen = tester.compress(compressor,
                                              original,
                                              compressed);
    if (original instanceof ByteBuffer) {
      assertEquals(data.length, ((ByteBuffer) original).position());
      assertEquals(compressedLen, ((ByteBuffer) compressed).position());
      ((ByteBuffer) original).rewind();
      ((ByteBuffer) compressed).rewind();
    }

    // test decompression
    final T restored = tester.allocate(data.length);
    assertEquals(compressedLen, tester.decompress(decompressor, compressed, restored));
    if (original instanceof ByteBuffer) {
      assertEquals(compressedLen, ((ByteBuffer) compressed).position());
      assertEquals(data.length, ((ByteBuffer) restored).position());
    }
    assertArrayEquals(data, tester.copyOf(restored, 0, data.length));
    if (original instanceof ByteBuffer) {
      ((ByteBuffer) compressed).rewind();
      ((ByteBuffer) restored).rewind();
    }

    // try decompression when only the size of the compressed buffer is known
    tester.fill(restored, randomByte());
    // Truncate the compressed buffer to the exact size
    final T compressedExactSize = tester.copyOf(tester.copyOf(compressed, 0, compressedLen));
    assertEquals(data.length, tester.decompress(decompressor2, compressedExactSize, restored));
    if (original instanceof ByteBuffer) {
      assertEquals(compressedLen, ((ByteBuffer) compressedExactSize).position());
      assertEquals(data.length, ((ByteBuffer) restored).position());
    }
    assertArrayEquals(data, tester.copyOf(restored, 0, data.length));
    if (original instanceof ByteBuffer) {
      ((ByteBuffer) compressedExactSize).rewind();
      ((ByteBuffer) restored).rewind();
    }
  }

  public void testRoundTrip(byte[] data, int off, int len, LZ4Factory compressorFactory, LZ4Factory decompressorFactory) {
    for (LZ4Compressor compressor : Arrays.asList(
        compressorFactory.fastCompressor(), compressorFactory.highCompressor())) {
      testRoundTrip(data, off, len, compressor, decompressorFactory.fastDecompressor(), decompressorFactory.safeDecompressor());
    }
  }

  public void testRoundTrip(byte[] data, int off, int len) {
    for (LZ4Factory compressorFactory : Arrays.asList(
        LZ4Factory.nativeInsecureInstance(),
        LZ4Factory.unsafeInsecureInstance(),
        LZ4Factory.safeInstance())) {
      for (LZ4Factory decompressorFactory : Arrays.asList(
          LZ4Factory.nativeInsecureInstance(),
          LZ4Factory.unsafeInsecureInstance(),
          LZ4Factory.safeInstance())) {
        testRoundTrip(data, off, len, compressorFactory, decompressorFactory);
      }
    }
  }

  public void testRoundTrip(byte[] data) {
    testRoundTrip(data, 0, data.length);
  }

  public void testRoundTrip(String resource) throws IOException {
    final byte[] data = readResource(resource);
    testRoundTrip(data);
  }

  @Test
  public void testRoundtripGeo() throws IOException {
    testRoundTrip("/calgary/geo");
  }

  @Test
  public void testRoundtripBook1() throws IOException {
    testRoundTrip("/calgary/book1");
  }

  @Test
  public void testRoundtripPic() throws IOException {
    testRoundTrip("/calgary/pic");
  }

  @Test
  public void testNullMatchDec() {
    // 1 literal, 4 matchs with matchDec=0, 8 literals
    final byte[] invalid = new byte[] { 16, 42, 0, 0, (byte) 128, 42, 42, 42, 42, 42, 42, 42, 42 };
    // decompression should neither throw an exception nor loop indefinitely
    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      decompressor.decompress(invalid, 0, new byte[13], 0, 13);
    }
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      decompressor.decompress(invalid, 0, invalid.length, new byte[20], 0);
    }
  }

  @Test
  public void testMatchLengthBytesFollowedByTooFewLiterals() {
    // 1 literal, a match with a length byte, then only 4 last literals; the native decoder rejects this since lz4 1.11.0
    final byte[] invalid = new byte[] { 0x1F, 7, 1, 0, 0, 0x40, 1, 2, 3, 4 };
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      try {
        decompressor.decompress(invalid, 0, invalid.length, new byte[64], 0);
        fail(decompressor + " accepted a byte[] block");
      } catch (LZ4Exception expected) {
      }
      // direct buffers, since heap buffers go through the byte[] code
      ByteBuffer src = ByteBuffer.allocateDirect(invalid.length);
      src.put(invalid).flip();
      try {
        decompressor.decompress(src, 0, invalid.length, ByteBuffer.allocateDirect(64), 0, 64);
        fail(decompressor + " accepted a ByteBuffer block");
      } catch (LZ4Exception expected) {
      }
    }
    // with 5 last literals the same block is valid
    final byte[] valid = new byte[] { 0x1F, 7, 1, 0, 0, 0x50, 1, 2, 3, 4, 5 };
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      assertEquals(decompressor.toString(), 25, decompressor.decompress(valid, 0, valid.length, new byte[64], 0));
    }
  }

  /**
   * Asserts that every safe decompressor rejects {@code block} with a destination of {@code maxDestLen} bytes, and
   * every fast decompressor with a destination of exactly {@code destLen} bytes, also when more bytes follow the
   * block in the source buffer.
   */
  private static void assertAllReject(byte[] block, int maxDestLen, int destLen) {
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      assertThrows(decompressor.toString(), LZ4Exception.class,
          () -> decompressor.decompress(block, 0, block.length, new byte[maxDestLen], 0));
      ByteBuffer src = ByteBuffer.allocateDirect(block.length);
      src.put(block).flip();
      assertThrows(decompressor.toString(), LZ4Exception.class,
          () -> decompressor.decompress(src, 0, block.length, ByteBuffer.allocateDirect(maxDestLen), 0, maxDestLen));
    }
    final byte[] padded = Arrays.copyOf(block, block.length + 32);
    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      for (byte[] src : new byte[][] { block, padded }) {
        final String msg = decompressor + " trailing=" + (src.length - block.length);
        assertThrows(msg, LZ4Exception.class, () -> decompressor.decompress(src, 0, new byte[destLen], 0, destLen));
        ByteBuffer srcBuf = ByteBuffer.allocateDirect(src.length);
        srcBuf.put(src).flip();
        assertThrows(msg, LZ4Exception.class,
            () -> decompressor.decompress(srcBuf, 0, ByteBuffer.allocateDirect(destLen), 0, destLen));
      }
    }
  }

  /**
   * Asserts that every decompressor decodes {@code block} to the same {@code decompressedLen} bytes, with an
   * exact-size destination and with a larger one.
   */
  private static void assertAllDecode(byte[] block, int decompressedLen) {
    final byte[] expected = LZ4Factory.safeInstance().safeDecompressor().decompress(block, decompressedLen);
    assertEquals(decompressedLen, expected.length);
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      for (int maxDestLen : new int[] { decompressedLen, decompressedLen + 64 }) {
        final String msg = decompressor + " maxDestLen=" + maxDestLen;
        byte[] dest = new byte[maxDestLen];
        assertEquals(msg, decompressedLen, decompressor.decompress(block, 0, block.length, dest, 0));
        assertArrayEquals(msg, expected, Arrays.copyOf(dest, decompressedLen));
        ByteBuffer src = ByteBuffer.allocateDirect(block.length);
        src.put(block).flip();
        ByteBuffer destBuf = ByteBuffer.allocateDirect(maxDestLen);
        assertEquals(msg, decompressedLen, decompressor.decompress(src, 0, block.length, destBuf, 0, maxDestLen));
        destBuf.get(dest);
        assertArrayEquals(msg, expected, Arrays.copyOf(dest, decompressedLen));
      }
    }
    final byte[] padded = Arrays.copyOf(block, block.length + 32);
    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      for (byte[] src : new byte[][] { block, padded }) {
        final String msg = decompressor + " trailing=" + (src.length - block.length);
        byte[] dest = new byte[decompressedLen];
        assertEquals(msg, block.length, decompressor.decompress(src, 0, dest, 0, decompressedLen));
        assertArrayEquals(msg, expected, dest);
        ByteBuffer srcBuf = ByteBuffer.allocateDirect(src.length);
        srcBuf.put(src).flip();
        ByteBuffer destBuf = ByteBuffer.allocateDirect(decompressedLen);
        assertEquals(msg, block.length, decompressor.decompress(srcBuf, 0, destBuf, 0, decompressedLen));
        destBuf.get(dest);
        assertArrayEquals(msg, expected, dest);
      }
    }
  }

  @Test
  public void testMatchFollowedByTooFewLastLiterals() {
    // 1 literal, a 19 byte match with one length byte, then only 4 last literals
    assertAllReject(new byte[] { 0x1F, 7, 1, 0, 0, 0x40, 1, 2, 3, 4 }, 64, 24);
    assertAllDecode(new byte[] { 0x1F, 7, 1, 0, 0, 0x50, 1, 2, 3, 4, 5 }, 25);
    // the same with a 274 byte match, which needs two length bytes
    assertAllReject(new byte[] { 0x1F, 7, 1, 0, (byte) 0xFF, 0, 0x40, 1, 2, 3, 4 }, 1024, 279);
    assertAllDecode(new byte[] { 0x1F, 7, 1, 0, (byte) 0xFF, 0, 0x50, 1, 2, 3, 4, 5 }, 280);
  }

  @Test
  public void testLastMatchStartsTooCloseToEnd() {
    // 1 literal, a 4 byte match starting 9 bytes before the end, then 5 last literals. The last match must start at
    // least MF_LIMIT bytes before the end of the block, so this is invalid if the block fills the destination.
    assertAllReject(new byte[] { 0x10, 0x41, 1, 0, 0x50, 0x42, 0x43, 0x44, 0x45, 0x46 }, 10, 10);
    // 2 literals, a 4 byte match starting exactly MF_LIMIT bytes before the end, then 8 last literals
    assertAllDecode(new byte[] { 0x20, 0x41, 0x42, 1, 0, (byte) 0x80, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A },
        14);
  }

  @Test
  public void testEndsWithMatch() {
    // 6 literals, 4 matchs
    final byte[] invalid = new byte[] { 96, 42, 43, 44, 45, 46, 47, 5, 0 };
    final int decompressedLength = 10;

    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      try {
        // it is invalid to end with a match, should be at least 5 literals
        decompressor.decompress(invalid, 0, new byte[decompressedLength], 0, decompressedLength);
        assertTrue(decompressor.toString(), false);
      } catch (LZ4Exception e) {
        // OK
      }
    }

    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      try {
        // it is invalid to end with a match, should be at least 5 literals
        decompressor.decompress(invalid, 0, invalid.length, new byte[20], 0);
        assertTrue(false);
      } catch (LZ4Exception e) {
        // OK
      }
    }
  }

  @Test
  public void testEndsWithLessThan5Literals() {
    // 6 literals, 4 matchs
    final byte[] invalidBase = new byte[] { 96, 42, 43, 44, 45, 46, 47, 5, 0 };

    for (int i = 1; i < 5; ++i) {
      final byte[] invalid = Arrays.copyOf(invalidBase, invalidBase.length + 1 + i);
      invalid[invalidBase.length] = (byte) (i << 4); // i literals at the end

      for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
        try {
          // it is invalid to end with a match, should be at least 5 literals
          decompressor.decompress(invalid, 0, new byte[20], 0, 20);
          assertTrue(decompressor.toString(), false);
        } catch (LZ4Exception e) {
          // OK
        }
      }

      for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
        try {
          // it is invalid to end with a match, should be at least 5 literals
          decompressor.decompress(invalid, 0, invalid.length, new byte[20], 0);
          assertTrue(false);
        } catch (LZ4Exception e) {
          // OK
        }
      }
    }
  }

  @Test
  public void testWriteToReadOnlyBuffer() {
    for (LZ4Compressor compressor : COMPRESSORS) {
      ByteBuffer in = Tester.BYTE_BUFFER.copyOf(new byte[] {2, 3});
      ByteBuffer out = Tester.BYTE_BUFFER.allocate(100).asReadOnlyBuffer();
      try {
        compressor.compress(in, out);
        fail();
      } catch (ReadOnlyBufferException e) {
        // ok
      }
    }
    for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
      ByteBuffer in = Tester.BYTE_BUFFER.copyOf(COMPRESSORS[0].compress(new byte[] {2, 3}));
      ByteBuffer out = Tester.BYTE_BUFFER.allocate(100).asReadOnlyBuffer();
      try {
        decompressor.decompress(in, out);
        fail();
      } catch (ReadOnlyBufferException e) {
        // ok
      }
    }
    for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
      ByteBuffer in = Tester.BYTE_BUFFER.copyOf(COMPRESSORS[0].compress(new byte[] {2, 3}));
      ByteBuffer out = Tester.BYTE_BUFFER.allocate(100).asReadOnlyBuffer();
      out.limit(2);
      try {
        decompressor.decompress(in, out);
        fail();
      } catch (ReadOnlyBufferException e) {
        // ok
      }
    }
  }

  @Test
  public void testDecompressorWithLengthRejectsShortOutput() {
    final byte[] data = new byte[16];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) i;
    }
    final byte[] compressed = new LZ4CompressorWithLength(COMPRESSORS[0]).compress(data);
    // rewrite the little-endian length prefix to declare more bytes than the data holds
    compressed[0] = 20;
    compressed[1] = compressed[2] = compressed[3] = 0;
    assertEquals(20, LZ4DecompressorWithLength.getDecompressedLength(compressed));

    for (LZ4SafeDecompressor safeDecompressor : SAFE_DECOMPRESSORS) {
      final LZ4DecompressorWithLength decompressor = new LZ4DecompressorWithLength(safeDecompressor);
      try {
        decompressor.decompress(compressed);
        fail();
      } catch (LZ4Exception e) {
        assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
      }
      try {
        decompressor.decompress(compressed, 0, compressed.length);
        fail();
      } catch (LZ4Exception e) {
        assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
      }
      try {
        decompressor.decompress(compressed, new byte[20]);
        fail();
      } catch (LZ4Exception e) {
        assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
      }
      try {
        decompressor.decompress(compressed, 0, compressed.length, new byte[20], 0);
        fail();
      } catch (LZ4Exception e) {
        assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
      }
      for (boolean direct : new boolean[] {false, true}) {
        final ByteBuffer src = direct ? ByteBuffer.allocateDirect(compressed.length) : ByteBuffer.allocate(compressed.length);
        src.put(compressed).flip();
        final ByteBuffer dest = direct ? ByteBuffer.allocateDirect(20) : ByteBuffer.allocate(20);
        try {
          decompressor.decompress(src, 0, dest, 0);
          fail();
        } catch (LZ4Exception e) {
          assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
        }
        try {
          decompressor.decompress(src, 0, compressed.length, dest, 0);
          fail();
        } catch (LZ4Exception e) {
          assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
        }
        try {
          decompressor.decompress(src, dest);
          fail();
        } catch (LZ4Exception e) {
          assertTrue(e.getMessage(), e.getMessage().contains("length prefix declared"));
        }
        assertEquals(0, src.position());
        assertEquals(0, dest.position());
      }
    }
  }

  @Test
  @Repeat(iterations = 5)
  public void testAllEqual() {
    final int len = randomBoolean() ? randomInt(20) : randomInt(100000);
    final byte[] buf = new byte[len];
    Arrays.fill(buf, randomByte());
    testRoundTrip(buf);
  }

  @Test
  public void testMaxDistance() {
    final int len = randomIntBetween(1 << 17, 1 << 18);
    final int off = randomInt(len - (1 << 16) - (1 << 15));
    final byte[] buf = new byte[len];
    for (int i = 0; i < (1 << 15); ++i) {
      buf[off + i] = randomByte();
    }
    System.arraycopy(buf, off, buf, off + 65535, 1 << 15);
    testRoundTrip(buf);
  }

  @Test
  @Repeat(iterations = 10)
  public void testRandomData() {
    final int n = randomIntBetween(1, 15);
    final int off = randomInt(1000);
    final int len = randomBoolean() ? randomInt(1 << 16) : randomInt(1 << 20);
    final byte[] data = randomArray(off + len + randomInt(100), n);
    testRoundTrip(data, off, len);
  }

  @Test
  // https://github.com/jpountz/lz4-java/issues/12
  public void testRoundtripIssue12() {
    byte[] data = new byte[]{
        14, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 72, 14, 72, 14, 85, 3, 72, 14, 72, 14, 72, 14, 72, 14, 72, 14, 72, 14, 85, 3, 72,
        14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 50, 64, 0, 46, -1, 0, 0, 0, 29, 3, 85,
        8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 50, 64, 0, 47, -105, 0, 0, 0, 30, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85,
        8, -113, 0, 68, -97, 3, 0, 2, -97, 6, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 120, 64, 0, 48, 4, 0, 0, 0, 31, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72,
        33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72,
        43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72,
        28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72,
        35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72,
        41, 72, 32, 72, 18, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 39, 24, 32, 34, 124, 0, 120, 64, 0, 48, 80, 0, 0, 0, 31, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72,
        35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72,
        41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72,
        40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72,
        31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72,
        26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72,
        37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72,
        36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72,
        20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72,
        22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72,
        38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72,
        29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72,
        27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 50, 64, 0, 49, 20, 0, 0, 0, 32, 3, -97, 6, 0,
        68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2,
        3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2,
        3, -97, 6, 0, 50, 64, 0, 50, 53, 0, 0, 0, 34, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3,
        -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97,
        3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3,
        85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0,
        2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3,
        -97, 6, 0, 50, 64, 0, 51, 85, 0, 0, 0, 36, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, -97, 5, 0, 2, 3, 85, 8, -113, 0, 68,
        -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0,
        68, -113, 0, 2, 3, -97, 6, 0, 50, -64, 0, 51, -45, 0, 0, 0, 37, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6,
        0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 120, 64, 0, 52, -88, 0, 0,
        0, 39, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72,
        13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 72,
        13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, -19, -24, -101, -35
      };
    testRoundTrip(data, 9, data.length - 9);
  }

  private static void assertCompressedArrayEquals(String message, byte[] expected, byte[] actual) {
    int off = 0;
    int decompressedOff = 0;
    while (true) {
      if (off == expected.length) {
        break;
      }
      final Sequence sequence1 = readSequence(expected, off);
      final Sequence sequence2 = readSequence(actual, off);
      assertEquals(message + ", off=" + off + ", decompressedOff=" + decompressedOff, sequence1, sequence2);
      off += sequence1.length;
      decompressedOff += sequence1.literalLen + sequence1.matchLen;
    }
  }

  private static Sequence readSequence(byte[] buf, int off) {
    final int start = off;
    final int token = buf[off++] & 0xFF;
    int literalLen = token >>> 4;
    if (literalLen >= 0x0F) {
      int len;
      while ((len = buf[off++] & 0xFF) == 0xFF) {
        literalLen += 0xFF;
      }
      literalLen += len;
    }
    off += literalLen;
    if (off == buf.length) {
      return new Sequence(literalLen, -1, -1, off - start);
    }
    int matchDec = (buf[off++] & 0xFF) | ((buf[off++] & 0xFF) << 8);
    int matchLen = token & 0x0F;
    if (matchLen >= 0x0F) {
      int len;
      while ((len = buf[off++] & 0xFF) == 0xFF) {
        matchLen += 0xFF;
      }
      matchLen += len;
    }
    matchLen += 4;
    return new Sequence(literalLen, matchDec, matchLen, off - start);
  }

  private static class Sequence {
    final int literalLen, matchDec, matchLen, length;

    public Sequence(int literalLen, int matchDec, int matchLen, int length) {
      this.literalLen = literalLen;
      this.matchDec = matchDec;
      this.matchLen = matchLen;
      this.length = length;
    }

    @Override
    public String toString() {
      return "Sequence [literalLen=" + literalLen + ", matchDec=" + matchDec
        + ", matchLen=" + matchLen + "]";
    }

    @Override
    public int hashCode() {
      return 42;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Sequence other = (Sequence) obj;
      if (literalLen != other.literalLen)
        return false;
      if (matchDec != other.matchDec)
        return false;
      if (matchLen != other.matchLen)
        return false;
      return true;
    }

  }

}
