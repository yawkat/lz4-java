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

import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;

public class LZ4FastResetTest extends TestCase {

  public void testFastResetCompressorLifecycleAndRoundTrip() {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    byte[] data = repeatedData();

    try (LZ4JNIFastResetCompressor compressor = factory.fastResetCompressor()) {
      assertEquals(LZ4Constants.DEFAULT_ACCELERATION, compressor.getAcceleration());
      assertFalse(compressor.isClosed());

      byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
      int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
      byte[] restored = new byte[data.length];

      int restoredLength = factory.safeDecompressor().decompress(compressed, 0, compressedLength, restored, 0);
      assertEquals(data.length, restoredLength);
      assertArrayEquals(data, restored);

      compressor.close();
      assertTrue(compressor.isClosed());
      compressor.close();

      try {
        compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
        fail();
      } catch (IllegalStateException expected) {
        // expected
      }
    }
  }

  public void testFastResetAccelerationNormalization() {
    LZ4Factory factory = LZ4Factory.nativeInstance();

    try (LZ4JNIFastResetCompressor defaultCompressor = factory.fastResetCompressor();
         LZ4JNIFastResetCompressor negativeCompressor = factory.fastResetCompressor(-17);
         LZ4JNIFastResetCompressor maxCompressor = factory.fastResetCompressor(Integer.MAX_VALUE)) {
      assertEquals(LZ4Constants.DEFAULT_ACCELERATION, defaultCompressor.getAcceleration());
      assertEquals(LZ4Constants.MIN_ACCELERATION, negativeCompressor.getAcceleration());
      assertEquals(LZ4Constants.MAX_ACCELERATION, maxCompressor.getAcceleration());
    }
  }

  public void testHighFastResetCompressorLifecycleAndLevelNormalization() {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    byte[] data = repeatedData();

    try (LZ4JNIHCFastResetCompressor defaultCompressor = factory.highFastResetCompressor();
         LZ4JNIHCFastResetCompressor lowCompressor = factory.highFastResetCompressor(0);
         LZ4JNIHCFastResetCompressor highCompressor = factory.highFastResetCompressor(99)) {
      assertEquals(LZ4Constants.DEFAULT_COMPRESSION_LEVEL, defaultCompressor.getCompressionLevel());
      assertEquals(LZ4Constants.DEFAULT_COMPRESSION_LEVEL, lowCompressor.getCompressionLevel());
      assertEquals(LZ4Constants.MAX_COMPRESSION_LEVEL, highCompressor.getCompressionLevel());

      byte[] compressed = new byte[highCompressor.maxCompressedLength(data.length)];
      int compressedLength = highCompressor.compress(data, 0, data.length, compressed, 0, compressed.length);
      byte[] restored = new byte[data.length];

      int restoredLength = factory.safeDecompressor().decompress(compressed, 0, compressedLength, restored, 0);
      assertEquals(data.length, restoredLength);
      assertArrayEquals(data, restored);

      highCompressor.close();
      assertTrue(highCompressor.isClosed());
      highCompressor.close();

      try {
        highCompressor.compress(data, 0, data.length, compressed, 0, compressed.length);
        fail();
      } catch (IllegalStateException expected) {
        // expected
      }
    }
  }

  public void testFastResetByteBufferFallbackUsesSafeJavaCompressor() throws Exception {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    ByteBuffer src = ByteBuffer.wrap(repeatedData()).asReadOnlyBuffer();
    int maxCompressedLength = LZ4Utils.maxCompressedLength(src.remaining());

    try (LZ4JNIFastResetCompressor defaultCompressor = factory.fastResetCompressor();
         LZ4JNIFastResetCompressor acceleratedCompressor = factory.fastResetCompressor(9)) {
      defaultCompressor.compress(src.duplicate(), 0, src.remaining(), ByteBuffer.allocate(maxCompressedLength), 0, maxCompressedLength);
      acceleratedCompressor.compress(src.duplicate(), 0, src.remaining(), ByteBuffer.allocate(maxCompressedLength), 0, maxCompressedLength);

      Field safeInstanceField = LZ4JNIFastResetCompressor.class.getDeclaredField("safeInstance");
      safeInstanceField.setAccessible(true);

      assertSame(LZ4Factory.safeInstance().fastCompressor(), safeInstanceField.get(defaultCompressor));
      assertSame(LZ4Factory.safeInstance().fastCompressor(), safeInstanceField.get(acceleratedCompressor));
    }
  }

  public void testFastResetByteBufferAfterClose() {
    ByteBuffer src = ByteBuffer.wrap(repeatedData());
    ByteBuffer dest = ByteBuffer.allocate(LZ4Utils.maxCompressedLength(src.remaining()));

    try (LZ4JNIFastResetCompressor compressor = LZ4Factory.nativeInstance().fastResetCompressor()) {
      compressor.close();
      try {
        compressor.compress(src, 0, src.remaining(), dest, 0, dest.remaining());
        fail();
      } catch (IllegalStateException expected) {
        // expected
      }
    }
  }

  public void testHighFastResetByteBufferFallbackKeepsCompressionLevel() throws Exception {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    ByteBuffer src = ByteBuffer.wrap(repeatedData()).asReadOnlyBuffer();
    int maxCompressedLength = LZ4Utils.maxCompressedLength(src.remaining());

    try (LZ4JNIHCFastResetCompressor level1 = factory.highFastResetCompressor(1);
         LZ4JNIHCFastResetCompressor level17 = factory.highFastResetCompressor(17)) {
      level1.compress(src.duplicate(), 0, src.remaining(), ByteBuffer.allocate(maxCompressedLength), 0, maxCompressedLength);
      level17.compress(src.duplicate(), 0, src.remaining(), ByteBuffer.allocate(maxCompressedLength), 0, maxCompressedLength);

      Field safeInstanceField = LZ4JNIHCFastResetCompressor.class.getDeclaredField("safeInstance");
      safeInstanceField.setAccessible(true);

      assertSame(LZ4Factory.safeInstance().highCompressor(1), safeInstanceField.get(level1));
      assertSame(LZ4Factory.safeInstance().highCompressor(17), safeInstanceField.get(level17));
    }
  }

  public void testFastResetMethodsRequireNativeFactory() {
    assertFastResetUnsupported(LZ4Factory.safeInstance());
    assertFastResetUnsupported(LZ4Factory.unsafeInstance());
    assertFastResetUnsupported(LZ4Factory.unsafeInsecureInstance());
  }

  private static byte[] repeatedData() {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) ('a' + (i % 7));
    }
    return data;
  }

  private static void assertFastResetUnsupported(LZ4Factory factory) {
    try {
      factory.fastResetCompressor();
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }

    try {
      factory.highFastResetCompressor();
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}

