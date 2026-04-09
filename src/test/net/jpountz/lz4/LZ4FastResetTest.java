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

  public void testFastResetConstructorNormalization() {
    try (LZ4JNIFastResetCompressor negativeCompressor = new LZ4JNIFastResetCompressor(-17);
         LZ4JNIFastResetCompressor maxCompressor = new LZ4JNIFastResetCompressor(Integer.MAX_VALUE)) {
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

  public void testHighFastResetConstructorNormalization() {
    try (LZ4JNIHCFastResetCompressor lowCompressor = new LZ4JNIHCFastResetCompressor(0);
         LZ4JNIHCFastResetCompressor highCompressor = new LZ4JNIHCFastResetCompressor(99)) {
      assertEquals(LZ4Constants.DEFAULT_COMPRESSION_LEVEL, lowCompressor.getCompressionLevel());
      assertEquals(LZ4Constants.MAX_COMPRESSION_LEVEL, highCompressor.getCompressionLevel());
    }
  }

  public void testFastResetByteBufferRoundTripWithDirectBuffers() {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    byte[] data = repeatedData();
    ByteBuffer src = directBuffer(data);
    ByteBuffer compressed = ByteBuffer.allocateDirect(LZ4Utils.maxCompressedLength(data.length));
    ByteBuffer restored = ByteBuffer.allocateDirect(data.length);

    try (LZ4JNIFastResetCompressor compressor = factory.fastResetCompressor(9)) {
      int compressedLength = compressor.compress(src, 0, src.remaining(), compressed, 0, compressed.capacity());
      int restoredLength = factory.safeDecompressor().decompress(compressed, 0, compressedLength, restored, 0, restored.capacity());

      assertEquals(data.length, restoredLength);
      assertArrayEquals(data, readBuffer(restored, restoredLength));
    }
  }

  public void testFastResetByteBufferRejectsUnsupportedSourceBuffer() {
    ByteBuffer src = ByteBuffer.wrap(repeatedData()).asReadOnlyBuffer();
    ByteBuffer dest = ByteBuffer.allocate(LZ4Utils.maxCompressedLength(src.remaining()));

    try (LZ4JNIFastResetCompressor compressor = LZ4Factory.nativeInstance().fastResetCompressor()) {
      compressor.compress(src, 0, src.remaining(), dest, 0, dest.remaining());
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
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

  public void testFastResetRepeatedCompressCallsReuseState() {
    LZ4Factory factory = LZ4Factory.nativeInstance();

    try (LZ4JNIFastResetCompressor compressor = factory.fastResetCompressor(9)) {
      assertRepeatedCompressionReuse(compressor, factory.safeDecompressor());
    }
  }

  public void testFastResetRepeatedCompressCallsRecoverAfterFailure() {
    LZ4Factory factory = LZ4Factory.nativeInstance();

    try (LZ4JNIFastResetCompressor compressor = factory.fastResetCompressor()) {
      assertCompressionRecoversAfterFailure(compressor, factory.safeDecompressor());
    }
  }

  public void testHighFastResetByteBufferRoundTripWithDirectBuffers() {
    LZ4Factory factory = LZ4Factory.nativeInstance();
    byte[] data = repeatedData();
    ByteBuffer src = directBuffer(data);
    ByteBuffer compressed = ByteBuffer.allocateDirect(LZ4Utils.maxCompressedLength(data.length));
    ByteBuffer restored = ByteBuffer.allocateDirect(data.length);

    try (LZ4JNIHCFastResetCompressor compressor = factory.highFastResetCompressor(9)) {
      int compressedLength = compressor.compress(src, 0, src.remaining(), compressed, 0, compressed.capacity());
      int restoredLength = factory.safeDecompressor().decompress(compressed, 0, compressedLength, restored, 0, restored.capacity());

      assertEquals(data.length, restoredLength);
      assertArrayEquals(data, readBuffer(restored, restoredLength));
    }
  }

  public void testHighFastResetByteBufferRejectsUnsupportedSourceBuffer() {
    ByteBuffer src = ByteBuffer.wrap(repeatedData()).asReadOnlyBuffer();
    ByteBuffer dest = ByteBuffer.allocate(LZ4Utils.maxCompressedLength(src.remaining()));

    try (LZ4JNIHCFastResetCompressor compressor = LZ4Factory.nativeInstance().highFastResetCompressor()) {
      compressor.compress(src, 0, src.remaining(), dest, 0, dest.remaining());
      fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void testHighFastResetByteBufferAfterClose() {
    ByteBuffer src = ByteBuffer.wrap(repeatedData());
    ByteBuffer dest = ByteBuffer.allocate(LZ4Utils.maxCompressedLength(src.remaining()));

    try (LZ4JNIHCFastResetCompressor compressor = LZ4Factory.nativeInstance().highFastResetCompressor()) {
      compressor.close();
      try {
        compressor.compress(src, 0, src.remaining(), dest, 0, dest.remaining());
        fail();
      } catch (IllegalStateException expected) {
        // expected
      }
    }
  }

  public void testHighFastResetRepeatedCompressCallsReuseState() {
    LZ4Factory factory = LZ4Factory.nativeInstance();

    try (LZ4JNIHCFastResetCompressor compressor = factory.highFastResetCompressor(9)) {
      assertRepeatedCompressionReuse(compressor, factory.safeDecompressor());
    }
  }

  public void testHighFastResetRepeatedCompressCallsRecoverAfterFailure() {
    LZ4Factory factory = LZ4Factory.nativeInstance();

    try (LZ4JNIHCFastResetCompressor compressor = factory.highFastResetCompressor()) {
      assertCompressionRecoversAfterFailure(compressor, factory.safeDecompressor());
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

  private static ByteBuffer directBuffer(byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
    buffer.put(data);
    buffer.flip();
    return buffer;
  }

  private static byte[] alternateData() {
    byte[] data = new byte[333];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) ((i * 31) ^ 0x5A);
    }
    return data;
  }

  private static byte[] readBuffer(ByteBuffer buffer, int length) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.position(0);
    duplicate.limit(length);
    byte[] bytes = new byte[length];
    duplicate.get(bytes);
    return bytes;
  }

  private static void assertRepeatedCompressionReuse(LZ4Compressor compressor, LZ4SafeDecompressor decompressor) {
    byte[] first = repeatedData();
    byte[] second = alternateData();

    assertArrayRoundTrip(compressor, decompressor, first);
    assertArrayRoundTrip(compressor, decompressor, second);
    assertDirectByteBufferRoundTrip(compressor, decompressor, second);
    assertDirectByteBufferRoundTrip(compressor, decompressor, first);
  }

  private static void assertCompressionRecoversAfterFailure(LZ4Compressor compressor, LZ4SafeDecompressor decompressor) {
    byte[] data = repeatedData();
    byte[] tooSmallDest = new byte[1];

    try {
      compressor.compress(data, 0, data.length, tooSmallDest, 0, tooSmallDest.length);
      fail();
    } catch (LZ4Exception expected) {
      // expected
    }

    assertArrayRoundTrip(compressor, decompressor, alternateData());
    assertDirectByteBufferRoundTrip(compressor, decompressor, data);
  }

  private static void assertArrayRoundTrip(LZ4Compressor compressor, LZ4SafeDecompressor decompressor, byte[] data) {
    byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
    int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
    byte[] restored = new byte[data.length];
    int restoredLength = decompressor.decompress(compressed, 0, compressedLength, restored, 0);

    assertEquals(data.length, restoredLength);
    assertArrayEquals(data, restored);
  }

  private static void assertDirectByteBufferRoundTrip(LZ4Compressor compressor, LZ4SafeDecompressor decompressor, byte[] data) {
    ByteBuffer src = directBuffer(data);
    ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedLength(data.length));
    ByteBuffer restored = ByteBuffer.allocateDirect(data.length);

    int srcPosition = src.position();
    int compressedPosition = compressed.position();
    int restoredPosition = restored.position();

    int compressedLength = compressor.compress(src, 0, src.remaining(), compressed, 0, compressed.capacity());
    int restoredLength = decompressor.decompress(compressed, 0, compressedLength, restored, 0, restored.capacity());

    assertEquals(srcPosition, src.position());
    assertEquals(compressedPosition, compressed.position());
    assertEquals(restoredPosition, restored.position());
    assertEquals(data.length, restoredLength);
    assertArrayEquals(data, readBuffer(restored, restoredLength));
  }

  private static void assertFastResetUnsupported(LZ4Factory factory) {
    try (LZ4JNIFastResetCompressor ignored = factory.fastResetCompressor()) {
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }

    try (LZ4JNIHCFastResetCompressor ignored = factory.highFastResetCompressor()) {
      fail();
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}

