package net.jpountz.lz4;

/*
 * Copyright 2026 Jonas Konrad and the lz4-java contributors.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Checks the exception messages of the JNI decompressors for the error codes returned by native lz4 (1.11.0+).
 */
public class LZ4JNIErrorTest {
  /** Truncated input: the last literals run declares 5 bytes, but only 3 follow. */
  private static final byte[] TRUNCATED = {0x50, 'a', 'a', 'a'};
  /** Valid block of 5 literals, decompressed into a buffer that is too small. */
  private static final byte[] LITERALS_5 = {0x50, 'a', 'a', 'a', 'a', 'a'};
  /** One literal followed by a match with offset 2, which points before the start of the output. */
  private static final byte[] BAD_OFFSET = {0x10, 'a', 0x02, 0x00, 0x50, 'a', 'a', 'a', 'a', 'a'};

  static Stream<Arguments> safeCases() {
    return Stream.of(
      Arguments.of(TRUNCATED, 100, -1, "Malformed input"),
      Arguments.of(LITERALS_5, 4, -2, "Malformed input or destination buffer too small"),
      Arguments.of(BAD_OFFSET, 100, -3, "Malformed input: invalid match offset")
    );
  }

  private static ByteBuffer direct(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes).flip();
    return buffer;
  }

  @ParameterizedTest
  @MethodSource("safeCases")
  public void safeDecompressor(byte[] input, int maxDestLen, int expectedCode, String expectedMessage) {
    LZ4SafeDecompressor decompressor = LZ4Factory.nativeInstance().safeDecompressor();
    assertEquals(LZ4JNISafeDecompressor.class, decompressor.getClass());

    // the crafted input must actually trigger the error code under test
    assertEquals(expectedCode, LZ4JNI.LZ4_decompress_safe(input, null, 0, input.length, new byte[maxDestLen], null, 0, maxDestLen));

    // prefix the input so that the message does not depend on srcOff
    byte[] shifted = new byte[input.length + 3];
    System.arraycopy(input, 0, shifted, 3, input.length);
    LZ4Exception e = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(shifted, 3, input.length, new byte[maxDestLen], 0, maxDestLen));
    assertEquals(expectedMessage, e.getMessage());

    e = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(direct(shifted), 3, input.length, ByteBuffer.allocateDirect(maxDestLen), 0, maxDestLen));
    assertEquals(expectedMessage, e.getMessage());
  }

  @Test
  public void fastDecompressor() {
    LZ4FastDecompressor decompressor = LZ4Factory.nativeInsecureInstance().fastDecompressor();
    assertEquals(LZ4JNIFastDecompressor.class, decompressor.getClass());
    // LZ4_decompress_fast does not bound reads by the input length, so only BAD_OFFSET (which fails before reading
    // past its end) is safe to use here. It only ever reports -1.
    byte[] input = BAD_OFFSET;
    int destLen = 6;
    assertEquals(-1, LZ4JNI.LZ4_decompress_fast(input, null, 0, new byte[destLen], null, 0, destLen));

    LZ4Exception e = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(input, 0, new byte[destLen], 0, destLen));
    assertEquals("Malformed input", e.getMessage());

    e = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(direct(input), 0, ByteBuffer.allocateDirect(destLen), 0, destLen));
    assertEquals("Malformed input", e.getMessage());
  }
}
