package net.jpountz.lz4;

/*
 * Copyright 2025 Jonas Konrad and the lz4-java contributors.
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
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.jpountz.lz4.LZ4Constants.MIN_MATCH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OutOfBoundsTest {
  private static Stream<LZ4Factory> lz4Factories() {
    return Stream.of(
      LZ4Factory.fastestInstance(),
      LZ4Factory.fastestJavaInstance(),
      // LZ4Factory.nativeInsecureInstance(),
      LZ4Factory.nativeInstance(),
      LZ4Factory.safeInstance(),
      LZ4Factory.unsafeInsecureInstance()
    );
  }

  static Stream<LZ4FastDecompressor> fastDecompressors() {
    return lz4Factories().map(LZ4Factory::fastDecompressor);
  }

  private static Stream<LZ4SafeDecompressor> safeDecompressors() {
    return lz4Factories().map(LZ4Factory::safeDecompressor);
  }

  static Stream<LZ4DecompressorWithLength> decompressorsWithLength() {
    LZ4Factory factory = LZ4Factory.safeInstance();
    return Stream.of(
      new LZ4DecompressorWithLength(factory.fastDecompressor()),
      new LZ4DecompressorWithLength(factory.safeDecompressor())
    );
  }

  static Stream<LZ4DecompressorWithLength> limitedDecompressorsWithLength() {
    LZ4Factory factory = LZ4Factory.safeInstance();
    return Stream.of(
      new LZ4DecompressorWithLength(factory.fastDecompressor(), 15),
      new LZ4DecompressorWithLength(factory.safeDecompressor(), 15)
    );
  }

  /**
   * Abstraction over {@link LZ4FastDecompressor} and {@link LZ4SafeDecompressor}.
   *
   * <p>Should only be used for decompression which is expected to fail, because for the non-failing case
   * {@link LZ4FastDecompressor} and {@link LZ4SafeDecompressor} behave differently regarding whether the
   * input or output should be fully consumed.
   */
  public interface FallibleDecompressor {
    void decompress(byte[] input, byte[] output) throws LZ4Exception;

    static FallibleDecompressor fromFast(LZ4FastDecompressor fastDecompressor) {
      return new FallibleDecompressor() {
        @Override
        public void decompress(byte[] src, byte[] dest) throws LZ4Exception {
          fastDecompressor.decompress(src, dest);
        }

        @Override
        public String toString() {
          return fastDecompressor.toString();
        }
      };
    }

    static FallibleDecompressor fromSafe(LZ4SafeDecompressor safeDecompressor) {
      return new FallibleDecompressor() {
        @Override
        public void decompress(byte[] src, byte[] dest) throws LZ4Exception {
          safeDecompressor.decompress(src, dest);
        }

        @Override
        public String toString() {
          return safeDecompressor.toString();
        }
      };
    }
  }

  static Stream<FallibleDecompressor> allDecompressors() {
    return Stream.concat(fastDecompressors().map(FallibleDecompressor::fromFast),
      safeDecompressors().map(FallibleDecompressor::fromSafe));
  }

  @ParameterizedTest
  @MethodSource("allDecompressors")
  public void incompleteInput(FallibleDecompressor decompressor) {
    byte[] input = {
      (byte) 0xf0,
      -1, -1, -1, -1, -1, -1, -1, -1, 0
    };
    byte[] output = new byte[2055];
    assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
  }

  @ParameterizedTest
  @MethodSource("decompressorsWithLength")
  public void impossibleDeclaredLength(LZ4DecompressorWithLength decompressor) {
    byte[] input = {
      0, 0, 0x10, 0, // 1 MiB decompressed length
      0 // empty LZ4 block
    };
    LZ4Exception exception = assertThrows(LZ4Exception.class, () -> decompressor.decompress(input));
    assertEquals("Decompressed length 1048576 exceeds maximum compression ratio of 255 for compressed length 1",
      exception.getMessage());

    Arrays.fill(input, 0, 4, (byte) 0xff); // negative decompressed length
    exception = assertThrows(LZ4Exception.class, () -> decompressor.decompress(input));
    assertEquals("Invalid decompressed length: -1", exception.getMessage());

    int declaredLength = 64 * 1024 * 1024 + 1;
    byte[] defaultLimitInput = new byte[4 + (declaredLength + 254) / 255];
    defaultLimitInput[0] = (byte) declaredLength;
    defaultLimitInput[1] = (byte) (declaredLength >>> 8);
    defaultLimitInput[2] = (byte) (declaredLength >>> 16);
    defaultLimitInput[3] = (byte) (declaredLength >>> 24);
    exception = assertThrows(LZ4Exception.class, () -> decompressor.decompress(defaultLimitInput));
    assertEquals("Decompressed length 67108865 exceeds configured maximum 67108864", exception.getMessage());
  }

  @ParameterizedTest
  @MethodSource("decompressorsWithLength")
  public void destinationBufferLimitsDecompressedLength(LZ4DecompressorWithLength decompressor) {
    LZ4Factory factory = LZ4Factory.safeInstance();
    byte[] compressed = new LZ4CompressorWithLength(factory.fastCompressor()).compress(new byte[16]);

    byte[] destination = new byte[16];
    LZ4Exception exception = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(compressed, 0, destination, 1));
    assertEquals("Decompressed length 16 exceeds destination length 15", exception.getMessage());

    ByteBuffer movingSrc = ByteBuffer.wrap(compressed);
    ByteBuffer movingDest = ByteBuffer.allocate(15);
    exception = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(movingSrc, movingDest));
    assertEquals("Decompressed length 16 exceeds destination length 15", exception.getMessage());
    assertEquals(0, movingSrc.position());
    assertEquals(0, movingDest.position());

    ByteBuffer indexedSrc = ByteBuffer.wrap(compressed);
    ByteBuffer indexedDest = ByteBuffer.allocate(16);
    exception = assertThrows(LZ4Exception.class,
      () -> decompressor.decompress(indexedSrc, 0, indexedDest, 1));
    assertEquals("Decompressed length 16 exceeds destination length 15", exception.getMessage());
    assertEquals(0, indexedSrc.position());
    assertEquals(0, indexedDest.position());
  }

  @ParameterizedTest
  @MethodSource("limitedDecompressorsWithLength")
  public void configuredMaximumOnlyLimitsAllocatedOutput(LZ4DecompressorWithLength decompressor) {
    LZ4Factory factory = LZ4Factory.safeInstance();
    byte[] compressed = new LZ4CompressorWithLength(factory.fastCompressor()).compress(new byte[16]);

    LZ4Exception exception = assertThrows(LZ4Exception.class, () -> decompressor.decompress(compressed));
    assertEquals("Decompressed length 16 exceeds configured maximum 15", exception.getMessage());

    byte[] destination = new byte[16];
    decompressor.decompress(compressed, destination);
    assertArrayEquals(new byte[16], destination);
  }

  @Test
  public void configuredMaximumValidation() {
    LZ4Factory factory = LZ4Factory.safeInstance();
    assertThrows(IllegalArgumentException.class,
      () -> new LZ4DecompressorWithLength(factory.fastDecompressor(), -1));
    assertThrows(IllegalArgumentException.class,
      () -> new LZ4DecompressorWithLength(factory.safeDecompressor(), -1));

    byte[] compressed = new LZ4CompressorWithLength(factory.fastCompressor()).compress(new byte[0]);
    assertArrayEquals(new byte[0],
      new LZ4DecompressorWithLength(factory.fastDecompressor(), 0).decompress(compressed));
    assertArrayEquals(new byte[0],
      new LZ4DecompressorWithLength(factory.safeDecompressor(), 0).decompress(compressed));

    compressed = new LZ4CompressorWithLength(factory.fastCompressor()).compress(new byte[16]);
    assertArrayEquals(new byte[16],
      new LZ4DecompressorWithLength(factory.fastDecompressor(), 16).decompress(compressed));
    assertArrayEquals(new byte[16],
      new LZ4DecompressorWithLength(factory.safeDecompressor(), 16).decompress(compressed));
  }

  @ParameterizedTest
  @MethodSource("fastDecompressors")
  public void beyondBufferCapacity(LZ4FastDecompressor fastDecompressor) {
    byte[] compressed = {
      // one frame with 4x literal 0x77 and a copy of the same
      (byte) 0x40,
      0x77, 0x77, 0x77, 0x77,
      0x04,
      0x00,
      // one frame with 8x literal 0x66
      (byte) 0x80,
      0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
      0x00,
      0x00
    };
    byte[] output = new byte[16];

    // normal decompression. so far so good.
    fastDecompressor.decompress(ByteBuffer.wrap(compressed), ByteBuffer.wrap(output));
    assertEquals(0x77, output[0]);
    assertEquals(0x66, output[8]);

    // but if we only pass half the input size, we should get an error
    assertThrows(LZ4Exception.class, () -> fastDecompressor.decompress(ByteBuffer.wrap(compressed, 0, 7).slice(), 0, ByteBuffer.wrap(output), 0, 16));
  }

  // Note: For JNI decompressor this might not actually overflow because it uses larger variable types,
  // but instead fails because the input is incomplete
  @ParameterizedTest
  @MethodSource("allDecompressors")
  public void literalLenOverflow(FallibleDecompressor decompressor) {
    ByteArrayOutputStream inputWriter = new ByteArrayOutputStream();
    // Token
    inputWriter.write((byte) 0b1111_0000);
    // Causes overflow for `literalLen`
    byte[] literalLenBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
    Arrays.fill(literalLenBytes, (byte) 255);
    inputWriter.writeBytes(literalLenBytes);
    inputWriter.write(1);

    inputWriter.writeBytes(new byte[20]);

    byte[] input = inputWriter.toByteArray();
    byte[] output = new byte[2055];
    assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
  }

  // Note: For JNI decompressor this might not actually overflow because it uses larger variable types,
  // but instead fails because the input is incomplete
  @ParameterizedTest
  @MethodSource("allDecompressors")
  public void matchLenOverflow(FallibleDecompressor decompressor) {
    ByteArrayOutputStream inputWriter = new ByteArrayOutputStream();
    // Token
    inputWriter.write((byte) 0b0000_1111);
    // matchDec
    inputWriter.writeBytes(new byte[2]);

    // Causes overflow for `matchLen`
    byte[] matchLenBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
    Arrays.fill(matchLenBytes, (byte) 255);
    inputWriter.writeBytes(matchLenBytes);
    inputWriter.write(1);

    // `matchLen` overflow only causes out-of-bounds access during next iteration

    // Token
    inputWriter.write(255);
    // Arbitrary literal data
    inputWriter.writeBytes(new byte[1000]);

    byte[] input = inputWriter.toByteArray();
    byte[] output = new byte[2055];
    assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
  }

  static Stream<Object[]> copyBeyondOutputInputs() {
    return allDecompressors()
      .flatMap(decompressor ->
        IntStream.range(0, 14).boxed().flatMap(dec ->
          IntStream.range(dec, 14).mapToObj(len -> new Object[]{decompressor, dec.byteValue(), len})));
  }

  @ParameterizedTest
  @MethodSource("copyBeyondOutputInputs")
  public void copyBeyondOutput(FallibleDecompressor decompressor, byte dec, int len) {
    byte[] compressed = {
      // padding frame (14 bytes)
      (byte) 0xe0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // copy len bytes (+ 4 MIN_MATCH) from -dec
      (byte) len, dec, 0,
      // padding frame (12 bytes)
      (byte) 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };
    byte[] output = new byte[14 + MIN_MATCH + len + MIN_MATCH + 12];
    Arrays.fill(output, (byte) 0x77);

    decompressor.decompress(compressed, output);

    assertArrayEquals(new byte[output.length], output); // should be all zero
  }
}
