package net.jpountz.util;

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

/**
 * <b>FOR INTERNAL USE ONLY</b>
 * <p>
 * Not part of the public API: it is public only because net.jpountz.lz4 uses it, and it may change or go away in any
 * release.
 * <p>
 * Multi-byte reads, writes, copies and comparisons on byte arrays for the safe implementations. This is the only class
 * with a Java 9+ variant: src/java9/net/jpountz/util/ByteArrays.java, loaded from META-INF/versions/9 of the
 * multi-release jar. Keep the two in sync, and keep everything else out of them. The byte loops themselves are in
 * {@link PortableByteArrays}, which the Java 9+ variant also uses where unaligned access is not allowed.
 */
public final class ByteArrays {

  private ByteArrays() {}

  static int readShortLE(byte[] buf, int i) {
    return PortableByteArrays.readShortLE(buf, i);
  }

  static void writeShortLE(byte[] buf, int off, int v) {
    PortableByteArrays.writeShortLE(buf, off, v);
  }

  static int readIntBE(byte[] buf, int i) {
    return PortableByteArrays.readIntBE(buf, i);
  }

  static int readIntLE(byte[] buf, int i) {
    return PortableByteArrays.readIntLE(buf, i);
  }

  /** Reads an int in native byte order. */
  static int readInt(byte[] buf, int i) {
    return PortableByteArrays.readInt(buf, i);
  }

  static long readLongLE(byte[] buf, int i) {
    return PortableByteArrays.readLongLE(buf, i);
  }

  /** Whether the 4 bytes at i equal the 4 bytes at j. All 8 bytes must be in bounds. */
  public static boolean readIntEquals(byte[] buf, int i, int j) {
    return PortableByteArrays.readIntEquals(buf, i, j);
  }

  /**
   * Copies 8 bytes from src[sOff] to dest[dOff]. Both ranges must be in bounds, and must not overlap if src and dest
   * are the same array: the Java 7 variant copies one byte at a time, the Java 9 variant may read all 8 bytes first.
   */
  public static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
    PortableByteArrays.copy8Bytes(src, sOff, dest, dOff);
  }

  /**
   * The number of equal bytes at o1 and o2, scanning forward until o2 reaches limit. Requires o1 &lt; o2 and
   * limit &lt;= b.length: the Java 9 variant checks the whole range up front, not only the bytes it compares.
   */
  public static int commonBytes(byte[] b, int o1, int o2, int limit) {
    return PortableByteArrays.commonBytes(b, o1, o2, limit);
  }

  /**
   * Copies an LZ4 match: dest[dOff, matchCopyEnd) from dest[matchOff, ...), where the ranges may overlap. Requires
   * matchOff &lt; dOff and matchCopyEnd - dOff &gt;= 4 (the minimum match length), and writes up to 7 bytes past
   * matchCopyEnd, which must be in bounds. The bytes written past matchCopyEnd differ between the variants.
   */
  public static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
    PortableByteArrays.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
  }
}
