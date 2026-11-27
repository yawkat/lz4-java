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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * <b>FOR INTERNAL USE ONLY</b>
 * <p>
 * Not part of the public API: it is public only because net.jpountz.lz4 uses it, and it may change or go away in any
 * release.
 * <p>
 * Java 9+ variant of src/java/net/jpountz/util/ByteArrays.java, loaded from META-INF/versions/9 of the multi-release
 * jar. VarHandle views and Arrays.mismatch read, write and compare several bytes per bounds check. The copies follow
 * LZ4UnsafeUtils, without Unsafe.
 * <p>
 * Where {@link Utils#isUnalignedAccessAllowed()} is false, HotSpot does not compile the VarHandle views and
 * Arrays.mismatch to wide accesses, and they are slower than byte loops. There every method uses the byte loops of
 * {@link PortableByteArrays}, as the Java 7 variant does.
 */
public final class ByteArrays {

  private ByteArrays() {}

  private static final VarHandle SHORT_LE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
  private static final boolean UNALIGNED = Utils.isUnalignedAccessAllowed();

  static int readShortLE(byte[] buf, int i) {
    if (!UNALIGNED) {
      return PortableByteArrays.readShortLE(buf, i);
    }
    return (short) SHORT_LE.get(buf, i) & 0xFFFF;
  }

  static void writeShortLE(byte[] buf, int off, int v) {
    if (!UNALIGNED) {
      PortableByteArrays.writeShortLE(buf, off, v);
      return;
    }
    SHORT_LE.set(buf, off, (short) v);
  }

  static int readIntBE(byte[] buf, int i) {
    if (!UNALIGNED) {
      return PortableByteArrays.readIntBE(buf, i);
    }
    return (int) INT_BE.get(buf, i);
  }

  static int readIntLE(byte[] buf, int i) {
    if (!UNALIGNED) {
      return PortableByteArrays.readIntLE(buf, i);
    }
    return (int) INT_LE.get(buf, i);
  }

  /** Reads an int in native byte order. */
  static int readInt(byte[] buf, int i) {
    if (!UNALIGNED) {
      return PortableByteArrays.readInt(buf, i);
    }
    return (int) INT.get(buf, i);
  }

  static long readLongLE(byte[] buf, int i) {
    if (!UNALIGNED) {
      return PortableByteArrays.readLongLE(buf, i);
    }
    return (long) LONG_LE.get(buf, i);
  }

  /** Whether the 4 bytes at i equal the 4 bytes at j. All 8 bytes must be in bounds. */
  public static boolean readIntEquals(byte[] buf, int i, int j) {
    if (!UNALIGNED) {
      return PortableByteArrays.readIntEquals(buf, i, j);
    }
    return (int) INT.get(buf, i) == (int) INT.get(buf, j);
  }

  /**
   * Copies 8 bytes from src[sOff] to dest[dOff]. Both ranges must be in bounds, and must not overlap if src and dest
   * are the same array: the Java 7 variant copies one byte at a time, the Java 9 variant may read all 8 bytes first.
   */
  public static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
    if (!UNALIGNED) {
      PortableByteArrays.copy8Bytes(src, sOff, dest, dOff);
      return;
    }
    LONG.set(dest, dOff, (long) LONG.get(src, sOff));
  }

  /**
   * The number of equal bytes at o1 and o2, scanning forward until o2 reaches limit. Requires o1 &lt; o2 and
   * limit &lt;= b.length: the Java 9 variant checks the whole range up front, not only the bytes it compares.
   */
  public static int commonBytes(byte[] b, int o1, int o2, int limit) {
    final int len = limit - o2;
    if (len < 8 || !UNALIGNED) {
      return PortableByteArrays.commonBytes(b, o1, o2, limit);
    }
    // Most matches end within a few bytes, so compare one word before paying for the setup of Arrays.mismatch.
    final long diff = (long) LONG_LE.get(b, o1) ^ (long) LONG_LE.get(b, o2);
    if (diff != 0) {
      return Long.numberOfTrailingZeros(diff) >>> 3;
    }
    final int mismatch = Arrays.mismatch(b, o1 + 8, o1 + len, b, o2 + 8, limit);
    return mismatch < 0 ? len : 8 + mismatch;
  }

  /**
   * Copies an LZ4 match: dest[dOff, matchCopyEnd) from dest[matchOff, ...), where the ranges may overlap. Requires
   * matchOff &lt; dOff and matchCopyEnd - dOff &gt;= 4 (the minimum match length), and writes up to 7 bytes past
   * matchCopyEnd, which must be in bounds. The bytes written past matchCopyEnd differ between the variants.
   */
  public static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
    // An 8-byte copy reads bytes it has not written yet while the distance is below 8. Write the start of the repeating
    // pattern first, so that matchOff can trail dOff by a multiple of the pattern length of at least 8: 8 for distances
    // 1 and 2, 9 for 3, and twice the distance for 4 to 7.
    if (!UNALIGNED) {
      PortableByteArrays.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
      return;
    }
    final int distance = dOff - matchOff;
    assert distance > 0 : distance;
    if (distance < 4) {
      for (int i = 0; i < 4; ++i) {
        dest[dOff + i] = dest[matchOff + i];
      }
      // The next 4 bytes continue the pattern from matchOff + 1 (distances 1 and 3) or matchOff + 2 (distance 2).
      matchOff += distance == 2 ? 2 : 1;
      INT.set(dest, dOff + 4, (int) INT.get(dest, matchOff));
      dOff += 8;
      if (distance == 3) {
        ++matchOff;
      }
    } else if (distance < 8) {
      copy8Bytes(dest, matchOff, dest, dOff);
      dOff += distance;
    }
    while (dOff < matchCopyEnd) {
      copy8Bytes(dest, matchOff, dest, dOff);
      dOff += 8;
      matchOff += 8;
    }
  }
}
