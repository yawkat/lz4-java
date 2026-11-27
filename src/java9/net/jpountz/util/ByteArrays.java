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
 * Java 9+ variant of src/java/net/jpountz/util/ByteArrays.java, loaded from META-INF/versions/9 of the multi-release
 * jar. VarHandle views and Arrays.mismatch read, write and compare several bytes per bounds check. The copies follow
 * LZ4UnsafeUtils, without Unsafe.
 */
public enum ByteArrays {
  ;

  private static final VarHandle SHORT_LE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

  static int readShortLE(byte[] buf, int i) {
    return (short) SHORT_LE.get(buf, i) & 0xFFFF;
  }

  static void writeShortLE(byte[] buf, int off, int v) {
    SHORT_LE.set(buf, off, (short) v);
  }

  static int readIntBE(byte[] buf, int i) {
    return (int) INT_BE.get(buf, i);
  }

  static int readIntLE(byte[] buf, int i) {
    return (int) INT_LE.get(buf, i);
  }

  /** Reads an int in native byte order. */
  static int readInt(byte[] buf, int i) {
    return (int) INT.get(buf, i);
  }

  static long readLongLE(byte[] buf, int i) {
    return (long) LONG_LE.get(buf, i);
  }

  /** Whether the 4 bytes at i equal the 4 bytes at j. */
  public static boolean readIntEquals(byte[] buf, int i, int j) {
    return (int) INT.get(buf, i) == (int) INT.get(buf, j);
  }

  public static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
    LONG.set(dest, dOff, (long) LONG.get(src, sOff));
  }

  /** The number of equal bytes at o1 and o2, scanning forward until o2 reaches limit. */
  public static int commonBytes(byte[] b, int o1, int o2, int limit) {
    final int len = limit - o2;
    if (len <= 0) {
      return 0;
    }
    final int mismatch = Arrays.mismatch(b, o1, o1 + len, b, o2, limit);
    return mismatch < 0 ? len : mismatch;
  }

  /**
   * Copies an LZ4 match: dest[dOff, matchCopyEnd) from dest[matchOff, ...), where the ranges may overlap and
   * matchOff &lt; dOff. Writes up to 7 bytes past matchCopyEnd.
   */
  public static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
    // An 8-byte copy reads bytes it has not written yet while the distance is below 8. Write the start of the repeating
    // pattern first, so that matchOff can trail dOff by a multiple of the pattern length of at least 8: 8 for distances
    // 1 and 2, 9 for 3, and twice the distance for 4 to 7.
    final int distance = dOff - matchOff;
    assert distance > 0 : distance;
    if (distance < 4) {
      for (int i = 0; i < 4; ++i) {
        dest[dOff + i] = dest[matchOff + i];
      }
      int dec = 0;
      switch (distance) {
        case 1:
          matchOff -= 3;
          break;
        case 2:
          matchOff -= 2;
          break;
        case 3:
          matchOff -= 3;
          dec = -1;
          break;
      }
      dOff += 4;
      matchOff += 4;
      INT.set(dest, dOff, (int) INT.get(dest, matchOff));
      dOff += 4;
      matchOff -= dec;
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
