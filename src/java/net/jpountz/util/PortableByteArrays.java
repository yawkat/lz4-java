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

import java.nio.ByteOrder;

/**
 * The {@link ByteArrays} methods as byte-at-a-time loops, which work on any JVM and CPU. The Java 7 ByteArrays uses
 * them for everything, and the Java 9+ ByteArrays where {@link Utils#isUnalignedAccessAllowed()} is false. See
 * ByteArrays for what each method requires.
 */
final class PortableByteArrays {

  private PortableByteArrays() {}

  static int readShortLE(byte[] buf, int i) {
    return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8);
  }

  static void writeShortLE(byte[] buf, int off, int v) {
    buf[off] = (byte) v;
    buf[off + 1] = (byte) (v >>> 8);
  }

  static int readIntBE(byte[] buf, int i) {
    return ((buf[i] & 0xFF) << 24) | ((buf[i + 1] & 0xFF) << 16) | ((buf[i + 2] & 0xFF) << 8) | (buf[i + 3] & 0xFF);
  }

  static int readIntLE(byte[] buf, int i) {
    return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) | ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
  }

  static int readInt(byte[] buf, int i) {
    if (Utils.NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      return readIntBE(buf, i);
    } else {
      return readIntLE(buf, i);
    }
  }

  static long readLongLE(byte[] buf, int i) {
    return (buf[i] & 0xFFL) | ((buf[i + 1] & 0xFFL) << 8) | ((buf[i + 2] & 0xFFL) << 16) | ((buf[i + 3] & 0xFFL) << 24)
      | ((buf[i + 4] & 0xFFL) << 32) | ((buf[i + 5] & 0xFFL) << 40) | ((buf[i + 6] & 0xFFL) << 48) | ((buf[i + 7] & 0xFFL) << 56);
  }

  static boolean readIntEquals(byte[] buf, int i, int j) {
    return buf[i] == buf[j] && buf[i + 1] == buf[j + 1] && buf[i + 2] == buf[j + 2] && buf[i + 3] == buf[j + 3];
  }

  static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
    for (int i = 0; i < 8; ++i) {
      dest[dOff + i] = src[sOff + i];
    }
  }

  static int commonBytes(byte[] b, int o1, int o2, int limit) {
    int count = 0;
    while (o2 < limit && b[o1++] == b[o2++]) {
      ++count;
    }
    return count;
  }

  static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
    do {
      copy8Bytes(dest, matchOff, dest, dOff);
      matchOff += 8;
      dOff += 8;
    } while (dOff < matchCopyEnd);
  }
}
