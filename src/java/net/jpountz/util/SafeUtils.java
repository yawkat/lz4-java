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

/** <b>FOR INTERNAL USE ONLY</b> */
public enum SafeUtils {
  ;

  public static void checkRange(byte[] buf, int off) {
    if (off < 0 || off >= buf.length) {
      throw new ArrayIndexOutOfBoundsException(off);
    }
  }

  public static void checkRange(byte[] buf, int off, int len) {
    checkLength(len);
    // buf.length intentionally performs the null check even when len is 0.
    if (off < 0 || off > buf.length - len) {
      throw new ArrayIndexOutOfBoundsException(off);
    }
  }

  public static void checkLength(int len) {
    if (len < 0) {
      throw new IllegalArgumentException("lengths must be >= 0");
    }
  }

  public static byte readByte(byte[] buf, int i) {
    return buf[i];
  }

  public static int readIntBE(byte[] buf, int i) {
    return ByteArrays.readIntBE(buf, i);
  }

  public static int readIntLE(byte[] buf, int i) {
    return ByteArrays.readIntLE(buf, i);
  }

  public static int readInt(byte[] buf, int i) {
    return ByteArrays.readInt(buf, i);
  }

  public static long readLongLE(byte[] buf, int i) {
    return ByteArrays.readLongLE(buf, i);
  }

  public static void writeShortLE(byte[] buf, int off, int v) {
    ByteArrays.writeShortLE(buf, off, v);
  }

  public static void writeInt(int[] buf, int off, int v) {
    buf[off] = v;
  }

  public static int readInt(int[] buf, int off) {
    return buf[off];
  }

  public static void writeByte(byte[] dest, int off, int i) {
    dest[off] = (byte) i;
  }

  public static void writeShort(short[] buf, int off, int v) {
    buf[off] = (short) v;
  }

  public static int readShortLE(byte[] buf, int i) {
    return ByteArrays.readShortLE(buf, i);
  }

  public static int readShort(short[] buf, int off) {
    return buf[off] & 0xFFFF;
  }
}
