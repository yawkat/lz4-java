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

import net.jpountz.util.ByteBufferUtils;
import net.jpountz.util.SafeUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

abstract class AbstractLZ4JNIFastResetCompressor extends LZ4Compressor implements AutoCloseable {

  private static final String IN_USE_ERROR = "This compressor is not thread-safe and is already in use";
  private static final String CLOSED_ERROR = "Compressor has been closed";
  private static final String UNSUPPORTED_BUFFER_ERROR = "ByteBuffer must be direct or array-backed";

  private final ReentrantLock lock = new ReentrantLock();
  private long statePtr;

  AbstractLZ4JNIFastResetCompressor(long statePtr, String allocationFailureMessage) {
    if (statePtr == 0) {
      throw new LZ4Exception(allocationFailureMessage);
    }
    this.statePtr = statePtr;
  }

  /**
   * Compresses {@code src[srcOff:srcOff+srcLen]} into
   * {@code dest[destOff:destOff+maxDestLen]}.
   *
   * @param src        source data
   * @param srcOff     the start offset in src
   * @param srcLen     the number of bytes to compress
   * @param dest       destination buffer
   * @param destOff    the start offset in dest
   * @param maxDestLen the maximum number of bytes to write in dest
   * @return the compressed size
   * @throws LZ4Exception if maxDestLen is too small
   * @throws IllegalStateException if the compressor has been closed or is already in use
   */
  @Override
  public final int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      long ptr = checkOpen();
      SafeUtils.checkRange(src, srcOff, srcLen);
      SafeUtils.checkRange(dest, destOff, maxDestLen);

      final int result = compressNative(
        ptr, src, null, srcOff, srcLen,
        dest, null, destOff, maxDestLen);
      return checkResult(result);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Compresses {@code src[srcOff:srcOff+srcLen]} into
   * {@code dest[destOff:destOff+maxDestLen]}.
   * <p>
   * Both buffers must be either direct or array-backed.
   * {@link ByteBuffer} positions remain unchanged.
   *
   * @param src        source data
   * @param srcOff     the start offset in src
   * @param srcLen     the number of bytes to compress
   * @param dest       destination buffer
   * @param destOff    the start offset in dest
   * @param maxDestLen the maximum number of bytes to write in dest
   * @return the compressed size
   * @throws LZ4Exception if maxDestLen is too small
   * @throws IllegalArgumentException if src or dest is neither array-backed nor direct
   * @throws IllegalStateException if the compressor has been closed or is already in use
   */
  @Override
  public final int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      long ptr = checkOpen();
      checkByteBuffer(src);
      checkByteBuffer(dest);
      ByteBufferUtils.checkNotReadOnly(dest);
      ByteBufferUtils.checkRange(src, srcOff, srcLen);
      ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

      byte[] srcArr = src.hasArray() ? src.array() : null;
      byte[] destArr = dest.hasArray() ? dest.array() : null;
      ByteBuffer srcBuf = srcArr == null ? src : null;
      ByteBuffer destBuf = destArr == null ? dest : null;
      int srcBufferOff = srcOff + (srcArr != null ? src.arrayOffset() : 0);
      int destBufferOff = destOff + (destArr != null ? dest.arrayOffset() : 0);

      final int result = compressNative(
        ptr, srcArr, srcBuf, srcBufferOff, srcLen,
        destArr, destBuf, destBufferOff, maxDestLen);
      return checkResult(result);
    } finally {
      lock.unlock();
    }
  }

  public final boolean isClosed() {
    lock.lock();
    try {
      return statePtr == 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Closes this compressor and releases native resources.
   * After calling this method, all compress methods will throw {@link IllegalStateException}.
   *
   * @throws IllegalStateException if the compressor is in use by another thread
   */
  @Override
  public final void close() {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      long ptr = statePtr;
      statePtr = 0;
      if (ptr != 0) {
        freeState(ptr);
      }
    } finally {
      lock.unlock();
    }
  }

  private long checkOpen() {
    if (statePtr == 0) {
      throw new IllegalStateException(CLOSED_ERROR);
    }
    return statePtr;
  }

  private static int checkResult(int result) {
    if (result <= 0) {
      throw new LZ4Exception("maxDestLen is too small");
    }
    return result;
  }

  private static void checkByteBuffer(ByteBuffer buffer) {
    if (!(buffer.hasArray() || buffer.isDirect())) {
      throw new IllegalArgumentException(UNSUPPORTED_BUFFER_ERROR);
    }
  }

  protected abstract int compressNative(long ptr, byte[] srcArr, ByteBuffer srcBuf, int srcOff, int srcLen,
                                        byte[] destArr, ByteBuffer destBuf, int destOff, int maxDestLen);

  protected abstract void freeState(long ptr);
}



