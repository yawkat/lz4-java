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

import static net.jpountz.lz4.LZ4Constants.MIN_ACCELERATION;
import static net.jpountz.lz4.LZ4Constants.MAX_ACCELERATION;

/**
 * An optimized LZ4 compressor that uses native {@code LZ4_compress_fast_extState_fastReset}.
 * <p>
 * This compressor pre-allocates an {@code LZ4_stream_t} once and reuses it for every compression
 * call through {@code LZ4_compress_fast_extState_fastReset}. This avoids the expensive full
 * state initialization that {@link LZ4JNICompressor LZ4_compress_default()} performs on every call.
 * <p>
 * Each compression call is independent, making the output identical to {@link LZ4JNICompressor}
 * for the same acceleration level when operating on array-backed or direct buffers.
 * {@link ByteBuffer} inputs must be array-backed or direct.
 * <p>
 * <b>Thread Safety:</b> This class is <b>NOT</b> thread-safe. Each instance holds
 * mutable native state and must be used by only one thread at a time. Concurrent use
 * or close attempts fail fast with {@link IllegalStateException}.
 * <p>
 * <b>Resource Management:</b> This class holds native memory that must be freed.
 * Always use try-with-resources or explicitly call {@link #close()}.
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * LZ4Factory factory = LZ4Factory.nativeInstance();
 * try (LZ4JNIFastResetCompressor compressor = factory.fastResetCompressor()) {
 *     byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
 *     int compressedLen = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
 *     // ... use compressed[0..compressedLen-1]
 * }
 * }</pre>
 *
 * @see LZ4Factory#fastResetCompressor()
 * @see LZ4Factory#fastResetCompressor(int)
 * @see LZ4JNICompressor
 */
public final class LZ4JNIFastResetCompressor extends LZ4Compressor implements AutoCloseable {

  private static final String IN_USE_ERROR = "This compressor is not thread-safe and is already in use";
  private static final String CLOSED_ERROR = "Compressor has been closed";
  private static final String UNSUPPORTED_BUFFER_ERROR = "ByteBuffer must be direct or array-backed";

  private final ReentrantLock lock = new ReentrantLock();
  private long statePtr;
  private final int acceleration;

  /**
   * Creates a new fast-reset compressor with default acceleration (1).
   * Package-private: use {@link LZ4Factory#fastResetCompressor()}.
   */
  LZ4JNIFastResetCompressor() {
    this(LZ4Constants.DEFAULT_ACCELERATION);
  }

  /**
   * Creates a new fast-reset compressor with specified acceleration.
   * Package-private: use {@link LZ4Factory#fastResetCompressor(int)}.
   *
   * @param acceleration acceleration factor (1 = default, higher = faster but less compression)
   */
  LZ4JNIFastResetCompressor(int acceleration) {
    if (acceleration < MIN_ACCELERATION) {
      acceleration = MIN_ACCELERATION;
    } else if (acceleration > MAX_ACCELERATION) {
      acceleration = MAX_ACCELERATION;
    }
    this.acceleration = acceleration;
    long ptr = LZ4JNI.LZ4_createStream();
    if (ptr == 0) {
      throw new LZ4Exception("Failed to allocate LZ4 state");
    }
    this.statePtr = ptr;
  }

  /**
   * Returns the acceleration factor.
   *
   * @return acceleration factor (1 = default)
   */
  public int getAcceleration() {
    return acceleration;
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
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      if (statePtr == 0) {
        throw new IllegalStateException(CLOSED_ERROR);
      }
      SafeUtils.checkRange(src, srcOff, srcLen);
      SafeUtils.checkRange(dest, destOff, maxDestLen);

      final int result = LZ4JNI.LZ4_compress_fast_extState_fastReset(
        statePtr, src, null, srcOff, srcLen,
        dest, null, destOff, maxDestLen, acceleration);

      if (result <= 0) {
        throw new LZ4Exception("maxDestLen is too small");
      }
      return result;
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
  public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      if (statePtr == 0) {
        throw new IllegalStateException(CLOSED_ERROR);
      }
      checkByteBuffer(src);
      checkByteBuffer(dest);
      ByteBufferUtils.checkNotReadOnly(dest);
      ByteBufferUtils.checkRange(src, srcOff, srcLen);
      ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

      return compressNativeBuffers(statePtr, src, srcOff, srcLen, dest, destOff, maxDestLen);
    } finally {
      lock.unlock();
    }
  }

  private int compressNativeBuffers(long ptr, ByteBuffer src, int srcOff, int srcLen,
                                    ByteBuffer dest, int destOff, int maxDestLen) {
    byte[] srcArr = src.hasArray() ? src.array() : null;
    byte[] destArr = dest.hasArray() ? dest.array() : null;
    ByteBuffer srcBuf = srcArr == null ? src : null;
    ByteBuffer destBuf = destArr == null ? dest : null;
    int srcBufferOff = srcOff + (srcArr != null ? src.arrayOffset() : 0);
    int destBufferOff = destOff + (destArr != null ? dest.arrayOffset() : 0);

    final int result = LZ4JNI.LZ4_compress_fast_extState_fastReset(
      ptr, srcArr, srcBuf, srcBufferOff, srcLen,
      destArr, destBuf, destBufferOff, maxDestLen, acceleration);

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

  /**
   * Returns true if this compressor has been closed.
   *
   * @return true if closed
   */
  public boolean isClosed() {
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
  public void close() {
    if (!lock.tryLock()) {
      throw new IllegalStateException(IN_USE_ERROR);
    }

    try {
      long ptr = statePtr;
      statePtr = 0;
      if (ptr != 0) {
        LZ4JNI.LZ4_freeStream(ptr);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return "LZ4JNIFastResetCompressor[acceleration=" + acceleration + ", closed=" + isClosed() + "]";
  }
}
