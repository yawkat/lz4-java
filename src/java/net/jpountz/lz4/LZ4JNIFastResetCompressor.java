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

import java.nio.ByteBuffer;

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
public final class LZ4JNIFastResetCompressor extends AbstractLZ4JNIFastResetCompressor {

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
    super(LZ4JNI.LZ4_createStream(), "Failed to allocate LZ4 state");
    if (acceleration < MIN_ACCELERATION) {
      acceleration = MIN_ACCELERATION;
    } else if (acceleration > MAX_ACCELERATION) {
      acceleration = MAX_ACCELERATION;
    }
    this.acceleration = acceleration;
  }

  /**
   * Returns the acceleration factor.
   *
   * @return acceleration factor (1 = default)
   */
  public int getAcceleration() {
    return acceleration;
  }

  @Override
  protected int compressNative(long ptr, byte[] srcArr, ByteBuffer srcBuf, int srcOff, int srcLen,
                               byte[] destArr, ByteBuffer destBuf, int destOff, int maxDestLen) {
    return LZ4JNI.LZ4_compress_fast_extState_fastReset(
      ptr, srcArr, srcBuf, srcOff, srcLen,
      destArr, destBuf, destOff, maxDestLen, acceleration);
  }

  @Override
  protected void freeState(long ptr) {
    LZ4JNI.LZ4_freeStream(ptr);
  }

  @Override
  public String toString() {
    return "LZ4JNIFastResetCompressor[acceleration=" + acceleration + ", closed=" + isClosed() + "]";
  }
}
