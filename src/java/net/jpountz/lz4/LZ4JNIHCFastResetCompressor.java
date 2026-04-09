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

import static net.jpountz.lz4.LZ4Constants.DEFAULT_COMPRESSION_LEVEL;
import static net.jpountz.lz4.LZ4Constants.MAX_COMPRESSION_LEVEL;

import java.nio.ByteBuffer;

/**
 * An optimized LZ4 HC compressor that uses native {@code LZ4_compress_HC_extStateHC_fastReset}.
 * <p>
 * This compressor pre-allocates an {@code LZ4_streamHC_t} once and reuses it for every compression
 * call through {@code LZ4_compress_HC_extStateHC_fastReset}. This avoids the expensive full
 * state initialization that {@link LZ4HCJNICompressor LZ4_compress_HC()} performs on every call.
 * <p>
 * Each compression call is independent, making the output identical to {@link LZ4HCJNICompressor}
 * for the same compression level when operating on array-backed or direct buffers.
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
 * try (LZ4JNIHCFastResetCompressor compressor = factory.highFastResetCompressor()) {
 *     byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
 *     int compressedLen = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
 *     // ... use compressed[0..compressedLen-1]
 * }
 * }</pre>
 *
 * @see LZ4Factory#highFastResetCompressor()
 * @see LZ4Factory#highFastResetCompressor(int)
 * @see LZ4HCJNICompressor
 */
public final class LZ4JNIHCFastResetCompressor extends AbstractLZ4JNIFastResetCompressor {

  private final int compressionLevel;

  /**
   * Creates a new HC fast-reset compressor with default compression level.
   * Package-private: use {@link LZ4Factory#highFastResetCompressor()}.
   */
  LZ4JNIHCFastResetCompressor() {
    this(DEFAULT_COMPRESSION_LEVEL);
  }

  /**
   * Creates a new HC fast-reset compressor with specified compression level.
   * Package-private: use {@link LZ4Factory#highFastResetCompressor(int)}.
   *
   * @param compressionLevel compression level (1-17, higher = better compression)
   */
  LZ4JNIHCFastResetCompressor(int compressionLevel) {
    super(LZ4JNI.LZ4_createStreamHC(), "Failed to allocate LZ4 HC state");
    if (compressionLevel > MAX_COMPRESSION_LEVEL) {
      compressionLevel = MAX_COMPRESSION_LEVEL;
    } else if (compressionLevel < 1) {
      compressionLevel = DEFAULT_COMPRESSION_LEVEL;
    }
    this.compressionLevel = compressionLevel;
  }

  /**
   * Returns the compression level.
   *
   * @return compression level (default = 9)
   */
  public int getCompressionLevel() {
    return compressionLevel;
  }

  @Override
  protected int compressNative(long ptr, byte[] srcArr, ByteBuffer srcBuf, int srcOff, int srcLen,
                               byte[] destArr, ByteBuffer destBuf, int destOff, int maxDestLen) {
    return LZ4JNI.LZ4_compress_HC_extStateHC_fastReset(
      ptr, srcArr, srcBuf, srcOff, srcLen,
      destArr, destBuf, destOff, maxDestLen, compressionLevel);
  }

  @Override
  protected void freeState(long ptr) {
    LZ4JNI.LZ4_freeStreamHC(ptr);
  }

  @Override
  public String toString() {
    return "LZ4JNIHCFastResetCompressor[compressionLevel=" + compressionLevel + ", closed=" + isClosed() + "]";
  }
}
