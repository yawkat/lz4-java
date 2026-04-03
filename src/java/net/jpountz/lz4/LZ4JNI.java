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

import net.jpountz.util.Native;


/**
 * JNI bindings to the original C implementation of LZ4.
 */
enum LZ4JNI {
  ;

  static {
    Native.load();
    init();
  }

  static native void init();
  static native int LZ4_compress_limitedOutput(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  static native int LZ4_compressHC(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen, int compressionLevel);
  static native int LZ4_decompress_fast(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, byte[] destArray, ByteBuffer destBuffer, int destOff, int destLen);
  static native int LZ4_decompress_safe(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  static native int LZ4_compressBound(int len);

  /**
   * Creates a new LZ4 stream object.
   * <p>
   * The allocated native memory must be freed with {@link #LZ4_freeStream(long)} when no longer needed.
   *
   * @return pointer to the allocated LZ4_stream_t or 0 on failure
   */
  static native long LZ4_createStream();

  /**
   * Frees the native memory allocated for the LZ4 stream object.
   *
   * @param streamPtr pointer to LZ4_stream_t
   * @return 0 on success
   */
  static native int LZ4_freeStream(long streamPtr);

  /**
   * Compresses using a pre-initialized state with fast-reset semantics.
   * <p>
   * A variant of {@code LZ4_compress_fast_extState()} that avoids an expensive
   * full state initialization on every call.
   * <p>
   * The state must have been properly initialized once (e.g. via {@link #LZ4_createStream()}).
   *
   * @param statePtr    pointer to a properly initialized LZ4_stream_t
   * @param srcArray    source data (byte array), or null if using srcBuffer
   * @param srcBuffer   source data (direct ByteBuffer), or null if using srcArray
   * @param srcOff      offset in source
   * @param srcLen      length to compress
   * @param destArray   destination buffer (byte array), or null if using destBuffer
   * @param destBuffer  destination buffer (direct ByteBuffer), or null if using destArray
   * @param destOff     offset in destination
   * @param maxDestLen  maximum bytes to write
   * @param acceleration acceleration factor (1 = default, higher = faster but less compression)
   * @return compressed size, or 0 on failure
   */
  static native int LZ4_compress_fast_extState_fastReset(long statePtr,
                                                         byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen,
                                                         byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen,
                                                         int acceleration);

  /**
   * Creates a new LZ4 HC stream object.
   * <p>
   * The allocated native memory must be freed with {@link #LZ4_freeStreamHC(long)} when no longer needed.
   *
   * @return pointer to the allocated LZ4_streamHC_t or 0 on failure
   */
  static native long LZ4_createStreamHC();

  /**
   * Frees the native memory allocated for the LZ4 HC stream object.
   *
   * @param streamPtr pointer to LZ4_streamHC_t
   * @return 0 on success
   */
  static native int LZ4_freeStreamHC(long streamPtr);

  /**
   * Compresses using a pre-initialized HC state with fast-reset semantics.
   * <p>
   * A variant of {@code LZ4_compress_HC_extStateHC()} that avoids an expensive
   * full state initialization on every call.
   * <p>
   * The state must have been properly initialized once (e.g. via {@link #LZ4_createStreamHC()}).
   *
   * @param statePtr         pointer to a properly initialized LZ4_streamHC_t
   * @param srcArray         source data (byte array), or null if using srcBuffer
   * @param srcBuffer        source data (direct ByteBuffer), or null if using srcArray
   * @param srcOff           offset in source
   * @param srcLen           length to compress
   * @param destArray        destination buffer (byte array), or null if using destBuffer
   * @param destBuffer       destination buffer (direct ByteBuffer), or null if using destArray
   * @param destOff          offset in destination
   * @param maxDestLen       maximum bytes to write
   * @param compressionLevel compression level (1-17, higher = better compression)
   * @return compressed size, or 0 on failure
   */
  static native int LZ4_compress_HC_extStateHC_fastReset(long statePtr,
                                                         byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen,
                                                         byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen,
                                                         int compressionLevel);

}

