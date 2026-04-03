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

#define LZ4_STATIC_LINKING_ONLY // exposes LZ4_compress_fast_extState_fastReset
#include "lz4.h"
#define LZ4_HC_STATIC_LINKING_ONLY // exposes LZ4_compress_HC_extStateHC_fastReset
#include "lz4hc.h"
#include <jni.h>

static jclass OutOfMemoryError;
static jclass IllegalArgumentException;
static jclass IllegalStateException;

static int init_class_ref(JNIEnv *env, jclass *target, const char *className) {
  jclass localClass = (*env)->FindClass(env, className);
  if (localClass == NULL) {
    return 0;
  }

  *target = (jclass) (*env)->NewGlobalRef(env, localClass);
  (*env)->DeleteLocalRef(env, localClass);
  return *target != NULL;
}

/*
 * Class:     net_jpountz_lz4_LZ4
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_init
  (JNIEnv *env, jclass cls) {
  if (!init_class_ref(env, &OutOfMemoryError, "java/lang/OutOfMemoryError")) {
    return;
  }
  if (!init_class_ref(env, &IllegalArgumentException, "java/lang/IllegalArgumentException")) {
    return;
  }
  if (!init_class_ref(env, &IllegalStateException, "java/lang/IllegalStateException")) {
    return;
  }
}

static void throw_OOM(JNIEnv *env) {
  (*env)->ThrowNew(env, OutOfMemoryError, "Out of memory");
}

static void throw_IAE(JNIEnv *env, const char* msg) {
  (*env)->ThrowNew(env, IllegalArgumentException, msg);
}

static void throw_ISE(JNIEnv *env, const char* msg) {
  (*env)->ThrowNew(env, IllegalStateException, msg);
}

/**
 * Validates that offset and length are non-negative and don't overflow.
 * Returns 1 if valid, 0 otherwise (and throws exception).
 */
static int validate_range(JNIEnv *env, jint off, jint len) {
  if (off < 0) {
    throw_IAE(env, "Offset must be non-negative");
    return 0;
  }

  if (len < 0) {
    throw_IAE(env, "Length must be non-negative");
    return 0;
  }

  // Check for integer overflow: off + len
  if (len > 0 && off > INT32_MAX - len) {
    throw_IAE(env, "Offset + length would overflow");
    return 0;
  }

  return 1;
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_limitedOutput
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 *
 * Though LZ4_compress_limitedOutput is no longer called as it was deprecated,
 * keep the method name of LZ4_compress_limitedOutput for backward compatibility,
 * so that the old JNI bindings in src/resources can still be used.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1limitedOutput
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_default(in + srcOff, out + destOff, srcLen, maxDestLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compressHC
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;III)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compressHC
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen, jint compressionLevel) {

  char* in;
  char* out;
  jint compressed;
  
  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_HC(in + srcOff, out + destOff, srcLen, maxDestLen, compressionLevel);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_decompress_fast
 * Signature: ([BLjava/nio/ByteBuffer;I[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1decompress_1fast
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jbyteArray destArray, jobject destBuffer, jint destOff, jint destLen) {

  char* in;
  char* out;
  jint compressed;
  
  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  } 
  
  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 
    
  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  } 
  
  if (out == NULL) {
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_decompress_fast(in + srcOff, out + destOff, destLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_decompress_safe
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1decompress_1safe
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  char* in;
  char* out;
  jint decompressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  } 
  
  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 
    
  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  } 
  
  if (out == NULL) {
    throw_OOM(env);
    return 0;
  }

  decompressed = LZ4_decompress_safe(in + srcOff, out + destOff, srcLen, maxDestLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return decompressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compressBound
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compressBound
  (JNIEnv *env, jclass cls, jint len) {

  return LZ4_compressBound(len);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_createStream
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1createStream
  (JNIEnv *env, jclass cls) {

  LZ4_stream_t* stream = LZ4_createStream();
  return (jlong)(intptr_t)stream;
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_freeStream
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1freeStream
  (JNIEnv *env, jclass cls, jlong streamPtr) {

  if (streamPtr == 0) {
    return 0;
  }

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  return LZ4_freeStream(stream);
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_fast_extState_fastReset
 * Signature: (J[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;III)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1fast_1extState_1fastReset
  (JNIEnv *env, jclass cls, jlong statePtr,
  jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
  jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen, jint acceleration) {

  if (statePtr == 0) {
    throw_ISE(env, "Compressor state has been freed");
    return 0;
  }

  if (!validate_range(env, srcOff, srcLen) || !validate_range(env, destOff, maxDestLen)) {
    return 0;
  }

  void* state = (void*)(intptr_t)statePtr;
  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_fast_extState_fastReset(state, in + srcOff, out + destOff, srcLen, maxDestLen, acceleration);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_createStreamHC
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1createStreamHC
  (JNIEnv *env, jclass cls) {

  LZ4_streamHC_t* stream = LZ4_createStreamHC();
  return (jlong)(intptr_t)stream;
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_freeStreamHC
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1freeStreamHC
  (JNIEnv *env, jclass cls, jlong streamPtr) {

  if (streamPtr == 0) {
    return 0;
  }

  LZ4_streamHC_t* stream = (LZ4_streamHC_t*)(intptr_t)streamPtr;
  return LZ4_freeStreamHC(stream);
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_HC_extStateHC_fastReset
 * Signature: (J[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;III)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1HC_1extStateHC_1fastReset
  (JNIEnv *env, jclass cls, jlong statePtr,
  jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
  jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen, jint compressionLevel) {

  if (statePtr == 0) {
    throw_ISE(env, "Compressor state has been freed");
    return 0;
  }

  if (!validate_range(env, srcOff, srcLen) || !validate_range(env, destOff, maxDestLen)) {
    return 0;
  }

  void* state = (void*)(intptr_t)statePtr;
  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_HC_extStateHC_fastReset(state, in + srcOff, out + destOff, srcLen, maxDestLen, compressionLevel);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;
}
