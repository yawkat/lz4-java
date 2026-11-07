package net.jpountz.fuzz;

/*
 * Copyright 2026 Dominik Opwis and the lz4-java contributors.
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

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.DictionaryEntries;
import com.code_intelligence.jazzer.junit.FuzzTest;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.xxhash.XXHashFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Feeds raw fuzz input to LZ4FrameInputStream and LZ4BlockInputStream through a mix of read(), read(byte[]), skip()
 * and available() calls. Only IOException may escape, results must respect the InputStream contract, end of stream
 * and failures must be sticky, and the stream must not allocate much more than its input justifies.
 */
public class LZ4InputStreamTest {
  // Each of the first OP_COUNT input bytes picks a call and its size. The calls repeat in order until the stream ends.
  private static final int OP_COUNT = 16;
  // Calls made after the stream ended or failed, to check that it stays that way
  private static final int CALLS_AFTER_END = 2 * OP_COUNT;
  // Bounds runs whose calls make no progress, such as only available()
  private static final int MAX_CALLS = 1 << 16;
  private static final long ALLOCATION_BUDGET = 32L << 20;
  private static final long ALLOCATION_PER_INPUT_BYTE = 4096;

  private static final com.sun.management.ThreadMXBean THREADS =
      (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
  private static final ClassLoadingMXBean CLASSES = ManagementFactory.getClassLoadingMXBean();
  // Magic numbers, so the fuzzer gets past the stream headers
  private static final String FRAME_MAGIC = "\u0004\"M\u0018";
  private static final String SKIPPABLE_FRAME_MAGIC = "P*M\u0018";
  private static final String BLOCK_MAGIC = "LZ4Block";
  private static final byte[] READ_BUFFER = new byte[1 << 15];

  private interface StreamFactory {
    InputStream open(InputStream in) throws IOException;
  }

  private enum Outcome { DATA, END, FAILED }

  private static void drive(FuzzedDataProvider data, StreamFactory factory) throws IOException {
    byte[] ops = data.consumeBytes(OP_COUNT);
    byte[] input = data.consumeRemainingAsBytes();
    if (ops.length == 0) {
      return;
    }
    long classesBefore = CLASSES.getTotalLoadedClassCount();
    long allocatedBefore = THREADS.getCurrentThreadAllocatedBytes();
    InputStream stream = factory.open(new ByteArrayInputStream(input));
    Outcome state = Outcome.DATA;
    int callsAfterEnd = 0;
    for (int call = 0; call < MAX_CALLS && callsAfterEnd < CALLS_AFTER_END; call++) {
      if (state != Outcome.DATA) {
        callsAfterEnd++;
      }
      Outcome outcome = call(stream, ops[call % ops.length], state);
      if (state == Outcome.DATA) {
        state = outcome;
      }
    }
    stream.close();
    long allocated = THREADS.getCurrentThreadAllocatedBytes() - allocatedBefore;
    // Loading and instrumenting a class allocates on this thread too, so only runs that load none are checked
    if (CLASSES.getTotalLoadedClassCount() == classesBefore) {
      long budget = ALLOCATION_BUDGET + ALLOCATION_PER_INPUT_BYTE * input.length;
      assertTrue(allocated <= budget, "allocated " + allocated + " bytes for " + input.length + " input bytes");
    }
  }

  /**
   * Makes one call. The low 2 bits of op pick it, the rest its size. Returns END once the stream reports its end.
   */
  private static Outcome call(InputStream stream, byte op, Outcome state) {
    int sizeBits = (op & 0xFF) >>> 2;
    int size = sizeBits == 0 ? 0 : 1 << (sizeBits % 16);
    if (state == Outcome.FAILED) {
      // A failed stream must keep failing, and report nothing available
      switch (op & 3) {
        case 0:
          assertThrows(IOException.class, stream::read);
          break;
        case 1:
          assertThrows(IOException.class, () -> stream.read(READ_BUFFER, 0, size));
          break;
        case 2:
          assertThrows(IOException.class, () -> stream.skip(size));
          break;
        default:
          assertEquals(0, available(stream));
      }
      return Outcome.FAILED;
    }
    // Messages are built only on failure, so that they don't count against the allocation budget
    try {
      switch (op & 3) {
        case 0: {
          int b = stream.read();
          if (b < -1 || b > 0xFF) {
            fail("read() returned " + b);
          }
          return endIf(b == -1, state);
        }
        case 1: {
          int n = stream.read(READ_BUFFER, 0, size);
          // Both streams return -1 rather than 0 for a zero-length read at the end, which is tolerated
          if (n < -1 || n > size || (size > 0 && n == 0)) {
            fail("read(byte[], 0, " + size + ") returned " + n);
          }
          return size == 0 ? state : endIf(n == -1, state);
        }
        case 2: {
          long n = stream.skip(size);
          if (n < 0 || n > size || (state == Outcome.END && n != 0)) {
            fail("skip(" + size + ") returned " + n + (state == Outcome.END ? " after the end of the stream" : ""));
          }
          return state;
        }
        default:
          int n = stream.available();
          if (n < 0 || (state == Outcome.END && n != 0)) {
            fail("available() returned " + n + (state == Outcome.END ? " after the end of the stream" : ""));
          }
          return state;
      }
    } catch (IOException e) {
      if (state == Outcome.END) {
        fail("IOException after the end of the stream", e);
      }
      return Outcome.FAILED;
    }
  }

  private static Outcome endIf(boolean end, Outcome state) {
    if (state == Outcome.END && !end) {
      fail("returned data after the end of the stream");
    }
    return end ? Outcome.END : state;
  }

  private static int available(InputStream stream) {
    try {
      return stream.available();
    } catch (IOException e) {
      return 0;
    }
  }

  private static void frame(FuzzedDataProvider data, boolean readSingleFrame) throws IOException {
    drive(data, in -> new LZ4FrameInputStream(in, LZ4Factory.safeInstance().safeDecompressor(),
        XXHashFactory.safeInstance().hash32(), readSingleFrame));
  }

  private static void block(FuzzedDataProvider data, boolean fast, boolean stopOnEmptyBlock,
                            boolean acceptOversizedBlocks) throws IOException {
    LZ4BlockInputStream.Builder builder = LZ4BlockInputStream.newBuilder()
        // 0x9747b28c is the seed LZ4BlockOutputStream writes with
        .withChecksum(XXHashFactory.safeInstance().newStreamingHash32(0x9747b28c).asChecksum())
        .withStopOnEmptyBlock(stopOnEmptyBlock)
        .withAcceptOversizedBlocks(acceptOversizedBlocks);
    if (fast) {
      builder.withDecompressor(LZ4Factory.safeInstance().fastDecompressor());
    } else {
      builder.withDecompressor(LZ4Factory.safeInstance().safeDecompressor());
    }
    drive(data, builder::build);
  }

  @FuzzTest
  @DictionaryEntries({FRAME_MAGIC, SKIPPABLE_FRAME_MAGIC})
  public void frame_multi(FuzzedDataProvider data) throws IOException {
    frame(data, false);
  }

  @FuzzTest
  @DictionaryEntries({FRAME_MAGIC, SKIPPABLE_FRAME_MAGIC})
  public void frame_single(FuzzedDataProvider data) throws IOException {
    frame(data, true);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_safe_stop(FuzzedDataProvider data) throws IOException {
    block(data, false, true, false);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_safe_nostop(FuzzedDataProvider data) throws IOException {
    block(data, false, false, false);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_safe_stop_oversized(FuzzedDataProvider data) throws IOException {
    block(data, false, true, true);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_safe_nostop_oversized(FuzzedDataProvider data) throws IOException {
    block(data, false, false, true);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_fast_stop(FuzzedDataProvider data) throws IOException {
    block(data, true, true, false);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_fast_nostop(FuzzedDataProvider data) throws IOException {
    block(data, true, false, false);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_fast_stop_oversized(FuzzedDataProvider data) throws IOException {
    block(data, true, true, true);
  }

  @FuzzTest
  @DictionaryEntries(BLOCK_MAGIC)
  public void block_fast_nostop_oversized(FuzzedDataProvider data) throws IOException {
    block(data, true, false, true);
  }
}
