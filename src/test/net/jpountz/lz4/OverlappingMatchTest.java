package net.jpountz.lz4;

/*
 * Copyright 2026 Jonas Konrad and the lz4-java contributors.
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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static net.jpountz.lz4.Instances.FAST_DECOMPRESSORS;
import static net.jpountz.lz4.Instances.SAFE_DECOMPRESSORS;
import static net.jpountz.lz4.LZ4Constants.MIN_MATCH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A match whose distance is shorter than its length repeats a pattern. The decompressors copy matches in 4 and 8 byte
 * steps, which must not read bytes before they are written.
 */
public class OverlappingMatchTest {
  private static final int LITERALS = 12;
  private static final int TRAILING_LITERALS = 12;

  @Test
  public void overlappingMatches() {
    for (int distance = 1; distance <= LITERALS; distance++) {
      for (int matchLen = MIN_MATCH; matchLen <= 40; matchLen++) {
        byte[] expected = expected(distance, matchLen);
        byte[] block = block(expected, distance, matchLen);
        String what = "distance " + distance + ", match length " + matchLen + ", ";
        for (LZ4SafeDecompressor decompressor : SAFE_DECOMPRESSORS) {
          byte[] out = new byte[expected.length];
          assertEquals(expected.length, decompressor.decompress(block, 0, block.length, out, 0, out.length));
          assertArrayEquals(expected, out, what + decompressor);
        }
        for (LZ4FastDecompressor decompressor : FAST_DECOMPRESSORS) {
          byte[] out = new byte[expected.length];
          assertEquals(block.length, decompressor.decompress(block, 0, out, 0, out.length));
          assertArrayEquals(expected, out, what + decompressor);
        }
      }
    }
  }

  /** LITERALS distinct bytes, the match expanded one byte at a time, then TRAILING_LITERALS more bytes. */
  private static byte[] expected(int distance, int matchLen) {
    byte[] out = new byte[LITERALS + matchLen + TRAILING_LITERALS];
    for (int i = 0; i < out.length; i++) {
      boolean inMatch = i >= LITERALS && i < LITERALS + matchLen;
      out[i] = inMatch ? out[i - distance] : (byte) (i + 1);
    }
    return out;
  }

  /** One sequence with the literals and the match, then a last sequence with only the trailing literals. */
  private static byte[] block(byte[] expected, int distance, int matchLen) {
    ByteArrayOutputStream block = new ByteArrayOutputStream();
    int extraMatchLen = matchLen - MIN_MATCH;
    block.write(LITERALS << 4 | Math.min(extraMatchLen, 15));
    block.write(expected, 0, LITERALS);
    block.write(distance);
    block.write(distance >>> 8);
    if (extraMatchLen >= 15) {
      block.write(extraMatchLen - 15);
    }
    block.write(TRAILING_LITERALS << 4);
    block.write(expected, LITERALS + matchLen, TRAILING_LITERALS);
    return block.toByteArray();
  }
}
