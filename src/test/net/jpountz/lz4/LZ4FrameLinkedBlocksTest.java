package net.jpountz.lz4;

/*
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

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Linked blocks (FLG.B.Indep = 0): matches may reference up to 64 KB of the data decoded before the block, in the same
 * frame. Frames are written by hand here, along with the data they decode to, or by the lz4 CLI.
 */
public class LZ4FrameLinkedBlocksTest {

  private static final int FLG_VERSION = 0x40;
  private static final int FLG_BLOCK_INDEPENDENCE = 0x20;
  private static final int FLG_BLOCK_CHECKSUM = 0x10;
  private static final int FLG_CONTENT_SIZE = 0x08;
  private static final int FLG_CONTENT_CHECKSUM = 0x04;
  private static final int BD_64KB = 0x40;
  private static final int BD_256KB = 0x50;
  private static final int BD_4MB = 0x70;
  private static final int WINDOW = 65535; // largest match offset
  private static final XXHash32 XXH32 = XXHashFactory.safeInstance().hash32();

  static LZ4SafeDecompressor[] decompressors() {
    return Instances.SAFE_DECOMPRESSORS;
  }

  /** Growable byte array. */
  private static final class Bytes {
    byte[] buf = new byte[256];
    int size;

    void write(byte[] b) {
      ensure(b.length);
      System.arraycopy(b, 0, buf, size, b.length);
      size += b.length;
    }

    void write(int b) {
      ensure(1);
      buf[size++] = (byte) b;
    }

    void writeIntLE(int v) {
      for (int i = 0; i < 4; i++) {
        write(v >>> (8 * i));
      }
    }

    // Byte by byte, since a match may overlap its own output
    void copyMatch(int offset, int len) {
      ensure(len);
      for (int i = 0; i < len; i++, size++) {
        buf[size] = size >= offset ? buf[size - offset] : 0; // 0 for invalid frames, which must not decode
      }
    }

    private void ensure(int n) {
      if (size + n > buf.length) {
        buf = Arrays.copyOf(buf, Math.max(2 * buf.length, size + n));
      }
    }

    byte[] toByteArray() {
      return Arrays.copyOf(buf, size);
    }
  }

  /** Writes a frame block by block, and keeps the data it decodes to. */
  private static final class FrameWriter {
    final int flg;
    final int bd;
    final Bytes content = new Bytes();
    final List<byte[]> blocks = new ArrayList<>();
    final List<Boolean> compressed = new ArrayList<>();
    // the block being written by sequence() and lastLiterals()
    private Bytes block;

    FrameWriter(int flg, int bd) {
      this.flg = flg;
      this.bd = bd;
    }

    int maxBlockSize() {
      return 1 << (2 * ((bd >>> 4) & 7) + 8);
    }

    FrameWriter uncompressedBlock(byte[] data) {
      blocks.add(data);
      compressed.add(false);
      content.write(data);
      return this;
    }

    FrameWriter sequence(byte[] literals, int offset, int matchLen) {
      if (block == null) {
        block = new Bytes();
      }
      final int litLen = literals.length;
      final int ml = matchLen - LZ4Constants.MIN_MATCH;
      block.write((Math.min(litLen, 15) << 4) | Math.min(ml, 15));
      writeLength(litLen);
      block.write(literals);
      block.write(offset);
      block.write(offset >>> 8);
      writeLength(ml);
      content.write(literals);
      content.copyMatch(offset, matchLen);
      return this;
    }

    // Ends the compressed block. The last 5 bytes of a block are literals, and its last match starts at least 12 bytes
    // before its end: callers pass at least 12 literals after a match.
    FrameWriter lastLiterals(byte[] literals) {
      if (block == null) {
        block = new Bytes();
      }
      block.write(Math.min(literals.length, 15) << 4);
      writeLength(literals.length);
      block.write(literals);
      content.write(literals);
      blocks.add(block.toByteArray());
      compressed.add(true);
      block = null;
      return this;
    }

    private void writeLength(int len) {
      if (len >= 15) {
        len -= 15;
        for (; len >= 255; len -= 255) {
          block.write(255);
        }
        block.write(len);
      }
    }

    byte[] toByteArray() {
      final Bytes out = new Bytes();
      out.writeIntLE(LZ4FrameOutputStream.MAGIC);
      final int descriptorStart = out.size;
      out.write(flg);
      out.write(bd);
      if ((flg & FLG_CONTENT_SIZE) != 0) {
        out.writeIntLE(content.size);
        out.writeIntLE(0);
      }
      out.write((XXH32.hash(out.buf, descriptorStart, out.size - descriptorStart, 0) >> 8) & 0xFF);
      for (int i = 0; i < blocks.size(); i++) {
        final byte[] b = blocks.get(i);
        out.writeIntLE(b.length | (compressed.get(i) ? 0 : LZ4FrameOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK));
        out.write(b);
        if ((flg & FLG_BLOCK_CHECKSUM) != 0) {
          out.writeIntLE(XXH32.hash(b, 0, b.length, 0));
        }
      }
      out.writeIntLE(0); // EndMark
      if ((flg & FLG_CONTENT_CHECKSUM) != 0) {
        out.writeIntLE(XXH32.hash(content.buf, 0, content.size, 0));
      }
      return out.toByteArray();
    }
  }

  private static byte[] randomBytes(Random rnd, int len) {
    final byte[] b = new byte[len];
    rnd.nextBytes(b);
    return b;
  }

  private static byte[] decode(byte[] frames, LZ4SafeDecompressor decompressor) throws IOException {
    try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frames), decompressor, XXH32)) {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      final byte[] buf = new byte[8192];
      for (int n; (n = is.read(buf)) != -1; ) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }

  // Decodes with a mix of read(), read(byte[], int, int) and skip() of various sizes, and checks the result
  private static void assertDecodes(byte[] expected, byte[] frames, LZ4SafeDecompressor decompressor, Random rnd)
      throws IOException {
    try (InputStream is = new LZ4FrameInputStream(new ByteArrayInputStream(frames), decompressor, XXH32)) {
      final byte[] buf = new byte[1 << 17];
      int pos = 0;
      while (true) {
        final int op = rnd.nextInt(8);
        final int len = 1 + rnd.nextInt(1 << rnd.nextInt(18));
        if (op == 0) {
          final int b = is.read();
          if (b == -1) {
            break;
          }
          assertEquals(expected[pos++], (byte) b, "at " + pos);
        } else if (op == 1) {
          final long n = is.skip(len);
          assertTrue(n >= 0 && n <= len);
          if (n == 0) { // end of stream
            break;
          }
          pos += (int) n;
        } else {
          final int n = is.read(buf, 0, len);
          if (n == -1) {
            break;
          }
          assertArrayEquals(Arrays.copyOfRange(expected, pos, pos + n), Arrays.copyOf(buf, n), "at " + pos);
          pos += n;
        }
      }
      assertEquals(expected.length, pos);
    }
  }

  @ParameterizedTest
  @MethodSource("decompressors")
  public void matchesReachBackAcrossBlocks(LZ4SafeDecompressor decompressor) throws IOException {
    final Random rnd = new Random(1);
    final FrameWriter w = new FrameWriter(FLG_VERSION | FLG_CONTENT_CHECKSUM, BD_64KB)
        .uncompressedBlock(randomBytes(rnd, 40000))
        .uncompressedBlock(randomBytes(rnd, 30000))
        // the farthest match, into the first block; a match across the boundary of the first two blocks
        .sequence(new byte[0], WINDOW, 100)
        .sequence(randomBytes(rnd, 3), 30000 + 100 + 3 + 50, 100)
        .lastLiterals(randomBytes(rnd, 20))
        // a match that starts in the previous block and continues into this one, overlapping its own output
        .sequence(new byte[0], 10, 1000)
        .lastLiterals(randomBytes(rnd, 12));
    final byte[] frame = w.toByteArray();
    assertArrayEquals(w.content.toByteArray(), decode(frame, decompressor));

    // the same blocks must not decode as independent blocks
    final FrameWriter independent = new FrameWriter(w.flg | FLG_BLOCK_INDEPENDENCE, w.bd);
    independent.blocks.addAll(w.blocks);
    independent.compressed.addAll(w.compressed);
    independent.content.write(w.content.toByteArray());
    assertThrows(IOException.class, () -> decode(independent.toByteArray(), decompressor));
  }

  @ParameterizedTest
  @MethodSource("decompressors")
  public void matchBeforeFrameStartIsRejected(LZ4SafeDecompressor decompressor) throws IOException {
    final Random rnd = new Random(2);
    final byte[] frame = new FrameWriter(FLG_VERSION, BD_64KB)
        .uncompressedBlock(randomBytes(rnd, 1000))
        .sequence(new byte[0], 1001, 4)
        .lastLiterals(randomBytes(rnd, 12))
        .toByteArray();
    assertThrows(IOException.class, () -> decode(frame, decompressor));

    // the history doesn't carry over to the next frame
    final byte[] first = new FrameWriter(FLG_VERSION, BD_64KB).uncompressedBlock(randomBytes(rnd, 1000)).toByteArray();
    final byte[] second = new FrameWriter(FLG_VERSION, BD_64KB)
        .sequence(randomBytes(rnd, 1), 2, 4)
        .lastLiterals(randomBytes(rnd, 12))
        .toByteArray();
    final byte[] frames = new byte[first.length + second.length];
    System.arraycopy(first, 0, frames, 0, first.length);
    System.arraycopy(second, 0, frames, first.length, second.length);
    assertThrows(IOException.class, () -> decode(frames, decompressor));
  }

  // Writes a frame of blockCount blocks of random sizes. Compressed blocks have random matches over the whole window
  private static FrameWriter randomFrame(Random rnd, int flg, int bd, int blockCount) {
    final FrameWriter w = new FrameWriter(flg, bd);
    final boolean linked = (flg & FLG_BLOCK_INDEPENDENCE) == 0;
    for (int i = 0; i < blockCount; i++) {
      final int size;
      switch (rnd.nextInt(4)) {
        case 0:
          size = 1 + rnd.nextInt(100);
          break;
        case 1:
          size = 1 + rnd.nextInt(w.maxBlockSize());
          break;
        default:
          size = w.maxBlockSize() - rnd.nextInt(100);
      }
      if (rnd.nextInt(5) == 0) {
        w.uncompressedBlock(randomBytes(rnd, size));
        continue;
      }
      final int blockStart = w.content.size;
      final int historyStart = linked ? Math.max(0, blockStart - WINDOW) : blockStart;
      int remaining = size;
      // up to 19 literals and a match, followed by at least 12 last literals
      while (remaining >= 19 + LZ4Constants.MIN_MATCH + 12) {
        byte[] literals = randomBytes(rnd, rnd.nextInt(20));
        int available = Math.min(WINDOW, w.content.size + literals.length - historyStart);
        if (available == 0) { // nothing to reference yet
          literals = randomBytes(rnd, 1);
          available = 1;
        }
        final int maxMatchLen = remaining - 19 - 12;
        final int matchLen = LZ4Constants.MIN_MATCH
            + rnd.nextInt(Math.min(maxMatchLen - LZ4Constants.MIN_MATCH + 1, 1 << rnd.nextInt(16)));
        // either anywhere in the window, or near its start
        final int offset = rnd.nextBoolean() ? 1 + rnd.nextInt(available) : available - rnd.nextInt(Math.min(available, 64));
        w.sequence(literals, offset, matchLen);
        remaining -= literals.length + matchLen;
      }
      w.lastLiterals(randomBytes(rnd, remaining));
    }
    return w;
  }

  @ParameterizedTest
  @MethodSource("decompressors")
  public void randomFrames(LZ4SafeDecompressor decompressor) throws IOException {
    final Random rnd = new Random(3);
    final int[] blockSizes = {BD_64KB, BD_256KB, BD_4MB};
    final int[] options = {0, FLG_BLOCK_CHECKSUM, FLG_CONTENT_SIZE | FLG_CONTENT_CHECKSUM};
    for (int iter = 0; iter < 20; iter++) {
      // concatenated frames, mostly linked, with different block sizes, so that buffers are reused across frames
      final Bytes frames = new Bytes();
      final Bytes expected = new Bytes();
      final int frameCount = 1 + rnd.nextInt(3);
      for (int f = 0; f < frameCount; f++) {
        // 4 MB blocks are slow to write here, so only some iterations use them
        final int bd = blockSizes[rnd.nextInt(iter % 5 == 0 ? 3 : 2)];
        final int flg = FLG_VERSION | options[rnd.nextInt(options.length)]
            | (rnd.nextInt(4) == 0 ? FLG_BLOCK_INDEPENDENCE : 0);
        final FrameWriter w = randomFrame(rnd, flg, bd, bd == BD_4MB ? 2 : 1 + rnd.nextInt(12));
        frames.write(w.toByteArray());
        expected.write(w.content.toByteArray());
      }
      assertDecodes(expected.toByteArray(), frames.toByteArray(), decompressor, rnd);
    }
  }

  @ParameterizedTest
  @MethodSource("decompressors")
  public void historyIsMovedOncePer64KB(LZ4SafeDecompressor decompressor) throws IOException {
    // records where blocks with history are decoded, and where they end: the history was moved when a block is not
    // decoded right after the previous one
    final List<Integer> destOffs = new ArrayList<>();
    final List<Integer> destEnds = new ArrayList<>();
    final LZ4SafeDecompressor recording = new LZ4SafeDecompressor() {
      @Override
      public int decompress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        return decompressor.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
      }

      @Override
      public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
        return decompressor.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
      }

      @Override
      int decompressWithPrefix(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen,
                               int prefixLen) {
        final int n = decompressor.decompressWithPrefix(src, srcOff, srcLen, dest, destOff, maxDestLen, prefixLen);
        destOffs.add(destOff);
        destEnds.add(destOff + n);
        return n;
      }
    };

    // Small compressed blocks: each one may decompress to the maximum block size, so the stream needs room for 64 KB
    // after the history for each of them. Each block references the farthest byte of the history.
    final Random rnd = new Random(5);
    final FrameWriter w = new FrameWriter(FLG_VERSION | FLG_CONTENT_CHECKSUM, BD_64KB)
        .uncompressedBlock(randomBytes(rnd, 1 << 16));
    final int blockCount = 2000;
    for (int i = 0; i < blockCount; i++) {
      w.sequence(randomBytes(rnd, 300), WINDOW, 4).lastLiterals(randomBytes(rnd, 12));
    }
    assertArrayEquals(w.content.toByteArray(), decode(w.toByteArray(), recording));

    assertEquals(blockCount, destOffs.size());
    int moves = 0;
    for (int i = 1; i < destOffs.size(); i++) {
      if (!destOffs.get(i).equals(destEnds.get(i - 1))) {
        moves++;
      }
    }
    final int decodedAfterFirstBlock = w.content.size - (1 << 16);
    assertTrue(moves > 0 && moves <= decodedAfterFirstBlock / (1 << 16) + 1, "moves: " + moves);
  }

  @Test
  public void decompressorsOutsideThisPackageRejectAPrefix() throws IOException {
    final LZ4SafeDecompressor safe = LZ4Factory.safeInstance().safeDecompressor();
    // does not override decompressWithPrefix, like a subclass outside of this package
    final LZ4SafeDecompressor custom = new LZ4SafeDecompressor() {
      @Override
      public int decompress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        return safe.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
      }

      @Override
      public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
        return safe.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
      }
    };
    final Random rnd = new Random(4);
    // the first block of a frame has no prefix
    final FrameWriter first = new FrameWriter(FLG_VERSION, BD_64KB)
        .sequence(randomBytes(rnd, 10), 10, 100)
        .lastLiterals(randomBytes(rnd, 12));
    assertArrayEquals(first.content.toByteArray(), decode(first.toByteArray(), custom));

    final byte[] frame = new FrameWriter(FLG_VERSION, BD_64KB)
        .uncompressedBlock(randomBytes(rnd, 1000))
        .sequence(new byte[0], 1000, 4)
        .lastLiterals(randomBytes(rnd, 12))
        .toByteArray();
    final IOException e = assertThrows(IOException.class, () -> decode(frame, custom));
    assertTrue(e.getCause().getMessage().contains("does not support linked blocks"), e.getCause().getMessage());
  }

  // A block whose first match has the given offset
  private static byte[] blockWithMatch(int offset) {
    final FrameWriter w = new FrameWriter(FLG_VERSION, BD_64KB)
        .sequence(new byte[0], offset, 20)
        .lastLiterals(new byte[12]);
    return w.blocks.get(0);
  }

  @ParameterizedTest
  @MethodSource("decompressors")
  public void decompressWithPrefixBounds(LZ4SafeDecompressor decompressor) {
    final int destOff = 100;
    final byte[] dest = new byte[destOff + 64];
    for (int i = 0; i < destOff; i++) {
      dest[i] = (byte) i;
    }
    final byte[] block = blockWithMatch(30);
    assertEquals(32, decompressor.decompressWithPrefix(block, 0, block.length, dest, destOff, 64, 30));
    for (int i = 0; i < 20; i++) {
      assertEquals((byte) (destOff - 30 + i % 30), dest[destOff + i]);
    }

    // the match starts one byte before the prefix
    assertThrows(LZ4Exception.class, () -> decompressor.decompressWithPrefix(block, 0, block.length, dest, destOff, 64, 29));
    assertThrows(LZ4Exception.class, () -> decompressor.decompressWithPrefix(block, 0, block.length, dest, destOff, 64, 0));
    assertThrows(LZ4Exception.class, () -> decompressor.decompress(block, 0, block.length, dest, destOff, 64));
    assertThrows(IllegalArgumentException.class,
        () -> decompressor.decompressWithPrefix(block, 0, block.length, dest, destOff, 64, -1));
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> decompressor.decompressWithPrefix(block, 0, block.length, dest, destOff, 64, destOff + 1));

    // a prefix of 64 KB or more
    final byte[] large = new byte[2 * WINDOW + 64];
    final byte[] farthest = blockWithMatch(WINDOW);
    assertEquals(32, decompressor.decompressWithPrefix(farthest, 0, farthest.length, large, 2 * WINDOW, 64, 2 * WINDOW));
  }

  private static boolean hasLz4Cli() {
    try {
      return new ProcessBuilder("lz4", "-V").redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD)
          .start().waitFor() == 0;
    } catch (IOException | InterruptedException e) {
      return false;
    }
  }

  // Compressible data, whose repetitions are less than 64 KB apart, with incompressible parts in between
  private static byte[] cliTestData() throws IOException {
    final byte[] text;
    try (InputStream is = LZ4FrameLinkedBlocksTest.class.getResourceAsStream("/calgary/book1")) {
      text = new byte[40000];
      int n = 0;
      for (int r; n < text.length && (r = is.read(text, n, text.length - n)) != -1; ) {
        n += r;
      }
      assertEquals(text.length, n);
    }
    final Random rnd = new Random(5);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (out.size() < 6 << 20) {
      if (rnd.nextInt(10) == 0) {
        out.write(randomBytes(rnd, 200000), 0, 200000);
      } else {
        final int start = rnd.nextInt(text.length / 2);
        final byte[] chunk = Arrays.copyOfRange(text, start, start + 1000 + rnd.nextInt(text.length / 2 - 1000));
        chunk[rnd.nextInt(chunk.length)] ^= 1;
        out.write(chunk, 0, chunk.length);
      }
    }
    return out.toByteArray();
  }

  @Test
  public void decodesLz4CliLinkedFrames() throws IOException, InterruptedException {
    final boolean cli = hasLz4Cli();
    // CI must run this test, like LZ4FrameIOStreamTest
    assertFalse(!cli && "true".equals(System.getenv("CI")), "lz4 CLI not available");
    Assumptions.assumeTrue(cli);
    final byte[] data = cliTestData();
    final File in = Files.createTempFile("lz4linked", ".dat").toFile();
    final File out = Files.createTempFile("lz4linked", ".lz4").toFile();
    try {
      Files.write(in.toPath(), data);
      final String[][] optionSets = {
          {"-B4"}, {"-B5"}, {"-B6"}, {"-B7"}, {"-B4", "-BX"}, {"-B5", "--content-size", "--no-frame-crc"}, {"-B4", "-9"}};
      for (String[] options : optionSets) {
        final List<String> cmd = new ArrayList<>(Arrays.asList("lz4", "-q", "-f", "-BD"));
        cmd.addAll(Arrays.asList(options));
        cmd.add(in.getAbsolutePath());
        cmd.add(out.getAbsolutePath());
        assertEquals(0, new ProcessBuilder(cmd).inheritIO().start().waitFor(), cmd.toString());
        final byte[] frame = Files.readAllBytes(out.toPath());
        assertEquals(0, frame[4] & FLG_BLOCK_INDEPENDENCE, "linked blocks expected");
        for (LZ4SafeDecompressor decompressor : decompressors()) {
          assertArrayEquals(data, decode(frame, decompressor), cmd + " " + decompressor);
        }

        // the frame depends on the history: it doesn't decode as independent blocks
        final int descriptorEnd = (frame[4] & FLG_CONTENT_SIZE) != 0 ? 14 : 6;
        frame[4] |= FLG_BLOCK_INDEPENDENCE;
        frame[descriptorEnd] = (byte) ((XXH32.hash(frame, 4, descriptorEnd - 4, 0) >> 8) & 0xFF);
        try {
          assertFalse(Arrays.equals(data, decode(frame, LZ4Factory.safeInstance().safeDecompressor())), cmd.toString());
        } catch (IOException expected) {
          // matches before the block start
        }
      }
    } finally {
      in.delete();
      out.delete();
    }
  }
}
