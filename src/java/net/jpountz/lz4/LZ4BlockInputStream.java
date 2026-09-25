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

import static net.jpountz.lz4.LZ4BlockOutputStream.COMPRESSION_LEVEL_BASE;
import static net.jpountz.lz4.LZ4BlockOutputStream.COMPRESSION_METHOD_LZ4;
import static net.jpountz.lz4.LZ4BlockOutputStream.COMPRESSION_METHOD_RAW;
import static net.jpountz.lz4.LZ4BlockOutputStream.DEFAULT_SEED;
import static net.jpountz.lz4.LZ4BlockOutputStream.HEADER_LENGTH;
import static net.jpountz.lz4.LZ4BlockOutputStream.MAGIC;
import static net.jpountz.lz4.LZ4BlockOutputStream.MAGIC_LENGTH;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * {@link InputStream} implementation to decode data written with
 * {@link LZ4BlockOutputStream}. This class is not thread-safe and does not
 * support {@link #mark(int)}/{@link #reset()}.
 * <p>Use {@link Builder#withAcceptOversizedBlocks(boolean)} only for trusted
 * inputs that may contain noncanonical legacy LZ4 blocks. Enabling it restores
 * acceptance of blocks whose compressed length is greater than or equal to the
 * original length, which may permit large attacker-controlled allocations.</p>
 * @see LZ4BlockOutputStream
 */
public class LZ4BlockInputStream extends FilterInputStream {

  private final LZ4FastDecompressor fastDecompressor;
  private final LZ4SafeDecompressor safeDecompressor;
  private final Checksum checksum;
  private final boolean stopOnEmptyBlock;
  private final boolean acceptOversizedBlocks;
  private byte[] buffer;
  private byte[] compressedBuffer;
  private int originalLen;
  private int o;
  private boolean finished;

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream.
   *
   * @param in                the {@link InputStream} to poll
   * @param fastDecompressor  the {@link LZ4FastDecompressor} instance to
   *                          use
   * @param checksum          the {@link Checksum} instance to use, must be
   *                          equivalent to the instance which has been used to
   *                          write the stream
   * @param stopOnEmptyBlock  whether read is stopped on an empty block
   * @deprecated Use {@link #newBuilder()} instead.
   */
  @Deprecated
  public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor fastDecompressor, Checksum checksum, boolean stopOnEmptyBlock) {
    this(in, fastDecompressor, null, checksum, stopOnEmptyBlock, false);
  }

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream.
   *
   * @param in                the {@link InputStream} to poll
   * @param fastDecompressor  the {@link LZ4FastDecompressor} instance to
   *                          use
   * @param checksum          the {@link Checksum} instance to use, must be
   *                          equivalent to the instance which has been used to
   *                          write the stream
   *
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor, Checksum, boolean)
   * @deprecated Use {@link #newBuilder()} instead.
   */
  @Deprecated
  public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor fastDecompressor, Checksum checksum) {
    this(in, fastDecompressor, null, checksum, true, false);
  }

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream, using {@link XXHash32} for checksuming.
   *
   * @param in                the {@link InputStream} to poll
   * @param fastDecompressor  the {@link LZ4FastDecompressor} instance to
   *                          use
   *
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor, Checksum, boolean)
   * @see StreamingXXHash32#asChecksum()
   * @deprecated Use {@link #newBuilder()} instead.
   */
  @Deprecated
  public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor fastDecompressor) {
    this(in, fastDecompressor, null, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true, false);
  }

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream, using {@link XXHash32} for checksuming.
   *
   * @param in                the {@link InputStream} to poll
   * @param stopOnEmptyBlock  whether read is stopped on an empty block
   *
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor, Checksum, boolean)
   * @see LZ4Factory#fastestInstance()
   * @see StreamingXXHash32#asChecksum()
   * @deprecated Use {@link #newBuilder()} instead.
   */
  @Deprecated
  public LZ4BlockInputStream(InputStream in, boolean stopOnEmptyBlock) {
    this(in, LZ4Factory.fastestInstance().fastDecompressor(), null, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), stopOnEmptyBlock, false);
  }

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream, using {@link XXHash32} for checksuming.
   *
   * @param in                the {@link InputStream} to poll
   *
   * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor)
   * @see LZ4Factory#fastestInstance()
   * @deprecated Use {@link #newBuilder()} instead.
   */
  @Deprecated
  public LZ4BlockInputStream(InputStream in) {
    this(in, LZ4Factory.fastestInstance().fastDecompressor(), null, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true, false);
  }

  /**
   * Creates a new LZ4 input stream to read from the specified underlying InputStream.
   *
   * @param in                the {@link InputStream} to poll
   * @param fastDecompressor  the {@link LZ4FastDecompressor} instance to
   *                          use
   * @param safeDecompressor  the {@link LZ4SafeDecompressor} instance to
   *                          use (if both fastDecompressor and safeDecompressor are
   *                          specified then the fastDecompressor gets used)
   * @param checksum          the {@link Checksum} instance to use, must be
   *                          equivalent to the instance which has been used to
   *                          write the stream
   * @param stopOnEmptyBlock  whether read is stopped on an empty block
   * @param acceptOversizedBlocks whether noncanonical legacy LZ4 blocks with
   *                              compressed length greater than or equal to
   *                              original length are accepted
   */
  private LZ4BlockInputStream(InputStream in, LZ4FastDecompressor fastDecompressor, LZ4SafeDecompressor safeDecompressor,
                              Checksum checksum, boolean stopOnEmptyBlock, boolean acceptOversizedBlocks) {
    super(in);

    this.fastDecompressor = fastDecompressor;
    this.safeDecompressor = safeDecompressor;
    this.checksum = checksum;
    this.stopOnEmptyBlock = stopOnEmptyBlock;
    this.acceptOversizedBlocks = acceptOversizedBlocks;
    this.buffer = new byte[0];
    this.compressedBuffer = new byte[HEADER_LENGTH];
    o = originalLen = 0;
    finished = false;
  }

  /**
   * Creates a new LZ4 block input stream builder. The following are defaults:
   * <ul>
   * <li> decompressor - {@code LZ4Factory.fastestInstance().safeDecompressor()} </li>
   * <li> checksum - {@link XXHash32} </li>
   * <li> stopOnEmptyBlock - {@code true} </li>
   * <li> acceptOversizedBlocks - {@code false} </li>
   * </ul>
   * @return new instance of {@link Builder} to be used to configure and build new LZ4 input stream
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int available() throws IOException {
    return originalLen - o;
  }

  @Override
  public int read() throws IOException {
    if (finished) {
      return -1;
    }
    if (o == originalLen) {
      refill();
    }
    if (finished) {
      return -1;
    }
    return buffer[o++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    SafeUtils.checkRange(b, off, len);
    if (finished) {
      return -1;
    }
    if (o == originalLen) {
      refill();
    }
    if (finished) {
      return -1;
    }
    len = Math.min(len, originalLen - o);
    System.arraycopy(buffer, o, b, off, len);
    o += len;
    return len;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0 || finished) {
      return 0;
    }
    if (o == originalLen) {
      refill();
    }
    if (finished) {
      return 0;
    }
    final int skipped = (int) Math.min(n, originalLen - o);
    o += skipped;
    return skipped;
  }

  private void refill() throws IOException {
    // Loop rather than recurse over empty blocks so that a long run of them cannot overflow the stack
    while (!readBlock()) {
      // empty block with stopOnEmptyBlock == false, continue with the next block
    }
  }

  /**
   * Reads the next block.
   *
   * @return {@code false} if an empty block was read and reading should continue with the next block
   */
  private boolean readBlock() throws IOException {
    final int headerRead = tryReadFully(compressedBuffer, HEADER_LENGTH);
    if (headerRead != HEADER_LENGTH) {
      // Only a stream that ends exactly at a block boundary is a clean end
      if (headerRead == 0 && !stopOnEmptyBlock) {
        finished = true;
        return true;
      }
      throw new EOFException("Stream ended prematurely");
    }
    for (int i = 0; i < MAGIC_LENGTH; ++i) {
      if (compressedBuffer[i] != MAGIC[i]) {
        throw new IOException("Stream is corrupted");
      }
    }
    final int token = compressedBuffer[MAGIC_LENGTH] & 0xFF;
    final int compressionMethod = token & 0xF0;
    final int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
    if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4) {
      throw new IOException("Stream is corrupted");
    }
    final int compressedLen = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 1);
    originalLen = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 5);
    final int check = SafeUtils.readIntLE(compressedBuffer, MAGIC_LENGTH + 9);
    assert HEADER_LENGTH == MAGIC_LENGTH + 13;
    if (originalLen > 1 << compressionLevel
      || originalLen < 0
      || compressedLen < 0
      || (originalLen == 0 && compressedLen != 0)
      || (originalLen != 0 && compressedLen == 0)
      || (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)
      || (compressionMethod == COMPRESSION_METHOD_LZ4 && !acceptOversizedBlocks && compressedLen >= originalLen)) {
      throw new IOException("Stream is corrupted");
    }
    if (originalLen == 0 && compressedLen == 0) {
      if (check != 0) {
        throw new IOException("Stream is corrupted");
      }
      o = 0;
      if (!stopOnEmptyBlock) {
        return false;
      }
      finished = true;
      return true;
    }
    if (buffer.length < originalLen) {
      buffer = new byte[Math.max(originalLen, buffer.length * 3 / 2)];
    }
    switch (compressionMethod) {
      case COMPRESSION_METHOD_RAW:
        readFully(buffer, originalLen);
        break;
      case COMPRESSION_METHOD_LZ4:
        if (compressedBuffer.length < compressedLen) {
          compressedBuffer = new byte[Math.max(compressedLen, compressedBuffer.length * 3 / 2)];
        }
        readFully(compressedBuffer, compressedLen);
        try {
          if (fastDecompressor == null) {
            final int decompressedLen = safeDecompressor.decompress(compressedBuffer, 0, compressedLen, buffer, 0, originalLen);
            if (decompressedLen != originalLen) {
              throw new IOException("Stream is corrupted");
            }
          } else {
            final int compressedLen2 = fastDecompressor.decompress(compressedBuffer, 0, buffer, 0, originalLen);
            if (compressedLen != compressedLen2) {
              throw new IOException("Stream is corrupted");
            }
          }
        } catch (LZ4Exception e) {
          throw new IOException("Stream is corrupted", e);
        }
        break;
      default:
        throw new AssertionError();
    }
    checksum.reset();
    checksum.update(buffer, 0, originalLen);
    if ((int) checksum.getValue() != check) {
      throw new IOException("Stream is corrupted");
    }
    o = 0;
    return true;
  }

  // Like readFully(), except it signals incomplete reads by returning
  // the number of bytes read (less than len) instead of throwing EOFException.
  private int tryReadFully(byte[] b, int len) throws IOException {
    int read = 0;
    while (read < len) {
      final int r = in.read(b, read, len - read);
      if (r < 0) {
        break;
      }
      read += r;
    }
    return read;
  }

  private void readFully(byte[] b, int len) throws IOException {
    if (tryReadFully(b, len) != len) {
      throw new EOFException("Stream ended prematurely");
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("sync-override")
  @Override
  public void mark(int readlimit) {
    // unsupported
  }

  @SuppressWarnings("sync-override")
  @Override
  public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(in=" + in
      + ", decompressor=" + (fastDecompressor != null ? fastDecompressor : safeDecompressor)
      + ", checksum=" + checksum + ")";
  }

  /**
   * Builder for {@link LZ4BlockInputStream}
   */
  public static final class Builder {
    private boolean stopOnEmptyBlock = true;
    private boolean acceptOversizedBlocks = false;
    private LZ4FastDecompressor fastDecompressor;
    private LZ4SafeDecompressor safeDecompressor;
    private Checksum checksum;

    private Builder() {
    }

    /**
     * Registers value of stopOnEmptyBlock to be used by the builder
     *
     * @param stopOnEmptyBlock  whether read is stopped on an empty block
     * @return current builder instance
     */
    public Builder withStopOnEmptyBlock(boolean stopOnEmptyBlock) {
      this.stopOnEmptyBlock = stopOnEmptyBlock;
      return this;
    }

    /**
     * Registers whether the builder should accept oversized blocks whose
     * compressed length is greater than or equal to the original length.
     * <p>Only enable this for trusted inputs. Accepting such blocks may permit
     * large attacker-controlled allocations.</p>
     *
     * @param acceptOversizedBlocks whether oversized blocks should be accepted
     * @return current builder instance
     * @since 1.11.2
     */
    public Builder withAcceptOversizedBlocks(boolean acceptOversizedBlocks) {
      this.acceptOversizedBlocks = acceptOversizedBlocks;
      return this;
    }

    /**
     * Registers {@link LZ4FastDecompressor} to be used by the builder as a decompressor. Overrides one set by
     * {@link #withDecompressor(LZ4SafeDecompressor)}
     *
     * @param fastDecompressor      the {@link LZ4FastDecompressor} instance to use
     * @return current builder instance
     */
    public Builder withDecompressor(LZ4FastDecompressor fastDecompressor) {
      this.fastDecompressor = fastDecompressor;
      this.safeDecompressor = null;
      return this;
    }

    /**
     * Registers {@link LZ4SafeDecompressor} to be used by the builder as a decompressor. Overrides one set by
     * {@link #withDecompressor(LZ4FastDecompressor)}
     *
     * @param safeDecompressor      the {@link LZ4SafeDecompressor} instance to use.
     * @return current builder instance
     */
    public Builder withDecompressor(LZ4SafeDecompressor safeDecompressor) {
      this.safeDecompressor = safeDecompressor;
      this.fastDecompressor = null;
      return this;
    }

    /**
     * Registers {@link Checksum} to be used by the builder
     * <p>Note: Since checksum objects are stateful, if you set one explicitly here, the builder can only be used once.
     *
     * @param checksum          the {@link Checksum} instance to use, must be
     *                          equivalent to the instance which has been used to
     *                          write the stream
     * @return current builder instance
     */
    public Builder withChecksum(Checksum checksum) {
      this.checksum = checksum;
      return this;
    }

    /**
     * Creates a new LZ4 input stream to read from the specified InputStream with specified parameters
     *
     * @param in    the {@link InputStream} to poll
     * @return new instance of {@link LZ4BlockInputStream} using parameters set in the builder and provided InputStream
     *
     * @see #withChecksum(Checksum)
     * @see #withDecompressor(LZ4FastDecompressor)
     * @see #withDecompressor(LZ4SafeDecompressor)
     * @see #withStopOnEmptyBlock(boolean)
     * @see #withAcceptOversizedBlocks(boolean)
     */
    public LZ4BlockInputStream build(InputStream in) {
      Checksum checksum = this.checksum;
      LZ4FastDecompressor fastDecompressor = this.fastDecompressor;
      LZ4SafeDecompressor safeDecompressor = this.safeDecompressor;

      if (checksum == null) {
        checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
      }
      if (fastDecompressor == null && safeDecompressor == null) {
        safeDecompressor = LZ4Factory.fastestInstance().safeDecompressor();
      }
      return new LZ4BlockInputStream(in, fastDecompressor, safeDecompressor, checksum, stopOnEmptyBlock, acceptOversizedBlocks);
    }
  }
}
