package net.jpountz.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.lz4.LZ4TestAccess;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LZ4DecompressorTest {
  private static final int MAX_LEN = 1 << 16;

  private void test(FuzzedDataProvider data, LZ4Factory factory, boolean fast, boolean byteBuffer) {
    int destLen = data.consumeInt(0, MAX_LEN);
    int destOff = data.consumeInt(0, 16);
    int srcOff = data.consumeInt(0, 16);
    int srcOffEnd = data.consumeInt(0, 16);
    byte[] src = data.consumeRemainingAsBytes();
    if (srcOff > 0 || srcOffEnd > 0) {
      byte[] oldSrc = src;
      src = new byte[src.length + srcOff + srcOffEnd];
      System.arraycopy(oldSrc, 0, src, srcOff, oldSrc.length);
    }

    try {
      if (byteBuffer) {
        ByteBuffer srcBuf = ByteBuffer.allocateDirect(src.length);
        srcBuf.put(src);
        srcBuf.flip();
        ByteBuffer destBuf = ByteBuffer.allocateDirect(destOff + destLen);
        if (fast) {
          factory.fastDecompressor().decompress(srcBuf, srcOff, destBuf, destOff, destLen);
        } else {
          factory.safeDecompressor().decompress(srcBuf, srcOff, src.length - srcOffEnd - srcOff, destBuf, destOff, destLen);
        }
      } else {
        // For byte[], we decompress twice with different prior data in the output array, and compare the results. This
        // makes sure no uninitialized data remains.
        byte[] dest1 = new byte[destOff + destLen];
        byte[] dest2 = new byte[destOff + destLen];
        Arrays.fill(dest2, (byte) 'x');
        if (fast) {
          int n1 = factory.fastDecompressor().decompress(src, srcOff, dest1, destOff, destLen);
          int n2 = factory.fastDecompressor().decompress(src, srcOff, dest2, destOff, destLen);
          assertEquals(n1, n2);
          assertArrayEquals(Arrays.copyOfRange(dest1, destOff, destOff + destLen), Arrays.copyOfRange(dest2, destOff, destOff + destLen));
        } else {
          int n1 = factory.safeDecompressor().decompress(src, srcOff, src.length - srcOffEnd - srcOff, dest1, destOff);
          int n2 = factory.safeDecompressor().decompress(src, srcOff, src.length - srcOffEnd - srcOff, dest2, destOff);
          assertEquals(n1, n2);
          assertArrayEquals(Arrays.copyOfRange(dest1, destOff, destOff + n1), Arrays.copyOfRange(dest2, destOff, destOff + n2));
        }
      }
    } catch (LZ4Exception ignored) {
    }
  }

  // Linked blocks: the input may also reference the prefixLen bytes before the output. Decompresses twice, with
  // different data before the prefix and after the output, and compares: the decompressor must not read them.
  private void testPrefix(FuzzedDataProvider data, LZ4Factory factory) {
    // up to twice the 64 KB window, so that liblz4 takes both of its prefix paths
    int prefixLen = data.consumeInt(0, 1 << 17);
    int garbageLen = data.consumeInt(0, 16);
    int destLen = data.consumeInt(0, MAX_LEN);
    byte seed = data.consumeByte();
    byte[] src = data.consumeRemainingAsBytes();
    int destOff = garbageLen + prefixLen;
    byte[] dest1 = new byte[destOff + destLen];
    byte[] dest2 = new byte[destOff + destLen];
    Arrays.fill(dest2, (byte) 'x');
    for (int i = 0; i < prefixLen; i++) {
      dest1[garbageLen + i] = dest2[garbageLen + i] = (byte) (seed + i * 31);
    }
    LZ4SafeDecompressor decompressor = factory.safeDecompressor();
    int n1;
    try {
      n1 = LZ4TestAccess.decompressWithPrefix(decompressor, src, 0, src.length, dest1, destOff, destLen, prefixLen);
    } catch (LZ4Exception e) {
      n1 = -1;
    }
    int n2;
    try {
      n2 = LZ4TestAccess.decompressWithPrefix(decompressor, src, 0, src.length, dest2, destOff, destLen, prefixLen);
    } catch (LZ4Exception e) {
      n2 = -1;
    }
    assertEquals(n1, n2);
    if (n1 >= 0) {
      assertArrayEquals(Arrays.copyOfRange(dest1, destOff, destOff + n1), Arrays.copyOfRange(dest2, destOff, destOff + n2));
    }
  }

  @FuzzTest
  public void safe_prefix(FuzzedDataProvider provider) {
    testPrefix(provider, LZ4Factory.safeInstance());
  }

  @FuzzTest
  public void unsafe_prefix(FuzzedDataProvider provider) {
    testPrefix(provider, LZ4Factory.unsafeInsecureInstance());
  }

  @FuzzTest
  public void native_prefix(FuzzedDataProvider provider) {
    testPrefix(provider, LZ4Factory.nativeInstance());
  }

  @FuzzTest
  public void safe_safe_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.safeInstance(), false, false);
  }

  @FuzzTest
  public void safe_fast_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.safeInstance(), true, false);
  }

  @FuzzTest
  public void unsafe_safe_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.unsafeInsecureInstance(), false, false);
  }

  @FuzzTest
  public void unsafe_fast_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.unsafeInsecureInstance(), true, false);
  }

  @FuzzTest
  public void native_safe_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.nativeInsecureInstance(), false, false);
  }

  @FuzzTest
  public void native_fast_array(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.nativeInsecureInstance(), true, false);
  }

  @FuzzTest
  public void safe_safe_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.safeInstance(), false, true);
  }

  @FuzzTest
  public void safe_fast_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.safeInstance(), true, true);
  }

  @FuzzTest
  public void unsafe_safe_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.unsafeInsecureInstance(), false, true);
  }

  @FuzzTest
  public void unsafe_fast_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.unsafeInsecureInstance(), true, true);
  }

  @FuzzTest
  public void native_safe_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.nativeInsecureInstance(), false, true);
  }

  @FuzzTest
  public void native_fast_bytebuffer(FuzzedDataProvider provider) {
    test(provider, LZ4Factory.nativeInsecureInstance(), true, true);
  }
}
