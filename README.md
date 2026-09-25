# LZ4 Java

[![Maven Central](https://img.shields.io/maven-central/v/at.yawk.lz4/lz4-java)](https://central.sonatype.com/artifact/at.yawk.lz4/lz4-java)

Fast [LZ4](https://github.com/lz4/lz4) compression and [xxHash](https://github.com/Cyan4973/xxHash) hashing for Java,
with JNI bindings to the reference C implementation and a pure Java fallback.

This is the community-maintained continuation of [lz4/lz4-java](https://github.com/lz4/lz4-java), which is no longer
maintained. The fork was originally created to fix
[CVE‐2025‐12183](https://sites.google.com/sonatype.com/vulnerabilities/cve-2025-12183) and has since received further
security fixes, an updated lz4 library, new native platforms and a modernized build. It is a drop-in replacement: the
Java package names (`net.jpountz.*`) and API are unchanged, only the Maven coordinates are different.

## Installation

Replace `VERSION` with the latest version shown on
[Maven Central](https://central.sonatype.com/artifact/at.yawk.lz4/lz4-java) or the
[releases page](https://github.com/yawkat/lz4-java/releases).

Maven:

```xml
<dependency>
    <groupId>at.yawk.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>VERSION</version>
</dependency>
```

Gradle:

```kotlin
implementation("at.yawk.lz4:lz4-java:VERSION")
```

If another library pulls in `org.lz4:lz4-java`, exclude it (or use Gradle's dependency substitution) and add this
artifact instead. Both contain the same classes, so make sure only one of them ends up on the classpath.

Requirements:

- Java 7 or newer. The JAR has the `Automatic-Module-Name` `org.lz4.java` and OSGi metadata.
- The JAR bundles native libraries for Linux (amd64, i386, aarch64, ppc64le, s390x, riscv64), macOS (x86_64,
  aarch64) and Windows (amd64). On other platforms, the pure Java implementation is used automatically.
- On Java 24 and newer, the JVM prints a warning when the native library is loaded. Pass
  `--enable-native-access=ALL-UNNAMED` (or the name of your module) to silence it.

> **Note:** Before 1.8.1, there was also an `lz4-pure-java` artifact without the native libraries. It has been
> discontinued and its latest version is still vulnerable to CVE‐2025‐12183. Please open an issue if you have a use
> case that requires it.

## Choosing an API

| You want to…                                                    | Use                                                                    |
|-----------------------------------------------------------------|------------------------------------------------------------------------|
| Exchange data with other tools or languages (e.g. the `lz4` CLI) | `LZ4FrameOutputStream` / `LZ4FrameInputStream` (LZ4 frame format)      |
| Compress a single `byte[]` or `ByteBuffer` and get it back       | `LZ4CompressorWithLength` / `LZ4DecompressorWithLength`                |
| Full control over raw LZ4 blocks                                 | `LZ4Compressor` / `LZ4SafeDecompressor`                                |
| Hash data                                                        | `XXHash32`, `XXHash64` and their streaming variants                    |

`LZ4BlockOutputStream` / `LZ4BlockInputStream` use a format specific to lz4-java. Prefer the frame streams for new
code, unless you need compatibility with data that was already written in the block stream format.

### Compressors

- **Fast compressor** (`fastCompressor()`): low memory footprint (~16 KB), very fast, reasonable compression ratio.
- **High compressor** (`highCompressor()`): LZ4 HC, higher memory footprint (~256 KB) and roughly 10× slower than the
  fast compressor, but a better compression ratio. The compression level can be tuned; higher levels cost
  significantly more CPU.

Both produce the same block format, and their output can be read by the same decompressor.

### Decompressors

**Use `safeDecompressor()`.** It takes the compressed length as input, is safe for untrusted data, and is the fastest
decompressor with the native implementation.

`fastDecompressor()` takes the *decompressed* length instead. It is kept for compatibility, but for security reasons it
now always uses the (slower) safe Java implementation, regardless of which factory it came from. The
`nativeInsecureInstance()` and `unsafeInsecureInstance()` factories return the old, faster fast decompressors, but
**must never be used with untrusted input**.

### Implementations

`LZ4Factory` and `XXHashFactory` provide the following implementations, which all produce compatible output:

- `nativeInstance()`: JNI bindings to the reference C implementation.
- `safeInstance()`: A pure Java port.
- `unsafeInstance()`: A Java port using `sun.misc.Unsafe`. For LZ4, this is currently an alias for the safe instance
  out of caution.

In most cases, just use `fastestInstance()`, which picks the native implementation if it can be loaded and falls back
to Java otherwise. Obtaining a factory is expensive, so store it in a static field and reuse it. See the
[LZ4Factory javadoc](https://lz4-java.yawk.at/current/javadoc/net/jpountz/lz4/LZ4Factory) for caveats of the native
implementation (temporary files, class loaders).

## Examples

### LZ4 frame streams

Files written this way can be read by the `lz4` command line tool and any other LZ4 frame implementation, and vice
versa.

```java
try (OutputStream out = new LZ4FrameOutputStream(new FileOutputStream("data.lz4"))) {
    out.write(data);
}

try (InputStream in = new LZ4FrameInputStream(new FileInputStream("data.lz4"))) {
    byte[] restored = in.readAllBytes();
}
```

### Compressing a byte array

`LZ4CompressorWithLength` prepends the original length to the compressed data, so you don't have to track it yourself.

```java
LZ4Factory factory = LZ4Factory.fastestInstance();

LZ4CompressorWithLength compressor = new LZ4CompressorWithLength(factory.fastCompressor());
byte[] compressed = compressor.compress(data);

LZ4DecompressorWithLength decompressor = new LZ4DecompressorWithLength(factory.safeDecompressor());
byte[] restored = decompressor.decompress(compressed);
```

To protect against decompression bombs, `LZ4DecompressorWithLength` refuses to allocate output buffers larger than
64 MiB by default. Pass a different maximum to the constructor, or set the
`net.jpountz.lz4.LZ4DecompressorWithLength.maxDecompressedLength` system property. For large data, use the frame
streams instead.

### Raw LZ4 blocks

With the low-level API, you are responsible for storing the original length alongside the compressed data.

```java
LZ4Factory factory = LZ4Factory.fastestInstance();

// compress
LZ4Compressor compressor = factory.fastCompressor();
byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);

// decompress
LZ4SafeDecompressor decompressor = factory.safeDecompressor();
byte[] restored = new byte[data.length];
int decompressedLength = decompressor.decompress(compressed, 0, compressedLength, restored, 0);
```

### xxHash

```java
XXHashFactory factory = XXHashFactory.fastestInstance();
long seed = 0; // any value, but use the same one for all hashes you want to compare

// one-shot
long hash = factory.hash64().hash(data, 0, data.length, seed);

// streaming
try (StreamingXXHash64 streamingHash = factory.newStreamingHash64(seed)) {
    byte[] buf = new byte[8192];
    int read;
    while ((read = in.read(buf)) != -1) {
        streamingHash.update(buf, 0, read);
    }
    long streamedHash = streamingHash.getValue();
}
```

xxHash is a very fast, non-cryptographic hash function. All implementations return the same hash for the same input,
on every JVM and platform.

## Compatibility notes

- Implementations are interchangeable: data compressed with the native implementation can be decompressed with the
  Java one, and vice versa.
- Compressed output is not guaranteed to be byte-for-byte identical across implementations, versions or platforms, but
  it can always be decompressed by any of them.

## Security

Security fixes are published as [GitHub security advisories](https://github.com/yawkat/lz4-java/security/advisories)
and noted in the [release notes](https://github.com/yawkat/lz4-java/releases). Please report vulnerabilities privately
through GitHub's [vulnerability reporting](https://github.com/yawkat/lz4-java/security/advisories/new).

## Documentation

- Javadoc: [lz4](https://lz4-java.yawk.at/current/javadoc/net/jpountz/lz4/package-summary.html),
  [xxhash](https://lz4-java.yawk.at/current/javadoc/net/jpountz/xxhash/package-summary.html)
- [Release notes](https://github.com/yawkat/lz4-java/releases) (1.8.1 and later)
- [Changelog](https://github.com/yawkat/lz4-java/blob/main/CHANGES.md) (1.8.0 and earlier)
- Benchmarks of the original project (1.8.0):
  [compression](https://lz4.github.io/lz4-java/1.8.0/lz4-compression-benchmark/),
  [decompression](https://lz4.github.io/lz4-java/1.8.0/lz4-decompression-benchmark/),
  [xxhash](https://lz4.github.io/lz4-java/1.3.0/xxhash-benchmark/)

## Building

You need:

- A JDK 25 to run Maven.
- A JDK 7 registered in your [Maven toolchains.xml](https://maven.apache.org/guides/mini/guide-using-toolchains.html),
  used to compile the library. Alternatively, pass `-Dmaven.compiler.source=8 -Dmaven.compiler.target=8` to build
  with the default JDK.
- A C compiler, to build the native library for your platform.

```bash
git submodule update --init
./mvnw verify
```

The released JAR contains native libraries for all supported platforms. These are built in GitHub Actions, see
[`build-all-and-publish.yml`](https://github.com/yawkat/lz4-java/blob/main/.github/workflows/build-all-and-publish.yml).

## License

[Apache License 2.0](https://github.com/yawkat/lz4-java/blob/main/LICENSE.txt)
