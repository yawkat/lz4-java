# Contributing to lz4-java

Thanks for your interest in contributing!

## Reporting bugs and requesting features

- **Bugs and feature requests:** open a [GitHub issue](https://github.com/yawkat/lz4-java/issues). For bugs, include
  the lz4-java version, Java version, OS and architecture, which `LZ4Factory`/`XXHashFactory` instance you use, and
  ideally a minimal reproducer.
- **Questions:** use [GitHub Discussions](https://github.com/yawkat/lz4-java/discussions).
- **Security vulnerabilities:** do **not** open a public issue. Report them privately through
  [GitHub's vulnerability reporting](https://github.com/yawkat/lz4-java/security/advisories/new).

## Pull requests

Pull requests are welcome. For larger changes, please open an issue first to discuss the approach.

To be accepted, a contribution must meet these requirements:

- **The build passes.** `./mvnw verify` must succeed, and CI must be green on all platforms. See the
  [README](README.md#building) for build prerequisites.
- **Java 7 compatibility.** The library is compiled with Java 7 and must not use newer language features or APIs.
  Tests may use the newer Java version configured in `pom.xml`.
- **No breaking API changes.** Public API must stay source and binary compatible. Deprecate instead of removing, and
  keep new non-user-facing members as private as possible.
- **Tests.** Bug fixes should come with a regression test, and new features with tests. New tests use JUnit 5 or
  newer (existing JUnit 4 tests run through the vintage engine). Code that parses input (decompressors, streams)
  should also be covered by a [Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) fuzz test, run with the
  `fuzz` Maven profile. Each fuzz test needs its own execution in that profile.
- **Code style.** There is no automated formatter. Follow the style of the surrounding code and
  [`.editorconfig`](.editorconfig) (2-space indentation), and keep diffs minimal: no unrelated reformatting.
- **Generated code.** Most of the Java and Unsafe implementations are generated from the MVEL templates in
  `src/build/source_templates`. Edit the templates, not the generated files under `target/`.
- **Implementations stay consistent.** A change to one implementation (JNI, Java safe, Java unsafe) usually needs a
  matching change in the others.
- **License.** Contributions are accepted under the [Apache License 2.0](LICENSE.txt), the project's license. New
  source files need the Apache license header used by existing files.

Keep pull requests focused on a single change and describe what changed and why.
