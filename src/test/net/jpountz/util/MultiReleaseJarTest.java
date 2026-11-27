package net.jpountz.util;

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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The failsafe execution sets lz4.testJar and runs the tests against the multi-release jar. There ByteArrays must be
 * the Java 9 variant, or that run tests the Java 7 class a second time.
 */
@EnabledIfSystemProperty(named = "lz4.testJar", matches = "true", disabledReason = "only runs against the packaged jar")
public class MultiReleaseJarTest {
  private static final int JAVA_9_CLASS_FILE = 53;

  @Test
  public void jarServesJava9ByteArrays() throws Exception {
    assertEquals(JAVA_9_CLASS_FILE, classFileMajorVersion(ByteArrays.class));
  }

  /**
   * Code compiles against the Java 7 ByteArrays, so a method or field missing from the Java 9 one fails only at run
   * time. Private and synthetic members are not reachable from other classes and may differ.
   */
  @Test
  public void java9ByteArraysHasTheSameMembers() throws Exception {
    byte[] java7Class;
    // Without a runtime version, JarFile returns the base entry rather than the META-INF/versions/9 one.
    try (JarFile jarFile = new JarFile(jar());
         InputStream in = jarFile.getInputStream(jarFile.getEntry("net/jpountz/util/ByteArrays.class"))) {
      java7Class = in.readAllBytes();
    }
    class Java7Loader extends ClassLoader {
      Java7Loader() {
        super(null);
      }

      Class<?> define() {
        return defineClass(ByteArrays.class.getName(), java7Class, 0, java7Class.length);
      }
    }
    assertEquals(accessibleMembers(new Java7Loader().define()), accessibleMembers(ByteArrays.class));
  }

  private static Set<String> accessibleMembers(Class<?> c) {
    return Stream.<Member>concat(Arrays.stream(c.getDeclaredMethods()), Arrays.stream(c.getDeclaredFields()))
        .filter(m -> !Modifier.isPrivate(m.getModifiers()) && !m.isSynthetic())
        .map(Member::toString)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Felix and Equinox refuse to resolve a bundle with an empty manifest header. bnd writes an empty Require-Capability
   * into META-INF/versions/9/OSGI-INF/MANIFEST.MF unless lz4-java.bnd disables those per-release manifests.
   */
  @Test
  public void manifestsHaveNoEmptyHeaders() throws Exception {
    try (JarFile jarFile = new JarFile(jar())) {
      for (Enumeration<JarEntry> entries = jarFile.entries(); entries.hasMoreElements(); ) {
        JarEntry entry = entries.nextElement();
        if (!entry.getName().endsWith("MANIFEST.MF")) {
          continue;
        }
        try (InputStream in = jarFile.getInputStream(entry)) {
          for (Map.Entry<Object, Object> header : new Manifest(in).getMainAttributes().entrySet()) {
            assertFalse(header.getValue().toString().trim().isEmpty(), entry.getName() + ": empty " + header.getKey());
          }
        }
      }
    }
  }

  private static File jar() throws Exception {
    return new File(ByteArrays.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  private static int classFileMajorVersion(Class<?> c) throws IOException {
    try (InputStream resource = c.getResourceAsStream(c.getSimpleName() + ".class");
         DataInputStream in = new DataInputStream(resource)) {
      in.readInt(); // magic
      in.readUnsignedShort(); // minor version
      return in.readUnsignedShort();
    }
  }
}
