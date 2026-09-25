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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NativeTest {
  private static final long NOW = TimeUnit.DAYS.toMillis(365 * 50);
  private static final long OLD = NOW - TimeUnit.HOURS.toMillis(2);
  private static final long FRESH = NOW - TimeUnit.MINUTES.toMillis(5);

  @TempDir
  File dir;

  private File file(String name, long lastModified) throws IOException {
    File f = new File(dir, name);
    assertTrue(f.createNewFile());
    assertTrue(f.setLastModified(lastModified));
    return f;
  }

  @Test
  public void cleanupOldTempLibs() throws IOException {
    File oldLib = file("liblz4-java-123.so", OLD);
    File oldLock = file("liblz4-java-123.so.lck", OLD);
    File oldLockOnly = file("liblz4-java-456.dll.lck", OLD);
    File freshLib = file("liblz4-java-789.so", FRESH);
    File freshLock = file("liblz4-java-789.so.lck", FRESH);
    File oldOther = file("libother-123.so", OLD);

    Native.cleanupOldTempLibs(dir, NOW);

    assertFalse(oldLib.exists());
    assertFalse(oldLock.exists());
    assertFalse(oldLockOnly.exists());
    assertTrue(freshLib.exists());
    assertTrue(freshLock.exists());
    assertTrue(oldOther.exists());
  }

  @Test
  public void cleanupOldTempLibsMissingDirectory() {
    Native.cleanupOldTempLibs(new File(dir, "missing"), NOW);
  }
}
