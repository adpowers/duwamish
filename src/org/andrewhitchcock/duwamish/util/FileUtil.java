package org.andrewhitchcock.duwamish.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileUtil {
  public static OutputStream newOutputStream(File file) {
    try {
      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }
      return new BufferedOutputStream(new FileOutputStream(file));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static InputStream newInputStream(File file) {
    try {
      return new BufferedInputStream(new FileInputStream(file));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void closeAll(Closeable ... c) {
    if (c.length == 0) {
      return;
    }
    Closeable first = c[0];
    try {
      first.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Closeable[] cs = new Closeable[c.length - 1];
      System.arraycopy(c, 1, cs, 0, c.length - 1);
      closeAll(cs);
    }
  }
}
