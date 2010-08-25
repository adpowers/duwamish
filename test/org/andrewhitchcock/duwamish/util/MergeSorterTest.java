package org.andrewhitchcock.duwamish.util;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Random;

import org.andrewhitchcock.duwamish.example.protos.Examples.DoubleMessage;
import org.andrewhitchcock.duwamish.util.FileUtil;
import org.andrewhitchcock.duwamish.util.MergeSorter;

public class MergeSorterTest {
  public static void main(String[] args) throws Exception {
    System.out.println(test(50000));
    System.out.println(test(100000));
    System.out.println(test(10000000));
  }
  
  private static long test(int count) throws Exception {
    long startTime = System.currentTimeMillis();
    
    File inputFile = new File("/tmp/input");
    File outputFile = new File("/tmp/output");
    if (inputFile.exists()) {
      inputFile.delete();
    }
    if (outputFile.exists()) {
      outputFile.delete();
    }
    
    Random r = new Random();
    OutputStream os = FileUtil.newOutputStream(inputFile);
    
    for (int i = 0; i < count; i++) {
      DoubleMessage.newBuilder().setValue(r.nextDouble()).build().writeDelimitedTo(os);
    }
    
    FileUtil.closeAll(os);
    
    MergeSorter<DoubleMessage> ms = MergeSorter.create(DoubleMessage.class, new DoubleMessageComparator(), new File("/tmp/merger/"));
    ms.sort(outputFile, inputFile);
    
    long endTime = System.currentTimeMillis();
    
    InputStream is = FileUtil.newInputStream(outputFile);
    DoubleMessage message = getNext(is);
    double previous = message.getValue();
    int encountered = 0;
    while (message != null) {
      if (previous > message.getValue()) {
        throw new RuntimeException("Bug!");
      }
      previous = message.getValue();
      encountered++;
      message = getNext(is);
    }
    
    System.out.println("encountered: " + encountered);
    return endTime - startTime;
  }
  
  
  private static class DoubleMessageComparator implements Comparator<DoubleMessage> {
    @Override
    public int compare(DoubleMessage o1, DoubleMessage o2) {
      return Double.compare(o1.getValue(), o2.getValue());
    }
  }

  private static DoubleMessage getNext(InputStream inputStream) throws Exception {
    DoubleMessage.Builder builder = DoubleMessage.newBuilder();
    boolean success = builder.mergeDelimitedFrom(inputStream);
    if (success) {
      return builder.build();
    } else {
      return null;
    }
  }
}

