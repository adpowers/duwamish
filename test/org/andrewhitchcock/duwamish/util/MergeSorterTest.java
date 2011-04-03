/**
 * Copyright 2011 Andrew Hitchcock
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
    System.out.println(test(1000000));
  }
  
  private static long test(int count) throws Exception {   
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
    
    long startTime = System.currentTimeMillis();

    MergeSorter<DoubleMessage> ms = MergeSorter.create(DoubleMessage.class, new DoubleMessageComparator());
    ms.sort(outputFile, inputFile);
    
    long endTime = System.currentTimeMillis();
    
    InputStream is = FileUtil.newInputStream(outputFile);
    DoubleMessage message = getNext(is);
    double previous = message.getValue();
    int encountered = 0;
    while (message != null) {
      if (previous > message.getValue()) {
        throw new RuntimeException("Bug! Elements not in sorted order.");
      }
      previous = message.getValue();
      encountered++;
      message = getNext(is);
    }
    
    if (encountered != count) {
      throw new RuntimeException("Bug! Wrong element count.");
    }
    
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

