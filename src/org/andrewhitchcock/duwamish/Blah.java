package org.andrewhitchcock.duwamish;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;


public class Blah {
  public static void main(String[] argss) {
    String[] values = new String[2000000];
    Random r = new Random();
    for (int i = 0; i < values.length; i++) {
      values[i] = Integer.toString(r.nextInt());
    }
    
    long startTime = System.currentTimeMillis();
    Arrays.sort(values, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });
    long endTime = System.currentTimeMillis();
    System.out.println(endTime - startTime);
  }
}
