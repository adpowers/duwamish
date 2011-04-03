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
