package org.andrewhitchcock.duwamish.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;


public class MergeSorter<T extends Message> {
  
  final int recordsToSortAtOnce = 1000;
  final int numberToMergeAtOnce = 10;

  final Class<T> clazz;
  @SuppressWarnings("unchecked")
  final Comparator comparator;
  final File tempDir;
  
  int sortCount = 0;
  Deque<File> mergeQueue = new ArrayDeque<File>();
  
  final Object[] records = new Object[recordsToSortAtOnce];
  
  private MergeSorter(Class<T> clazz, Comparator<T> comparator, File tempDir) {
    this.clazz = clazz;
    this.comparator = comparator;
    this.tempDir = tempDir;
  }
  
  public static <T extends Message> MergeSorter<T> create(Class<T> clazz, Comparator<T> comparator, File tempDir) {
    return new MergeSorter<T>(clazz, comparator, tempDir);
  }
  
  
  public void sort(File outputFile, File ... inputFiles) {
    // sort each input file
    for (File inputFile : inputFiles) {
      sortOneFile(inputFile);
    }
    
    // merge files
    System.out.println("Merging");
    while (!mergeQueue.isEmpty()) {
      boolean moreThanOnePassLeft = mergeQueue.size() > numberToMergeAtOnce;
      int currentPassSize = moreThanOnePassLeft ? numberToMergeAtOnce : mergeQueue.size();
      
      File[] inputs = new File[currentPassSize];
      for (int i = 0; i < currentPassSize; i++) {
        inputs[i] = mergeQueue.pop();
      }
      
      File output = moreThanOnePassLeft ? getNextFile() : outputFile;
      merge(output, inputs);
      
      for (int i = 0; i < currentPassSize; i++) {
        inputs[i].delete();
      }
    }
  }
  
  private void sortOneFile(File inputFile) {
    InputStream inputStream = FileUtil.newInputStream(inputFile);
    
    int pos = 0;
    T next = getNext(inputStream);
    while (next != null) {
      if (pos == recordsToSortAtOnce) {
        sortAndWriteRecordsToFile(getNextFile(), pos);
        pos = 0;
      }
      records[pos] = next;
      pos++;
      next = getNext(inputStream);
    }
    
    if (pos != 0) {
      sortAndWriteRecordsToFile(getNextFile(), pos);
    }
    
    FileUtil.closeAll(inputStream);
  }
  
  @SuppressWarnings("unchecked")
  private void sortAndWriteRecordsToFile(File outputFile, int count) {
    Arrays.sort(records, 0, count, comparator);
    try {
      OutputStream outputStream = FileUtil.newOutputStream(outputFile);
      for (int i = 0; i < count; i++) {
        ((T)records[i]).writeDelimitedTo(outputStream);
        records[i] = null;
      }
      FileUtil.closeAll(outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void merge(File outputFile, File ... inputFiles) {
    @SuppressWarnings("unchecked")
    MergeEntry<T>[] entries = new MergeEntry[inputFiles.length];
    for (int i = 0; i < inputFiles.length; i++) {
      entries[i] = new MergeEntry<T>();
      entries[i].inputStream = FileUtil.newInputStream(inputFiles[i]);
      entries[i].value = getNext(entries[i].inputStream);
    }

    OutputStream outputStream = FileUtil.newOutputStream(outputFile);
    
    try {
      int bestSoFar = returnLowestIndex(entries);
      while (bestSoFar != -1) {
        entries[bestSoFar].value.writeDelimitedTo(outputStream);
        entries[bestSoFar].value = getNext(entries[bestSoFar].inputStream);
        bestSoFar = returnLowestIndex(entries);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    for (int i = 0; i < inputFiles.length; i++) {
      FileUtil.closeAll(entries[i].inputStream);
    }
    FileUtil.closeAll(outputStream);
  }
  
  @SuppressWarnings("unchecked")
  private int returnLowestIndex(MergeEntry<T>[] entries) {
    int bestIndexSoFar = -1;
    T bestSoFar = null;
    for (int i = 0; i < entries.length; i++) {
      if (entries[i].value != null) {
        if (bestSoFar == null || comparator.compare(bestSoFar, entries[i].value) > 0) {
          bestIndexSoFar = i;
          bestSoFar = entries[i].value;
        }
      }
    }
    return bestIndexSoFar;
  }
  
  @SuppressWarnings("unchecked")
  private T getNext(InputStream inputStream) {
    try {
      Builder builder = (Builder)(clazz.getMethod("newBuilder").invoke(null));
      boolean success = builder.mergeDelimitedFrom(inputStream);
      if (success) {
        return (T)builder.build();
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private File getNextFile() {
    File temp = new File(tempDir, "part-" + sortCount++);
    mergeQueue.addLast(temp);
    return temp;
  }
  
  private static class MergeEntry<T> {
    InputStream inputStream;
    T value;
  }
}
