package org.andrewhitchcock.duwamish.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;


public class MergeSorter<T extends Message> {
  
  final int recordsToSortAtOnce = 5000;
  final int numberToMergeInMemory = 100;
  final int numberToMergeFromDisk = 50;

  final Class<T> clazz;
  @SuppressWarnings("unchecked")
  final Comparator comparator;
  final File tempDir;
  
  int sortCount = 0;
  Deque<File> mergeQueue = new ArrayDeque<File>();
  
  final Method builderMethod;
  
  private MergeSorter(Class<T> clazz, Comparator<T> comparator, File tempDir) {
    this.clazz = clazz;
    this.comparator = comparator;
    this.tempDir = tempDir;
    try {
      this.builderMethod = clazz.getMethod("newBuilder");
    } catch (Exception e) {
      throw new RuntimeException();
    }
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
    while (!mergeQueue.isEmpty()) {
      boolean moreThanOnePassLeft = mergeQueue.size() > numberToMergeFromDisk;
      int currentPassSize = moreThanOnePassLeft ? numberToMergeFromDisk : mergeQueue.size();
      
      File[] inputs = new File[currentPassSize];
      for (int i = 0; i < currentPassSize; i++) {
        inputs[i] = mergeQueue.pop();
      }
      
      File output = moreThanOnePassLeft ? getNextFile() : outputFile;
      mergeFiles(output, inputs);
      
      for (int i = 0; i < currentPassSize; i++) {
        inputs[i].delete();
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sortOneFile(File inputFile) {
    InputStream inputStream = FileUtil.newInputStream(inputFile);

    List<Object[]> inMemorySortedArrays = Lists.newArrayList();
    Object[] records = new Object[recordsToSortAtOnce];
    
    int pos = 0;
    T next = getNext(inputStream);
    while (next != null) {
      if (pos == recordsToSortAtOnce) {
        Arrays.sort(records, 0, pos, comparator);
        inMemorySortedArrays.add(records);
        records = new Object[recordsToSortAtOnce];
        pos = 0;
      }
      if (inMemorySortedArrays.size() > numberToMergeInMemory) {
        mergeAndWriteRecordsToFile(getNextFile(), inMemorySortedArrays);
      }
      
      records[pos] = next;
      pos++;
      next = getNext(inputStream);
    }
    
    if (pos != 0) {
      Arrays.sort(records, 0, pos, comparator);
      inMemorySortedArrays.add(records);
    }
    if (!inMemorySortedArrays.isEmpty()) {
      mergeAndWriteRecordsToFile(getNextFile(), inMemorySortedArrays);
    }
    
    FileUtil.closeAll(inputStream);
  }
  
  @SuppressWarnings("unchecked")
  private void mergeAndWriteRecordsToFile(File outputFile, List<Object[]> inMemorySortedArrays) {
    InMemoryMergeEntry[] entries = new InMemoryMergeEntry[inMemorySortedArrays.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = new InMemoryMergeEntry();
      entries[i].values = inMemorySortedArrays.get(i);
    }
    inMemorySortedArrays.clear();
    
    try {
      OutputStream outputStream = FileUtil.newOutputStream(outputFile);
      int bestSoFar = returnLowestIndex(entries);
      while (bestSoFar != -1) {
        T current = (T)(entries[bestSoFar].values[entries[bestSoFar].position]);
        current.writeDelimitedTo(outputStream);
        entries[bestSoFar].position++;
        bestSoFar = returnLowestIndex(entries);
      }
      FileUtil.closeAll(outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  private int returnLowestIndex(InMemoryMergeEntry[] entries) {
    int bestIndexSoFar = -1;
    T bestSoFar = null;
    for (int i = 0; i < entries.length; i++) {
      InMemoryMergeEntry entry = entries[i];
      if (entry.position < entry.values.length) {
        T current = (T)entry.values[entry.position];
        if (current != null && (bestSoFar == null || comparator.compare(bestSoFar, current) > 0)) {
          bestIndexSoFar = i;
          bestSoFar = current;
        }
      }
    }
    return bestIndexSoFar;
  }
  
  private void mergeFiles(File outputFile, File ... inputFiles) {
    @SuppressWarnings("unchecked")
    FileMergeEntry<T>[] entries = new FileMergeEntry[inputFiles.length];
    for (int i = 0; i < inputFiles.length; i++) {
      entries[i] = new FileMergeEntry<T>();
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
  private int returnLowestIndex(FileMergeEntry<T>[] entries) {
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
      Builder builder = (Builder)(builderMethod.invoke(null));
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
  
  private static class FileMergeEntry<T> {
    InputStream inputStream;
    T value;
  }
  
  private static class InMemoryMergeEntry {
    Object[] values;
    int position = 0;
  }
}
