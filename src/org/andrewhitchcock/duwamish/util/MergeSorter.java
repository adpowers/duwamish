package org.andrewhitchcock.duwamish.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;


public class MergeSorter<T extends Message> {
  
  final int recordsToSortAtOnce = 1000;
  final int numberToMergeInMemory = 20;
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
      mergeInputFiles(output, inputs);
      
      for (int i = 0; i < currentPassSize; i++) {
        inputs[i].delete();
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sortOneFile(File inputFile) {
    ProtocolBufferReader<T> inputReader = ProtocolBufferReader.newReader(clazz, FileUtil.newInputStream(inputFile));

    List<Object[]> inMemorySortedArrays = Lists.newArrayList();
    Object[] records = new Object[recordsToSortAtOnce];
    
    int pos = 0;
    while (inputReader.hasNext()) {
      if (pos == recordsToSortAtOnce) {
        Arrays.sort(records, 0, pos, comparator);
        inMemorySortedArrays.add(records);
        records = new Object[recordsToSortAtOnce];
        pos = 0;
      }
      if (inMemorySortedArrays.size() > numberToMergeInMemory) {
        mergeInMemorySortedArrays(getNextFile(), inMemorySortedArrays);
      }
      
      records[pos] = inputReader.next();
      pos++;
    }
    
    if (pos != 0) {
      Arrays.sort(records, 0, pos, comparator);
      inMemorySortedArrays.add(records);
    }
    if (!inMemorySortedArrays.isEmpty()) {
      mergeInMemorySortedArrays(getNextFile(), inMemorySortedArrays);
    }
    
    FileUtil.closeAll(inputReader);
  }
  
  @SuppressWarnings("unchecked")
  private void mergeInMemorySortedArrays(File outputFile, List<Object[]> inMemorySortedArrays) {
    ProtocolBufferReader<T>[] entries = new ProtocolBufferReader[inMemorySortedArrays.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = ProtocolBufferReader.newReader(inMemorySortedArrays.get(i));
    }
    inMemorySortedArrays.clear();
    
    merge(outputFile, entries);
  }
  
  private void merge(File outputFile, ProtocolBufferReader<T>[] entries) {
    OutputStream outputStream = FileUtil.newOutputStream(outputFile);
    
    try {
      int bestSoFar = returnLowestIndex(entries);
      while (bestSoFar != -1) {
        entries[bestSoFar].next().writeDelimitedTo(outputStream);
        bestSoFar = returnLowestIndex(entries);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    FileUtil.closeAll(outputStream);
  }
  
  @SuppressWarnings("unchecked")
  private int returnLowestIndex(ProtocolBufferReader<T>[] readers) {
    int bestIndexSoFar = -1;
    T bestSoFar = null;
    for (int i = 0; i < readers.length; i++) {
      ProtocolBufferReader<T> reader = readers[i];
      if (reader.hasNext()) {
        if (bestSoFar == null || comparator.compare(bestSoFar, reader.peak()) > 0) {
          bestIndexSoFar = i;
          bestSoFar = reader.peak();
        }
      }
    }
    return bestIndexSoFar;
  }
  
  private void mergeInputFiles(File outputFile, File ... inputFiles) {
    @SuppressWarnings("unchecked")
    ProtocolBufferReader<T>[] entries = new ProtocolBufferReader[inputFiles.length];
    for (int i = 0; i < inputFiles.length; i++) {
      entries[i] = ProtocolBufferReader.newReader(clazz, FileUtil.newInputStream(inputFiles[i]));
    }

    merge(outputFile, entries);
    
    for (int i = 0; i < inputFiles.length; i++) {
      FileUtil.closeAll(entries[i]);
    }
  }
  
  private File getNextFile() {
    File temp = new File(tempDir, "part-" + sortCount++);
    mergeQueue.addLast(temp);
    return temp;
  }
}
