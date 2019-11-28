package com.odps.arrow;

/**
 * Created by niki.lj on 2019/8/31.
 */
public class NativeBufWrapper {

  private final long memoryAddress;

  private final long size;

  /**
   * Construct a new instance.
   */
  NativeBufWrapper(long memoryAddress, long size) {
    this.memoryAddress = memoryAddress;
    this.size = size;
  }

  /**
   * Return the size of underlying chunk of memory that has valid data.
   * @return valid data size
   */
  long getSize() {
    return size;
  }

  /**
   * Return the memory address of underlying chunk of memory.
   * @return memory address
   */
  long getMemoryAddress() {
    return memoryAddress;
  }
}
