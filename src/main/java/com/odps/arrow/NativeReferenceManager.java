package com.odps.arrow;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by niki.lj on 2019/8/31.
 */
public class NativeReferenceManager implements ReferenceManager {

  private final AtomicInteger bufRefCnt = new AtomicInteger(0);

  private NativeBufWrapper memory;

  public NativeReferenceManager(NativeBufWrapper memory) {
    this.memory = memory;
  }

  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    // decrement the ref count
    final int refCnt;
    synchronized (this) {
      refCnt = bufRefCnt.addAndGet(-decrement);
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);
    bufRefCnt.addAndGet(increment);
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();
    return srcBuffer;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length) {
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(
        this,
        null,
        length, // length (in bytes) in the underlying memory chunk for this new ArrowBuf
        derivedBufferAddress, // starting byte address in the underlying memory for this new ArrowBuf,
        false);

    return derivedBuf;
  }

  @Override
  public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator getAllocator() {
    return null;
  }

  @Override
  public int getSize() {
    return (int)memory.getSize();
  }

  @Override
  public int getAccountedSize() {
    return 0;
  }
}
