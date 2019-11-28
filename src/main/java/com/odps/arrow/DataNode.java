package com.odps.arrow;

/**
 * Created by niki.lj on 2019/8/31.
 */
public class DataNode {

  public byte[] schemas;
  public long[] bufAddrs;
  public long[] bufSizes;

  public int valueCount;

  public DataNode(byte[] schemas, long[] bufAddrs, long[] bufSizes, int valueCount) {
    this.schemas = schemas;
    this.bufAddrs = bufAddrs;
    this.bufSizes = bufSizes;
    this.valueCount = valueCount;
  }
}
