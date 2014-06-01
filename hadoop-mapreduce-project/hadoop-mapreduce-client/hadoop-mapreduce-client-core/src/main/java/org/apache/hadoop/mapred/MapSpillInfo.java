package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class MapSpillInfo implements WritableComparable<MapSpillInfo> {
  private long start;
  private long end;
  private int spillIndex;
  private int attemptIndex;
  public long getStart() {
    return start;
  }
  public void setStart(long start) {
    this.start = start;
  }
  public long getEnd() {
    return end;
  }
  public void setEnd(long end) {
    this.end = end;
  }
  public int getSpillIndex() {
    return spillIndex;
  }
  public void setSpillIndex(int index) {
    this.spillIndex = index;
  }
  
  public int getAttemptIndex() {
    return attemptIndex;
  }
  public void setAttemptIndex(int attemptIndex) {
    this.attemptIndex = attemptIndex;
  }
  
  public MapSpillInfo() {
  }
  public MapSpillInfo(long start, long end, int spillIndex, int attemptIndex) {
    super();
    this.start = start;
    this.end = end;
    this.spillIndex = spillIndex;
    this.attemptIndex = attemptIndex;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(start);
    out.writeLong(end);
    WritableUtils.writeVInt(out, spillIndex);
    WritableUtils.writeVInt(out, attemptIndex);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    start = in.readLong();
    end = in.readLong();
    spillIndex = WritableUtils.readVInt(in);
    attemptIndex = WritableUtils.readVInt(in);
  }
  @Override
  public int compareTo(MapSpillInfo other) {
    if (this.start != other.start) {
      return this.start - other.start > 0 ? 1 : -1;
    } else {
      return Long.compare(this.end, other.end);
    }
  }
  
  public long getLength() {
    return end - start;
  }
  
  public String toString() {
    return "(" + spillIndex + ", " + start + ", " + end + ")";
  }
}
