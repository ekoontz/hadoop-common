package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MapSpillInfo implements WritableComparable<MapSpillInfo> {
  private long start;
  private long end;
  private int index;
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
  public int getIndex() {
    return index;
  }
  public void setIndex(int index) {
    this.index = index;
  }
  
  
  
  public MapSpillInfo() {
  }
  public MapSpillInfo(long start, long end, int index) {
    super();
    this.start = start;
    this.end = end;
    this.index = index;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(start);
    out.writeLong(end);
    out.writeInt(index);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    start = in.readLong();
    end = in.readLong();
    index = in.readInt();
  }
  @Override
  public int compareTo(MapSpillInfo other) {
    if (this.start != other.start) {
      return this.start - other.start > 0 ? 1 : -1;
    } else {
      return Long.compare(this.end, other.end);
    }
  }
  
  public String toString() {
    return "(" + index + ", " + start + ", " + end + ")";
  }
}
