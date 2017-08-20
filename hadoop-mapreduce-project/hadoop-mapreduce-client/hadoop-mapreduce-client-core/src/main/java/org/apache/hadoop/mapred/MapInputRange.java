package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MapInputRange implements Writable {
  public long start;
  public long end;
  public MapInputRange(long start, long end) {
    this.start = start;
    this.end = end;
  }
  public String toString() {
    return "(" + start + "," + end + ")";
  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(start);
    out.writeLong(end);
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    start = in.readLong();
    end = in.readLong();
  }
  public MapInputRange() {
    super();
    // TODO Auto-generated constructor stub
  }

}
