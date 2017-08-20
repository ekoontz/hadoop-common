package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MapInputRangeList implements Writable {
  private List<MapInputRange> ranges;

  @Override
  public void write(DataOutput out) throws IOException {
    if (ranges == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      WritableUtils.writeVInt(out, ranges.size());
      for (MapInputRange range : ranges) {
        range.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (ranges == null) {
      ranges = new ArrayList<MapInputRange>();
    } else {
      ranges.clear();
    }
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; ++i) {
      MapInputRange range = new MapInputRange();
      range.readFields(in);
      ranges.add(range);
    }
  }

  public List<MapInputRange> getRanges() {
    return ranges;
  }

  public void setRanges(List<MapInputRange> ranges) {
    this.ranges = ranges;
  }
}
