package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MapTaskSpillInfosUpdate implements Writable {
  private MapTaskSpillInfo[] infos;

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, infos.length);
    for (int i = 0; i < infos.length; ++i) {
      infos[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    infos = new MapTaskSpillInfo[WritableUtils.readVInt(in)];
    for (int i = 0; i < infos.length; ++i) {
      infos[i] = new MapTaskSpillInfo();
      infos[i].readFields(in);
    }
  }

  public MapTaskSpillInfosUpdate(MapTaskSpillInfo[] infos) {
    super();
    this.infos = infos;
  }

  public MapTaskSpillInfosUpdate() {
    super();
    // TODO Auto-generated constructor stub
  }

  public MapTaskSpillInfo[] getInfos() {
    return infos;
  }

  public void setInfos(MapTaskSpillInfo[] infos) {
    this.infos = infos;
  }
  
}
