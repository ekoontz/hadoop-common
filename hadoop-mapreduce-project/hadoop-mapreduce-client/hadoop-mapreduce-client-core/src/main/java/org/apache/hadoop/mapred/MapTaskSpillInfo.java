package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class MapTaskSpillInfo implements Writable {
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  static public enum Status {FAILED, NEW_SPILL, TIP_FAIL};
  
  public static final MapTaskSpillInfo[] EMPTY_SPILL_INFOS = new MapTaskSpillInfo[0];
  
  private int infoId;
  private String nodeHttp;
  private TaskAttemptID attemptID;
  private Status status;
  private MapSpillInfo spillInfo;
  
  public MapTaskSpillInfo() {
    super();
  }
  public MapTaskSpillInfo(int infoId, String nodeHttp, TaskAttemptID taskId,
      Status status, MapSpillInfo spillInfo) {
    super();
    this.infoId = infoId;
    this.nodeHttp = nodeHttp;
    this.attemptID = taskId;
    this.status = status;
    this.spillInfo = spillInfo;
  }
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, infoId);
    WritableUtils.writeString(out, nodeHttp);
    attemptID.write(out);
    WritableUtils.writeEnum(out, status);
    if (spillInfo != null) {
      out.writeBoolean(true);
      spillInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    infoId = WritableUtils.readVInt(in);
    nodeHttp = WritableUtils.readString(in);
    if (attemptID == null) {
      attemptID = new org.apache.hadoop.mapred.TaskAttemptID();
    }
    attemptID.readFields(in);
    status = WritableUtils.readEnum(in, Status.class);
    if (in.readBoolean()) {
      if (spillInfo == null) {
        spillInfo = new MapSpillInfo();
      }
      spillInfo.readFields(in);
    } else {
      spillInfo = null;
    }
  }
  public int getInfoId() {
    return infoId;
  }
  public void setInfoId(int infoId) {
    this.infoId = infoId;
  }
  public String getNodeHttp() {
    return nodeHttp;
  }
  public void setNodeHttp(String nodeHttp) {
    this.nodeHttp = nodeHttp;
  }
  public TaskAttemptID getAttemptID() {
    return attemptID;
  }
  public void setAttemptID(TaskAttemptID taskId) {
    this.attemptID = taskId;
  }
  public Status getStatus() {
    return status;
  }
  public void setStatus(Status status) {
    this.status = status;
  }
  public MapSpillInfo getSpillInfo() {
    return spillInfo;
  }
  public void setSpillInfo(MapSpillInfo spillInfo) {
    this.spillInfo = spillInfo;
  }
  
  public String toString() {
    return String.format("(%d, %s, %s, %s, %s)", infoId, nodeHttp, attemptID.toString(), status.toString(), spillInfo);
  }
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof MapTaskSpillInfo) {
      MapTaskSpillInfo m = (MapTaskSpillInfo)other;
      return this.infoId == m.infoId;
    } else {
      return false;
    }
  }
  
  public boolean equals(MapTaskSpillInfo other) {
    return infoId == other.infoId;
  }
  @Override
  public int hashCode() {
    return this.infoId;
  }
}
