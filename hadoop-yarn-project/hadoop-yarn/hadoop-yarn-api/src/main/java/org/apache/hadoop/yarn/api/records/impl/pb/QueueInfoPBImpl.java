/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class QueueInfoPBImpl extends ProtoBase<QueueInfoProto> implements
    QueueInfo {

  QueueInfoProto proto = QueueInfoProto.getDefaultInstance();
  QueueInfoProto.Builder builder = null;
  boolean viaProto = false;

  List<ApplicationReport> applicationsList;
  List<QueueInfo> childQueuesList;
  
  public QueueInfoPBImpl() {
    builder = QueueInfoProto.newBuilder();
  }
  
  public QueueInfoPBImpl(QueueInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ApplicationReport> getApplications() {
    initLocalApplicationsList();
    return this.applicationsList;
  }

  @Override
  public float getCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapacity()) ? p.getCapacity() : -1;
  }

  @Override
  public List<QueueInfo> getChildQueues() {
    initLocalChildQueuesList();
    return this.childQueuesList;
  }

  @Override
  public float getCurrentCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCurrentCapacity()) ? p.getCurrentCapacity() : 0;
  }

  @Override
  public float getMaximumCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasMaximumCapacity()) ? p.getMaximumCapacity() : -1;
  }

  @Override
  public String getQueueName() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public QueueState getQueueState() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setApplications(List<ApplicationReport> applications) {
    if (applications == null) {
      builder.clearApplications();
    }
    this.applicationsList = applications;
  }

  @Override
  public void setCapacity(float capacity) {
    maybeInitBuilder();
    builder.setCapacity(capacity);
  }

  @Override
  public void setChildQueues(List<QueueInfo> childQueues) {
    if (childQueues == null) {
      builder.clearChildQueues();
    }
    this.childQueuesList = childQueues;
  }

  @Override
  public void setCurrentCapacity(float currentCapacity) {
    maybeInitBuilder();
    builder.setCurrentCapacity(currentCapacity);
  }

  @Override
  public void setMaximumCapacity(float maximumCapacity) {
    maybeInitBuilder();
    builder.setMaximumCapacity(maximumCapacity);
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName(queueName);
  }

  @Override
  public void setQueueState(QueueState queueState) {
    maybeInitBuilder();
    if (queueState == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(queueState));
  }

  @Override
  public QueueInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void initLocalApplicationsList() {
    if (this.applicationsList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationReportProto> list = p.getApplicationsList();
    applicationsList = new ArrayList<ApplicationReport>();

    for (ApplicationReportProto a : list) {
      applicationsList.add(convertFromProtoFormat(a));
    }
  }

  private void addApplicationsToProto() {
    maybeInitBuilder();
    builder.clearApplications();
    if (applicationsList == null)
      return;
    Iterable<ApplicationReportProto> iterable = new Iterable<ApplicationReportProto>() {
      @Override
      public Iterator<ApplicationReportProto> iterator() {
        return new Iterator<ApplicationReportProto>() {
  
          Iterator<ApplicationReport> iter = applicationsList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public ApplicationReportProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllApplications(iterable);
  }

  private void initLocalChildQueuesList() {
    if (this.childQueuesList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueInfoProto> list = p.getChildQueuesList();
    childQueuesList = new ArrayList<QueueInfo>();

    for (QueueInfoProto a : list) {
      childQueuesList.add(convertFromProtoFormat(a));
    }
  }

  private void addChildQueuesInfoToProto() {
    maybeInitBuilder();
    builder.clearChildQueues();
    if (childQueuesList == null)
      return;
    Iterable<QueueInfoProto> iterable = new Iterable<QueueInfoProto>() {
      @Override
      public Iterator<QueueInfoProto> iterator() {
        return new Iterator<QueueInfoProto>() {
  
          Iterator<QueueInfo> iter = childQueuesList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public QueueInfoProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllChildQueues(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.childQueuesList != null) {
      addChildQueuesInfoToProto();
    }
    if (this.applicationsList != null) {
      addApplicationsToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }


  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto a) {
    return new ApplicationReportPBImpl(a);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }

  private QueueInfoPBImpl convertFromProtoFormat(QueueInfoProto a) {
    return new QueueInfoPBImpl(a);
  }
  
  private QueueInfoProto convertToProtoFormat(QueueInfo q) {
    return ((QueueInfoPBImpl)q).getProto();
  }

  private QueueState convertFromProtoFormat(QueueStateProto q) {
    return ProtoUtils.convertFromProtoFormat(q);
  }
  
  private QueueStateProto convertToProtoFormat(QueueState queueState) {
    return ProtoUtils.convertToProtoFormat(queueState);
  }

}
