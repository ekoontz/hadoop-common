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

package org.apache.hadoop.yarn.server.applicationhistoryservice.timeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;

/**
 * In-memory implementation of {@link TimelineStore}. This
 * implementation is for test purpose only. If users improperly instantiate it,
 * they may encounter reading and writing history data in different memory
 * store.
 * 
 */
@Private
@Unstable
public class MemoryTimelineStore
    extends AbstractService implements TimelineStore {

  private Map<EntityIdentifier, TimelineEntity> entities =
      new HashMap<EntityIdentifier, TimelineEntity>();
  private Map<EntityIdentifier, Long> entityInsertTimes =
      new HashMap<EntityIdentifier, Long>();

  public MemoryTimelineStore() {
    super(MemoryTimelineStore.class.getName());
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) {
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }

    Iterator<TimelineEntity> entityIterator = null;
    if (fromId != null) {
      TimelineEntity firstEntity = entities.get(new EntityIdentifier(fromId,
          entityType));
      if (firstEntity == null) {
        return new TimelineEntities();
      } else {
        entityIterator = new TreeSet<TimelineEntity>(entities.values())
            .tailSet(firstEntity, true).iterator();
      }
    }
    if (entityIterator == null) {
      entityIterator = new PriorityQueue<TimelineEntity>(entities.values())
          .iterator();
    }

    List<TimelineEntity> entitiesSelected = new ArrayList<TimelineEntity>();
    while (entityIterator.hasNext()) {
      TimelineEntity entity = entityIterator.next();
      if (entitiesSelected.size() >= limit) {
        break;
      }
      if (!entity.getEntityType().equals(entityType)) {
        continue;
      }
      if (entity.getStartTime() <= windowStart) {
        continue;
      }
      if (entity.getStartTime() > windowEnd) {
        continue;
      }
      if (fromTs != null && entityInsertTimes.get(new EntityIdentifier(
          entity.getEntityId(), entity.getEntityType())) > fromTs) {
        continue;
      }
      if (primaryFilter != null &&
          !matchPrimaryFilter(entity.getPrimaryFilters(), primaryFilter)) {
        continue;
      }
      if (secondaryFilters != null) { // AND logic
        boolean flag = true;
        for (NameValuePair secondaryFilter : secondaryFilters) {
          if (secondaryFilter != null && !matchPrimaryFilter(
              entity.getPrimaryFilters(), secondaryFilter) &&
              !matchFilter(entity.getOtherInfo(), secondaryFilter)) {
            flag = false;
            break;
          }
        }
        if (!flag) {
          continue;
        }
      }
      entitiesSelected.add(entity);
    }
    List<TimelineEntity> entitiesToReturn = new ArrayList<TimelineEntity>();
    for (TimelineEntity entitySelected : entitiesSelected) {
      entitiesToReturn.add(maskFields(entitySelected, fields));
    }
    Collections.sort(entitiesToReturn);
    TimelineEntities entitiesWrapper = new TimelineEntities();
    entitiesWrapper.setEntities(entitiesToReturn);
    return entitiesWrapper;
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) {
    if (fieldsToRetrieve == null) {
      fieldsToRetrieve = EnumSet.allOf(Field.class);
    }
    TimelineEntity entity = entities.get(new EntityIdentifier(entityId, entityType));
    if (entity == null) {
      return null;
    } else {
      return maskFields(entity, fieldsToRetrieve);
    }
  }

  @Override
  public TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd,
      Set<String> eventTypes) {
    TimelineEvents allEvents = new TimelineEvents();
    if (entityIds == null) {
      return allEvents;
    }
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (windowStart == null) {
      windowStart = Long.MIN_VALUE;
    }
    if (windowEnd == null) {
      windowEnd = Long.MAX_VALUE;
    }
    for (String entityId : entityIds) {
      EntityIdentifier entityID = new EntityIdentifier(entityId, entityType);
      TimelineEntity entity = entities.get(entityID);
      if (entity == null) {
        continue;
      }
      EventsOfOneEntity events = new EventsOfOneEntity();
      events.setEntityId(entityId);
      events.setEntityType(entityType);
      for (TimelineEvent event : entity.getEvents()) {
        if (events.getEvents().size() >= limit) {
          break;
        }
        if (event.getTimestamp() <= windowStart) {
          continue;
        }
        if (event.getTimestamp() > windowEnd) {
          continue;
        }
        if (eventTypes != null && !eventTypes.contains(event.getEventType())) {
          continue;
        }
        events.addEvent(event);
      }
      allEvents.addEvent(events);
    }
    return allEvents;
  }

  @Override
  public TimelinePutResponse put(TimelineEntities data) {
    TimelinePutResponse response = new TimelinePutResponse();
    for (TimelineEntity entity : data.getEntities()) {
      EntityIdentifier entityId =
          new EntityIdentifier(entity.getEntityId(), entity.getEntityType());
      // store entity info in memory
      TimelineEntity existingEntity = entities.get(entityId);
      if (existingEntity == null) {
        existingEntity = new TimelineEntity();
        existingEntity.setEntityId(entity.getEntityId());
        existingEntity.setEntityType(entity.getEntityType());
        existingEntity.setStartTime(entity.getStartTime());
        entities.put(entityId, existingEntity);
        entityInsertTimes.put(entityId, System.currentTimeMillis());
      }
      if (entity.getEvents() != null) {
        if (existingEntity.getEvents() == null) {
          existingEntity.setEvents(entity.getEvents());
        } else {
          existingEntity.addEvents(entity.getEvents());
        }
        Collections.sort(existingEntity.getEvents());
      }
      // check startTime
      if (existingEntity.getStartTime() == null) {
        if (existingEntity.getEvents() == null
            || existingEntity.getEvents().isEmpty()) {
          TimelinePutError error = new TimelinePutError();
          error.setEntityId(entityId.getId());
          error.setEntityType(entityId.getType());
          error.setErrorCode(TimelinePutError.NO_START_TIME);
          response.addError(error);
          entities.remove(entityId);
          entityInsertTimes.remove(entityId);
          continue;
        } else {
          Long min = Long.MAX_VALUE;
          for (TimelineEvent e : entity.getEvents()) {
            if (min > e.getTimestamp()) {
              min = e.getTimestamp();
            }
          }
          existingEntity.setStartTime(min);
        }
      }
      if (entity.getPrimaryFilters() != null) {
        if (existingEntity.getPrimaryFilters() == null) {
          existingEntity.setPrimaryFilters(new HashMap<String, Set<Object>>());
        }
        for (Entry<String, Set<Object>> pf :
            entity.getPrimaryFilters().entrySet()) {
          for (Object pfo : pf.getValue()) {
            existingEntity.addPrimaryFilter(pf.getKey(), maybeConvert(pfo));
          }
        }
      }
      if (entity.getOtherInfo() != null) {
        if (existingEntity.getOtherInfo() == null) {
          existingEntity.setOtherInfo(new HashMap<String, Object>());
        }
        for (Entry<String, Object> info : entity.getOtherInfo().entrySet()) {
          existingEntity.addOtherInfo(info.getKey(),
              maybeConvert(info.getValue()));
        }
      }
      // relate it to other entities
      if (entity.getRelatedEntities() == null) {
        continue;
      }
      for (Map.Entry<String, Set<String>> partRelatedEntities : entity
          .getRelatedEntities().entrySet()) {
        if (partRelatedEntities == null) {
          continue;
        }
        for (String idStr : partRelatedEntities.getValue()) {
          EntityIdentifier relatedEntityId =
              new EntityIdentifier(idStr, partRelatedEntities.getKey());
          TimelineEntity relatedEntity = entities.get(relatedEntityId);
          if (relatedEntity != null) {
            relatedEntity.addRelatedEntity(
                existingEntity.getEntityType(), existingEntity.getEntityId());
          } else {
            relatedEntity = new TimelineEntity();
            relatedEntity.setEntityId(relatedEntityId.getId());
            relatedEntity.setEntityType(relatedEntityId.getType());
            relatedEntity.setStartTime(existingEntity.getStartTime());
            relatedEntity.addRelatedEntity(existingEntity.getEntityType(),
                existingEntity.getEntityId());
            entities.put(relatedEntityId, relatedEntity);
            entityInsertTimes.put(relatedEntityId, System.currentTimeMillis());
          }
        }
      }
    }
    return response;
  }

  private static TimelineEntity maskFields(
      TimelineEntity entity, EnumSet<Field> fields) {
    // Conceal the fields that are not going to be exposed
    TimelineEntity entityToReturn = new TimelineEntity();
    entityToReturn.setEntityId(entity.getEntityId());
    entityToReturn.setEntityType(entity.getEntityType());
    entityToReturn.setStartTime(entity.getStartTime());
    entityToReturn.setEvents(fields.contains(Field.EVENTS) ?
        entity.getEvents() : fields.contains(Field.LAST_EVENT_ONLY) ?
            Arrays.asList(entity.getEvents().get(0)) : null);
    entityToReturn.setRelatedEntities(fields.contains(Field.RELATED_ENTITIES) ?
        entity.getRelatedEntities() : null);
    entityToReturn.setPrimaryFilters(fields.contains(Field.PRIMARY_FILTERS) ?
        entity.getPrimaryFilters() : null);
    entityToReturn.setOtherInfo(fields.contains(Field.OTHER_INFO) ?
        entity.getOtherInfo() : null);
    return entityToReturn;
  }

  private static boolean matchFilter(Map<String, Object> tags,
      NameValuePair filter) {
    Object value = tags.get(filter.getName());
    if (value == null) { // doesn't have the filter
      return false;
    } else if (!value.equals(filter.getValue())) { // doesn't match the filter
      return false;
    }
    return true;
  }

  private static boolean matchPrimaryFilter(Map<String, Set<Object>> tags,
      NameValuePair filter) {
    Set<Object> value = tags.get(filter.getName());
    if (value == null) { // doesn't have the filter
      return false;
    } else {
      return value.contains(filter.getValue());
    }
  }

  private static Object maybeConvert(Object o) {
    if (o instanceof Long) {
      Long l = (Long)o;
      if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
        return l.intValue();
      }
    }
    return o;
  }

}
