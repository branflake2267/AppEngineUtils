package org.gonevertical.appengineutils.util;

import java.util.Date;

import com.google.appengine.api.datastore.Entity;

public class EntityStat {
  /**
   * kind name
   */
  private String kind;
  
  /**
   * entity total
   */
  private long count;
  
  /**
   * entity total bytes
   */
  private long bytes;
  
  /**
   * timestamp
   */
  private Date timestamp;
  
  /**
   * init
   */
  public EntityStat() {
  }
  
  public void setData(String kind, long count, long bytes, Date timestamp) {
    this.kind = kind;
    this.count = count;
    this.bytes = bytes;
    this.timestamp = timestamp;
  }
  
  public String getKind() {
    return kind;
  }
  
  public long getCount() {
    return count;
  }
  
  public long getBytes() {
    return bytes;
  }
  
  public Date getTimestamp() {
    return timestamp;
  }

  public static EntityStat newInstance(Entity e) {
    String kind = e.getKey().getName();
    Long bytes = (Long) e.getProperty("bytes");
    Long count = (Long) e.getProperty("count");
    Date timestamp = (Date) e.getProperty("timestamp");
    EntityStat es = new EntityStat();
    es.setData(kind, count, bytes, timestamp);
    return es;
  }

}
