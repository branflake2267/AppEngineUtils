package org.gonevertical.appengineutils.util;

import java.util.ArrayList;
import java.util.List;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;

public class KindUtils {

  public static List<String> getKinds(boolean withSystemKinds) {
    
    ArrayList<String> r = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

    Query q = new Query("__kind__");
    Iterable<Entity> itr = ds.prepare(q).asIterable();
    for (Entity e : itr) {
      if (r == null) {
        r = new ArrayList<String>();
      }
      if (withSystemKinds == true) {
        r.add(e.getKey().getName());  
      } else {
        if (isSystemKind(e.getKey().getName()) == false) {
          r.add(e.getKey().getName());
        }
      }
    }

    return r;
  }
  
  /**
   * is Entity kind a system kind?
   * 
   * @param kind
   * @return
   */
  private static boolean isSystemKind(String kind) {
    if (kind == null) { 
      return false;
    }
    boolean isSystemKind = false;
    if (kind.matches("__.*?__") == true) {
      isSystemKind = true;
    }
    return isSystemKind;
  }
}
