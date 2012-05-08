package org.gonevertical.appengineutils.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.BaseDatastoreService;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;

public class AppEngineRemoteUtils {

  private static final Logger log = Logger.getLogger(AppEngineRemoteUtils.class.getName());

  /**
   * set Login options
   * 
   * http://code.google.com/appengine/docs/java/tools/remoteapi.html#Configuring_Remote_API_on_the_Client
   * @return
   */
  public static AppEngineRemoteUtils newInstance(String username, String password, String appid) throws IOException {
    AppEngineRemoteUtils utils = new AppEngineRemoteUtils();
    utils.setup(username, password, appid);
    return utils;
  }

  private RemoteApiOptions options;

  /**
   * use newInstance();
   */
  private AppEngineRemoteUtils() {
  }

  public void setup(String username, String password, String appid) throws IOException {
    this.options = new RemoteApiOptions()
      .server(appid, 443)
      .credentials(username, password);
    RemoteApiInstaller installer = new RemoteApiInstaller();
    installer.install(options);
    try {
      options.reuseCredentials(username, installer.serializeCredentials());
    } finally {
      installer.uninstall();
    }

    log.info("AppEngineRemoteUtils.setup(): app engine instance ready...");
  }

  public RemoteApiInstaller open() throws IOException {
    RemoteApiInstaller installer = new RemoteApiInstaller();
    try {
      installer.install(options);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return installer;
  }

  public void close(RemoteApiInstaller installer) {
    installer.uninstall();    
  }

  protected RemoteApiOptions getRemoteApiOptions() {
    return options;
  }

  public void put(Entity entity) throws IOException {
    RemoteApiInstaller installer = open();
    try {
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      System.out.println("Key of new entity is " + ds.put(entity));
    } finally {
      close(installer);
    }
  }

  /**
   * get entity kinds
   * 
   * TODO maybe it should be __Stat_Ns_Kind__
   * 
   * @param withSystemKinds include system kinds like __Stat_Total__
   * @return
   * @throws IOException 
   */
  public List<String> getKinds(boolean withSystemKinds) throws IOException {
    RemoteApiInstaller installer = open();
    ArrayList<String> r = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
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
    } catch (Exception e) {
      e.printStackTrace();
      //log.error("getKinds error. withSystemKinds=" + withSystemKinds, e);
    } finally {
      close(installer);
    }
    return r;
  }

  /**
   * Is Entity kind a system kind?
   * 
   * @param kind
   * @return
   */
  private boolean isSystemKind(String kind) {
    if (kind == null) { 
      return false;
    }
    boolean isSystemKind = false;
    if (kind.matches("__.*?__") == true) {
      isSystemKind = true;
    } else if (kind.matches("_AE_.*?") == true) {
      isSystemKind = true;
    }
    return isSystemKind;
  }

  /**
   * Get All entities total
   * 
   * @throws IOException 
   */
  public EntityStat getStatGlobal() throws IOException {
    RemoteApiInstaller installer = open();
    EntityStat es = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
      Entity e = ds.prepare(new Query("__Stat_Total__")).asSingleEntity();

      String kind = e.getKey().getName();
      Long bytes = (Long) e.getProperty("bytes");
      Long count = (Long) e.getProperty("count");
      Date timestamp = (Date) e.getProperty("timestamp");

      if (es == null) {
        es = new EntityStat();
      }
      es.setData(kind, count, bytes, timestamp);

    } catch (Exception e) {
      e.printStackTrace();
      //log.error("getStatGlobal error", e);
    } finally {
      close(installer);
    }
    return es;
  }

  /**
   * Get all the entity stats
   * 
   * @return
   * @throws IOException 
   */
  public List<EntityStat> getStats() throws IOException {
    RemoteApiInstaller installer = open();
    ArrayList<EntityStat> r = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
      Query q = new Query("__Stat_Kind__");
      Iterable<Entity> itr = ds.prepare(q).asIterable();
      for (Entity e : itr) {
        if (r == null) {
          r = new ArrayList<EntityStat>();
        }
        EntityStat es = EntityStat.newInstance(e);
        r.add(es);
      }
    } catch (Exception e) {
      e.printStackTrace();
      //log.error("getStats error", e);
    } finally {
      close(installer);
    }
    return r;
  }

  public EntityStat getStat(String kind) throws IOException {
    RemoteApiInstaller installer = open();
    EntityStat r = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
      Query q = new Query("__Stat_Kind__");
      q.addFilter("kind_name", FilterOperator.EQUAL, kind);
      Iterable<Entity> itr = ds.prepare(q).asIterable();
      for (Entity e : itr) {
        r = EntityStat.newInstance(e);
      }
    } catch (Exception e) {
      e.printStackTrace();
      //log.error("getStat error for kind=" + kind, e);
    } finally {
      close(installer);
    }
    return r;
  }

  public List<String> test() throws IOException {
    RemoteApiInstaller installer = open();
    ArrayList<String> r = null;
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
      Query q = new Query("__Stat_Kind__");
      Iterable<Entity> itr = ds.prepare(q).asIterable();
      for (Entity e : itr) {
        if (r == null) {
          r = new ArrayList<String>();
        }
        log.info("e=" + e);
      }
    } catch (Exception e) {
      e.printStackTrace();
      //log.error("error", e); 
    } finally {
      close(installer);
    }
    return r;
  }
}
