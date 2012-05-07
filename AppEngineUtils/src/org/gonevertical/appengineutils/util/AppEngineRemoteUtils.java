package org.gonevertical.appengineutils.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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

  private RemoteApiOptions options;

  /**
   * use newInstance();
   */
  private AppEngineRemoteUtils() {
  }

  public RemoteApiInstaller open() throws IOException {
    RemoteApiInstaller datastoreAccess = new RemoteApiInstaller();
    datastoreAccess.install(options);
    return datastoreAccess;
  }

  public void close(RemoteApiInstaller datastoreAccess) {
    datastoreAccess.uninstall();
  }

  protected RemoteApiOptions getRemoteApiOptions() {
    return options;
  }

  /**
   * sandbox
   */
  public  ArrayList<String> test() {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = open();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (datastoreAccess == null) {
      return null;
    }
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
      close(datastoreAccess);
    }
    return r;
  }

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

  public void setup(String username, String password, String appid) throws IOException {
    // Authenticating with username and password is slow, so we'll do it
    // once during construction and then store the credentials for reuse.
    this.options = new RemoteApiOptions()
    .server(appid, 443)
    .credentials(username, password);
    RemoteApiInstaller installer = new RemoteApiInstaller();
    installer.install(options);
    try {
      // Update the options with reusable credentials so we can skip
      // authentication on subsequent calls.
      options.reuseCredentials(username, installer.serializeCredentials());
    } finally {
      installer.uninstall();
    }

    log.info("AppEngineRemoteUtils.setup(): app engine instance ready...");
  }

  public void putInRemoteDatastore(Entity entity) throws IOException {
    RemoteApiInstaller installer = new RemoteApiInstaller();
    installer.install(options);
    try {
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      System.out.println("Key of new entity is " + ds.put(entity));
    } finally {
      installer.uninstall();
    }
  }

  /**
   * get entity kinds
   * 
   * TODO maybe it should be __Stat_Ns_Kind__
   * 
   * @param withSystemKinds include system kinds like __Stat_Total__
   * @return
   */
  public ArrayList<String> getKinds(boolean withSystemKinds) {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = open();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (datastoreAccess == null) {
      return null;
    }
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
      close(datastoreAccess);
    }
    return r;
  }

  /**
   * is Entity kind a system kind?
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
   */
  public EntityStat getStatGlobal() {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = open();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (datastoreAccess == null) {
      return null;
    }
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
      close(datastoreAccess);
    }
    return es;
  }

  /**
   * get all the entity stats
   * @return
   */
  public ArrayList<EntityStat> getStats() {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = open();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (datastoreAccess == null) {
      return null;
    }
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
      close(datastoreAccess);
    }
    return r;
  }

  public EntityStat getStat(String kind) {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = open();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (datastoreAccess == null) {
      return null;
    }
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
      close(datastoreAccess);
    }
    return r;
  }

  public DatastoreService getDataStore() {
    return DatastoreServiceFactory.getDatastoreService();
  }

}
