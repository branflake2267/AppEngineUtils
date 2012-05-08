package org.gonevertical.appengineutils.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.AppEngineRemoteUtils;
import org.gonevertical.appengineutils.util.EntityStat;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;

public class CopyKind implements DeferredTask {

  private static final Logger log = Logger.getLogger(CopyKind.class.getName());

  private int limit = 100;
  private String kind;
  private String appIdFull;
  private String remoteUserName;
  private String remotePassword;
  private String appKey;
  private AppEngineRemoteUtils remoteUtils;
  private DatastoreService localDataStore;

  public CopyKind(String remoteUserName, String remotePassword, String appIdFull, String kind) {
    this.appIdFull = appIdFull;
    this.remoteUserName = remoteUserName;
    this.remotePassword = remotePassword; 
    this.kind = kind;
  }
  
  private void loginAppEngine() throws IOException {
    remoteUtils = AppEngineRemoteUtils.newInstance(remoteUserName, remotePassword, appIdFull);
  }

  @Override
  public void run() {
    localDataStore = DatastoreServiceFactory.getDatastoreService();
    
    appKey = getAppKey();
    
    try {
      loginAppEngine();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    
    EntityStat stat = null;
    try {
      stat = remoteUtils.getStat(kind);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    
    if (stat.getCount() == 0) {
      return;
    }

    int offset = 0;
    do {
      try {
        query(offset, limit);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
      offset = offset + limit;
    } while (offset <= stat.getCount());
  }

  /**
   * create a entity so we can use EntityTranslater proto to get the app name from the key
   * @return
   */
  private String getAppKey() {
    Entity entity = new Entity("TestEntity", 1);
    localDataStore.put(entity);
    
    EntityProto proto = EntityTranslator.convertToPb(entity);
    String app = proto.getKey().getApp();
    return app;
  }

  private void query(int offset, int limit) throws IOException {
    System.out.println("offset=" + offset + " limit=" + limit + " kind=" + kind);
    
    RemoteApiInstaller installer = null;
    try {
      installer = remoteUtils.open();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    DatastoreService remoteDataStore = DatastoreServiceFactory.getDatastoreService();
    FetchOptions options = FetchOptions.Builder.withOffset(offset).limit(limit);
    List<Entity> entities = remoteDataStore.prepare(new Query(kind)).asList(options);

    remoteUtils.close(installer);

    // Lets just make sure the remote was closed properly.
    // Remote won't close if its iterable query, b/c of the async
    if (testIsLocal() == false) {
      return;
    }
    
    putEntities(entities);
  }

  /**
   * This will put remote Entities into local dev datastore local_db.bin
   */
  private void putEntities(List<Entity> entities) {
    for (Entity entity : entities) {
      putEntity(entity);
    }
  }

  private void putEntity(Entity remoteEntity) {
    EntityProto remoteProto = EntityTranslator.convertToPb(remoteEntity);
    
    // This is key to copying the entity!
    Reference refKey = remoteProto.getKey();
    refKey.setApp(appKey);
    remoteProto.setKey(refKey);
    
    Entity localEntity = EntityTranslator.createFromPb(remoteProto);
    
    Transaction txn = localDataStore.beginTransaction();
    try {
      localDataStore.put(txn, localEntity);
      txn.commit();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    //log.info("putEntity() localEntity=" + localEntity);
    System.out.println("putEntity() localEntity=" + localEntity);
  }

  /**
   * Be sure the localDataStore is local and not remote
   * This will happen if the remote is not closed/uninstalled
   * 
   * @return
   */
  private boolean testIsLocal() {
    Entity entity = new Entity("TestEntity", 1);
    
    try {
      entity = localDataStore.get(entity.getKey());
    } catch (EntityNotFoundException e) {
      e.printStackTrace();
      return false;
    }
    
    EntityProto proto = EntityTranslator.convertToPb(entity);
    String appKey = proto.getKey().getApp();
    
    boolean islocal = false;
    if (this.appKey.equals(appKey) == true) {
      islocal = true;
    }
    
    return islocal;
  }

}
