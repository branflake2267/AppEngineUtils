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

  private int limit = 1000;
  private String kind;
  private String appIdFull;
  private String remoteUserName;
  private String remotePassword;
  private String appKey;
  private AppEngineRemoteUtils remoteUtils;

  private EntityStat remoteEntityStat;

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
    System.out.println("CopyKind.run() " + " kind=" + kind);
    
    appKey = getAppKey();
    
    try {
      loginAppEngine();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    
    try {
      remoteEntityStat = remoteUtils.getStat(kind);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    
    if (remoteEntityStat.getCount() == 0) {
      return;
    }

    for (int offset=0; offset <= remoteEntityStat.getCount(); offset += limit) {
      query(offset, limit);  
    }
    
    System.out.println("finished kind=" + kind);
  }

  /**
   * create a entity so we can use EntityTranslater proto to get the app name from the key
   * @return
   */
  private String getAppKey() {
    Entity entity = new Entity("TestEntity", 1);
    DatastoreService localDataStore = DatastoreServiceFactory.getDatastoreService();
    localDataStore.put(entity);
    
    EntityProto proto = EntityTranslator.convertToPb(entity);
    String app = proto.getKey().getApp();
    return app;
  }

  private void query(int offset, int limit) {
    System.out.println("offset=" + offset + " limit=" + limit + " kind=" + kind + " total-offset=" + (remoteEntityStat.getCount() - offset));
    
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

    if (entities == null || entities.isEmpty() == true) {
      return;
    }
    
    List<Entity> detatched = new ArrayList<Entity>();
    for (int i=0; i < entities.size(); i++) {
      detatched.add(entities.get(i));
    }
    
    remoteUtils.close(installer);
    
    DatastoreService localDataStore = DatastoreServiceFactory.getDatastoreService();

    // Lets just make sure the remote was closed properly.
    // Remote won't close if its iterable query, b/c of the async
    if (testIsLocal(localDataStore) == false) {
      return;
    }
    
    putEntities(localDataStore, detatched);
    
    System.out.println("finished putting entities in this group kind=" + kind);
  }

  /**
   * This will put remote Entities into local dev datastore local_db.bin
   * @param localDataStore 
   */
  private void putEntities(DatastoreService localDataStore, List<Entity> entities) {
    if (entities == null || entities.isEmpty()) {
      return;
    }
    
    for (Entity entity : entities) {
      putEntity(localDataStore, entity);
    }
  }

  private void putEntity(DatastoreService localDataStore, Entity remoteEntity) {
    EntityProto remoteProto = EntityTranslator.convertToPb(remoteEntity);
    
    // This is key to copying the entity!
    Reference refKey = remoteProto.getKey();
    refKey.setApp(appKey);
    remoteProto.setKey(refKey);
    
    Entity localEntity = EntityTranslator.createFromPb(remoteProto);
    
    Transaction txn = null;
    try {
      txn = localDataStore.beginTransaction();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    try {
      localDataStore.put(txn, localEntity);
      txn.commit();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    //log.info("putEntity() localEntity=" + localEntity);
    //System.out.println("putEntity() localEntity=" + localEntity + " kind=" + kind);
    System.out.println("putEntity() key=" + localEntity.getKey());
  }

  /**
   * Be sure the localDataStore is local and not remote
   * This will happen if the remote is not closed/uninstalled
   * @param localDataStore 
   * 
   * @return
   */
  private boolean testIsLocal(DatastoreService localDataStore) {
    //System.out.println("testIsLocal()..." + " kind=" + kind);
    
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
    
    //System.out.println("testIsLocal()... islocal=" + islocal + " kind=" + kind);
    
    return islocal;
  }

}
