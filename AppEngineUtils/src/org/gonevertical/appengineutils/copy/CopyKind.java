package org.gonevertical.appengineutils.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.AppEngineRemoteUtils;

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
  private boolean hasData = true;
  private String kind;
  private Cursor startCursor;
  private Date runDate;

  private String appIdFull;
  private String remoteUserName;
  private String remotePassword;

  private AppEngineRemoteUtils remoteUtils;

  private String appKey;

  public CopyKind(String remoteUserName, String remotePassword, String appIdFull, Date runDate, String kind) {
    this.appIdFull = appIdFull;
    this.remoteUserName = remoteUserName;
    this.remotePassword = remotePassword; 
    this.runDate = runDate;
    this.kind = kind;
  }

  private void loginAppEngine() throws IOException {
    remoteUtils = AppEngineRemoteUtils.newInstance(remoteUserName, remotePassword, appIdFull);
  }

  @Override
  public void run() {
    appKey = getApp();

    try {
      loginAppEngine();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    do {
      try {
        query();
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
    } while (hasData);
  }

  private String getApp() {
    Entity entity = new Entity("TestEntity", 1);
    
    DatastoreService localDataStore = DatastoreServiceFactory.getDatastoreService();
    localDataStore.put(entity);
    
    EntityProto proto = EntityTranslator.convertToPb(entity);
    
    String app = proto.getKey().getApp();
    
    return app;
  }

  private void query() throws IOException {
    RemoteApiInstaller datastoreAccess = null;
    try {
      datastoreAccess = remoteUtils.open();
    } catch (IOException e1) {
      e1.printStackTrace();
      return;
    }

    DatastoreService remoteDataStore = DatastoreServiceFactory.getDatastoreService();
    Query q = new Query(kind);
    PreparedQuery pq = remoteDataStore.prepare(q);
    FetchOptions options = FetchOptions.Builder.withLimit(limit);

    if (startCursor != null) {
      options.startCursor(startCursor);
    }

    QueryResultList<Entity> results = pq.asQueryResultList(options);
    if (results == null || results.isEmpty()) {
      hasData = false;
      return;
    }

    List<Entity> entities = new ArrayList<Entity>();
    for (Entity entity : results) {
      entities.add(entity);
    }
    startCursor = results.getCursor();

    remoteUtils.close(datastoreAccess);

    writeList(entities);
  }

  /**
   * This will put remote Entities into local dev datastore local_db.bin
   */
  private void writeList(List<Entity> entities) {
    DatastoreService localDataStore = DatastoreServiceFactory.getDatastoreService();

    for (Entity entity : entities) {
      putEntity(localDataStore, entity);
    }
  }

  private void putEntity(DatastoreService localDataStore, Entity remoteEntity) {
    EntityProto proto = EntityTranslator.convertToPb(remoteEntity);
    
    // This is key to copying the entity!
    Reference x = proto.getKey();
    x.setApp(appKey);
    proto.setKey(x);
    
    Entity localEntity = EntityTranslator.createFromPb(proto);
    
    Transaction txn = localDataStore.beginTransaction();
    try {
      localDataStore.put(txn, localEntity);
      txn.commit();
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    log.info("putEntity() localEntity=" + localEntity);
    System.out.println("putEntity() localEntity=" + localEntity);
  }

}
