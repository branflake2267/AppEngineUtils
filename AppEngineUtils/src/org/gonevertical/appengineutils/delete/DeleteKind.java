package org.gonevertical.appengineutils.delete;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.AppEngineRemoteUtils;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.taskqueue.DeferredTask;

public class DeleteKind implements DeferredTask {

  private static final Logger log = Logger.getLogger(DeleteKind.class.getName());

  private int limit = 100;
  private boolean hasData = true;
  private String kind;
  private Cursor startCursor;
  private DatastoreService localDataStore; 

  public DeleteKind(String kind) {
    this.kind = kind;
  }
  
  @Override
  public void run() {
    localDataStore = DatastoreServiceFactory.getDatastoreService();

    do {
      query();
    } while (hasData);
  }
 
  private void query() {
    Query q = new Query(kind);
    
    PreparedQuery pq = localDataStore.prepare(q);
    FetchOptions options = FetchOptions.Builder.withLimit(limit);

    if (startCursor != null) {
      options.startCursor(startCursor);
    }

    QueryResultList<Entity> results = pq.asQueryResultList(options);
    if (results == null || results.isEmpty()) {
      hasData = false;
      return;
    }

    for (Entity entity : results) {
      delete(entity);
    }

    startCursor = results.getCursor();
  }

  private void delete(Entity entity) {
    localDataStore.delete(entity.getKey());
  }

}
