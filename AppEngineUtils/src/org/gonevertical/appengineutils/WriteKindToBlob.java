package org.gonevertical.appengineutils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.Blobber;
import org.gonevertical.appengineutils.util.FileNamingUtil;
import org.gonevertical.appengineutils.util.GsonUtils;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.LockException;
import com.google.appengine.api.taskqueue.DeferredTask;

public class WriteKindToBlob implements DeferredTask {
  
  private static final Logger log = Logger.getLogger(WriteKindToBlob.class.getName());
  
  private int limit = 100;
  private boolean hasData = true;
  private String kind;
  private Cursor startCursor;
  private DatastoreService datastore;
  private FileWriteChannel writeChannel;
  private Date runDate;

  private Blobber blobber;
  
  public WriteKindToBlob(Date runDate, String kind) {
    this.runDate = runDate;
    this.kind = kind;
  }
  
  @Override
  public void run() {
    String filename = FileNamingUtil.getFileName(kind, runDate);
    
    log.info("WriteKindToBlob task filename=" + filename);
    
    blobber = new Blobber();
    
    AppEngineFile file = null;
    try {
      file = blobber.create(filename);
    } catch (IOException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    }
    
    try {
      writeChannel = blobber.open(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    } catch (FinalizationException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    } catch (LockException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    } catch (IOException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    }
    
    datastore = DatastoreServiceFactory.getDatastoreService();

    do {
      query();
    } while (hasData);
    
    try {
      blobber.close(writeChannel);
    } catch (IllegalStateException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    } catch (IOException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    }
  }
  
  private void query() {
    Query q = new Query(kind);
    PreparedQuery pq = datastore.prepare(q);
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
      write(entity);
    }
    
    startCursor = results.getCursor();
  }

  private void write(Entity entity) {
    String serialized = GsonUtils.convertObjectToString(entity);
    
    log.info("serialized=" + serialized);
    
    try {
      blobber.write(writeChannel, serialized);
    } catch (IOException e) {
      e.printStackTrace();
      log.log(Level.SEVERE, "", e);
    }
  }
  
}
