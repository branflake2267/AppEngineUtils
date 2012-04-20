package org.gonevertical.appengineutils;

import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.Blobber;
import org.gonevertical.appengineutils.util.KindUtils;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

public class Backup implements DeferredTask {
  
  private static final Logger log = Logger.getLogger(Backup.class.getName());

  private Date runDate;

  public Backup() {
  }
  
  @Override
  public void run() {
    runDate = new Date();
    
    log.info("Backup task running. runDate=" + runDate.toGMTString());
    
    loopKinds();
  }

  private void loopKinds() {
    List<String> kinds = KindUtils.getKinds(false);
    
    for (String kind : kinds) {
      processKind(kind);
    }
  }

  private void processKind(String kind) {
    WriteKindToBlob task = new  WriteKindToBlob(runDate, kind);
    
    TaskOptions taskOptions = TaskOptions.Builder.withPayload(task);
    Queue queue = QueueFactory.getDefaultQueue();
    queue.add(taskOptions);
  }
}
