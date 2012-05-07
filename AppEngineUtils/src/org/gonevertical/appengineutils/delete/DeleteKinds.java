package org.gonevertical.appengineutils.delete;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.AppEngineRemoteUtils;
import org.gonevertical.appengineutils.util.KindUtils;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

public class DeleteKinds implements DeferredTask {

  private static final Logger log = Logger.getLogger(DeleteKinds.class.getName());

  private List<String> excludeList;

  public DeleteKinds() {
  }

  @Override
  public void run() {
    log.info("Copy task running");

    loopKinds();
  }

  private void loopKinds() {
    List<String> kinds = KindUtils.getKinds(false);

    for (String kind : kinds) {
      processKind(kind);
    }
  }

  private boolean excludeKind(String kind) {
    boolean exclude = false;
    if ((excludeList != null && excludeList.isEmpty() == false) && excludeList.contains(kind) == true) {
      exclude = true;
    }
    return exclude;
  }

  private void processKind(String kind) {
    if (excludeKind(kind) == true) {
      return;
    }

    DeleteKind task = new  DeleteKind(kind);

    TaskOptions taskOptions = TaskOptions.Builder.withPayload(task);
    Queue queue = QueueFactory.getDefaultQueue();
    queue.add(taskOptions);
  }

  public void setExcludeKinds(List<String> excludeList) {
    this.excludeList = excludeList;
  }
  
}
