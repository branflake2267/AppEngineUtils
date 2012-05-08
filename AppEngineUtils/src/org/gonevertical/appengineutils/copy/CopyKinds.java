package org.gonevertical.appengineutils.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.util.AppEngineRemoteUtils;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

public class CopyKinds implements DeferredTask {

  private static final Logger log = Logger.getLogger(CopyKinds.class.getName());

  private Date runDate;
  private List<String> excludeList;
  private String appIdFull;
  private String remoteUserName;
  private String remotePassword;

  private AppEngineRemoteUtils remoteUtils;

  public CopyKinds(String remoteUserName, String remotePassword, String appIdFull) {
    this.appIdFull = appIdFull;
    this.remoteUserName = remoteUserName;
    this.remotePassword = remotePassword; 
  }

  private void loginAppEngine() throws IOException {
    remoteUtils = AppEngineRemoteUtils.newInstance(remoteUserName, remotePassword, appIdFull);
  }

  @Override
  public void run() {
    runDate = new Date();

    log.info("Copy task running. runDate=" + runDate.toGMTString());

    try {
      loginAppEngine();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    loopKinds();
  }

  private void loopKinds() {
    List<String> kinds;
    try {
      kinds = remoteUtils.getKinds(false);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }


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

    CopyKind task = new CopyKind(remoteUserName, remotePassword, appIdFull, kind);
    TaskOptions taskOptions = TaskOptions.Builder.withPayload(task);
    Queue queue = QueueFactory.getDefaultQueue();
    queue.add(taskOptions);
    
    task.run();
  }

  public void setExcludeKinds(List<String> excludeList) {
    this.excludeList = excludeList;
  }
}
