package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.ODistributedNetwork;
import com.orientechnologies.orient.distributed.impl.ODistributedNetworkManager;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedCoordinator implements AutoCloseable {

  private final ExecutorService                        requestExecutor;
  private final OOperationLog                          operationLog;
  private final ConcurrentMap<OLogId, ORequestContext> contexts = new ConcurrentHashMap<>();
  private final Set<ONodeIdentity>                     members  = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Timer                                  timer;
  private final ODistributedLockManager                lockManager;
  private final OClusterPositionAllocator              allocator;
  private final ODistributedNetwork                    network;
  private final String                                 database;

  public ODistributedCoordinator(ExecutorService requestExecutor, OOperationLog operationLog, ODistributedLockManager lockManager,
      OClusterPositionAllocator allocator, ODistributedNetwork network, String database) {
    this.requestExecutor = requestExecutor;
    this.operationLog = operationLog;
    this.timer = new Timer(true);
    this.lockManager = lockManager;
    this.allocator = allocator;
    this.network = network;
    this.database = database;
  }

  public void submit(ONodeIdentity member, OSessionOperationId operationId, OSubmitRequest request) {
    requestExecutor.execute(() -> {
      request.begin(member, operationId, this);
    });
  }

  public void reply(ONodeIdentity member, OSessionOperationId operationId, OSubmitResponse response) {
    network.replay(member, database, operationId, response);
  }

  public void receive(ONodeIdentity member, OLogId relativeRequest, ONodeResponse response) {
    requestExecutor.execute(() -> {
      contexts.get(relativeRequest).receive(member, response);
    });
  }

  public OLogId log(ONodeRequest request) {
    return operationLog.log(request);
  }

  public ORequestContext sendOperation(OSubmitRequest submitRequest, ONodeRequest nodeRequest, OResponseHandler handler) {
    OLogId id = log(nodeRequest);
    Collection<ONodeIdentity> values = new ArrayList<>(members);
    ORequestContext context = new ORequestContext(this, submitRequest, nodeRequest, values, handler, id);
    contexts.put(id, context);
    network.sendRequest(values, database, id, nodeRequest);
    //Get the timeout from the configuration
    timer.schedule(context.getTimerTask(), 1000, 1000);
    return context;
  }

  public void join(ONodeIdentity nodeIdentity) {
    members.add(nodeIdentity);
  }

  @Override
  public void close() {
    timer.cancel();
    requestExecutor.shutdown();
    try {
      requestExecutor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }

  public void executeOperation(Runnable runnable) {
    requestExecutor.execute(runnable);
  }

  public void finish(OLogId requestId) {
    contexts.remove(requestId);
  }

  protected ConcurrentMap<OLogId, ORequestContext> getContexts() {
    return contexts;
  }

  public ODistributedLockManager getLockManager() {
    return lockManager;
  }

  public OClusterPositionAllocator getAllocator() {
    return allocator;
  }

  public void leave(ONodeIdentity nodeIdentity) {
    members.remove(nodeIdentity);
  }
}
