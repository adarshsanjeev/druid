/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.dart.controller;

import com.google.common.base.Preconditions;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holder for {@link Controller}, stored in {@link DartControllerRegistry}.
 */
public class ControllerHolder
{
  public enum State
  {
    /**
     * Query has been accepted, but not yet {@link Controller#run(QueryListener)}.
     */
    ACCEPTED,

    /**
     * Query has had {@link Controller#run(QueryListener)} called.
     */
    RUNNING,

    /**
     * Query has been canceled.
     */
    CANCELED
  }

  private final Controller controller;
  private final String sqlQueryId;
  private final String sql;
  private final AuthenticationResult authenticationResult;
  private final DateTime startTime;
  private final AtomicReference<State> state = new AtomicReference<>(State.ACCEPTED);

  public ControllerHolder(
      final Controller controller,
      final String sqlQueryId,
      final String sql,
      final AuthenticationResult authenticationResult,
      final DateTime startTime
  )
  {
    this.controller = Preconditions.checkNotNull(controller, "controller");
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.sql = sql;
    this.authenticationResult = authenticationResult;
    this.startTime = Preconditions.checkNotNull(startTime, "startTime");
  }

  public Controller getController()
  {
    return controller;
  }

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  public String getSql()
  {
    return sql;
  }

  public String getControllerHost()
  {
    return getControllerContext().selfNode().getHostAndPortToUse();
  }

  private ControllerContext getControllerContext()
  {
    return controller.getControllerContext();
  }

  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  public DateTime getStartTime()
  {
    return startTime;
  }

  public State getState()
  {
    return state.get();
  }

  /**
   * Call when a worker has gone offline. Closes its client and sends a {@link Controller#workerError}
   * to the controller.
   */
  public void workerOffline(final WorkerId workerId)
  {
    final String workerIdString = workerId.toString();

    ControllerContext controllerContext = getControllerContext();
    if (controllerContext instanceof DartControllerContext) {
      DartControllerContext dartControllerContext = (DartControllerContext) controllerContext;
      // For DartControllerContext, newWorkerClient() returns the same instance every time.
      // This will always be DartControllerContext in production; the instanceof check is here because certain
      // tests use a different context class.
      dartControllerContext.newWorkerClient().closeClient(workerId.getHostAndPort());
    }

    if (controller.hasWorker(workerIdString)) {
      controller.workerError(
          MSQErrorReport.fromFault(
              workerIdString,
              workerId.getHostAndPort(),
              null,
              new WorkerFailedFault(workerIdString, "Worker went offline")
          )
      );
    }
  }

  /**
   * Places this holder into {@link State#CANCELED}. Calls {@link Controller#stop(CancellationReason)} if it was
   * previously in state {@link State#RUNNING}.
   */
  public void cancel(CancellationReason reason)
  {
    if (state.getAndSet(State.CANCELED) == State.RUNNING) {
      controller.stop(reason);
    }
  }

  /**
   * Calls {@link Controller#run(QueryListener)}, and returns true, if this holder was previously in state
   * {@link State#ACCEPTED}. Otherwise returns false.
   *
   * @return whether {@link Controller#run(QueryListener)} was called.
   */
  public boolean run(final QueryListener listener) throws Exception
  {
    if (state.compareAndSet(State.ACCEPTED, State.RUNNING)) {
      controller.run(listener);
      return true;
    } else {
      return false;
    }
  }
}
