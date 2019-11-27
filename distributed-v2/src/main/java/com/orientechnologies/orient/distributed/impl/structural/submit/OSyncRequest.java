package com.orientechnologies.orient.distributed.impl.structural.submit;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.raft.OLeaderContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.SYNC_SUBMIT_REQUEST;

public class OSyncRequest implements OStructuralSubmitRequest {
  private Optional<OLogId> logId;

  public OSyncRequest(Optional<OLogId> logId) {
    this.logId = logId;
  }

  public OSyncRequest() {

  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    if (logId.isPresent()) {
      output.writeBoolean(true);
      OLogId.serialize(logId.get(), output);
    } else {
      output.writeBoolean(false);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    boolean isPresent = input.readBoolean();
    if (isPresent) {
      logId = Optional.ofNullable(OLogId.deserialize(input));
    } else {
      logId = Optional.empty();
    }
  }

  @Override
  public int getRequestType() {
    return SYNC_SUBMIT_REQUEST;
  }

  @Override
  public void begin(Optional<ONodeIdentity> requester, OSessionOperationId id, OLeaderContext context) {
    if (this.logId.isPresent()) {
      context.tryResend(requester.get(), this.logId.get());
    } else {
      context.sendFullConfiguration(requester.get());
    }
  }
}
