package com.orientechnologies.orient.distributed;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.util.Optional;

public class OElectionReply {
  private final ONodeIdentity    sender;
  private final Optional<OLogId> id;

  public OElectionReply(ONodeIdentity sender, Optional<OLogId> id) {
    this.sender = sender;
    this.id = id;
  }
}
