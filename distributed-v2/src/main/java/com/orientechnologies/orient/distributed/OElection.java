package com.orientechnologies.orient.distributed;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OElection {
  private int                  term;
  private int                  quorum;
  private List<OElectionReply> replies = new ArrayList<>();

  public OElection(int quorum, int term) {
    this.quorum = quorum;
    this.term = term;
  }

  public void addReply(ONodeIdentity sender, int term, Optional<OLogId> id) {
    if (this.term == term) {
      replies.add(new OElectionReply(sender, id));
    }
  }

  private synchronized Optional<OElectionReply> checkElection() {
    if (replies.size() > quorum) {
      for (OElectionReply reply : replies) {

      }
    }
    return Optional.empty();
  }
}
