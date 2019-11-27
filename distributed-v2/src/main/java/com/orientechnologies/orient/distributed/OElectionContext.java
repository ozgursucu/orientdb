package com.orientechnologies.orient.distributed;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OElectionContext {

  private Map<String, OElection> elections = new HashMap<>();

  public synchronized int startElection(String name, int quorum) {
    int term = 1;
    OElection election = new OElection(term, quorum);
    elections.put(name, election);
    return term;
  }

  public synchronized void received(ONodeIdentity sender, String database, int term, Optional<OLogId> id) {
    OElection election = elections.get(database);
    election.addReply(sender, term, id);
  }
}
