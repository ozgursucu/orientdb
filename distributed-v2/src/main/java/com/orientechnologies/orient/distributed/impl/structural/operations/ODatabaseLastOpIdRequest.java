package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LAST_OPLOG_ID_RESPONSE;

public class ODatabaseLastOpIdRequest implements OOperation {
  private String database;
  private int    term;

  public ODatabaseLastOpIdRequest() {

  }
  
  public ODatabaseLastOpIdRequest(String database, int term) {
    this.database = database;
    this.term = term;
  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    OLogId id = context.getDistributedContext(database).getOpLog().lastPersistentLog();
    context.getNetworkManager().send(sender, new ODatabaseLastOpIdResponse(database, term, Optional.ofNullable(id)));
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.database = input.readUTF();
    this.term = input.readInt();
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
    output.writeInt(term);
  }

  @Override
  public int getOperationId() {
    return DATABASE_LAST_OPLOG_ID_RESPONSE;
  }
}
