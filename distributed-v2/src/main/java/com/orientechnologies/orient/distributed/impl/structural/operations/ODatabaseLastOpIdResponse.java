package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LAST_OPLOG_ID_RESPONSE;

public class ODatabaseLastOpIdResponse implements OOperation {
  private String           database;
  private int              term;
  private Optional<OLogId> id;

  public ODatabaseLastOpIdResponse(String database, int term, Optional<OLogId> id) {
    this.database = database;
    this.term = term;
    this.id = id;
  }

  public ODatabaseLastOpIdResponse() {

  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    context.getElections().received(sender, database,term,id);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.database = input.readUTF();
    this.term = input.readInt();
    if (input.readBoolean()) {
      this.id = Optional.of(OLogId.deserialize(input));
    } else {
      this.id = Optional.empty();
    }
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(this.database);
    output.writeInt(this.term);
    if (id.isPresent()) {
      output.writeBoolean(true);
      OLogId.serialize(id.get(), output);
    } else {
      output.writeBoolean(false);
    }
  }

  @Override
  public int getOperationId() {
    return DATABASE_LAST_OPLOG_ID_RESPONSE;
  }
}
