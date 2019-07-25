/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2019-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.azure.internal.db;

import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.azure.internal.DeletedBlobIndex;
import org.sonatype.nexus.common.app.ManagedLifecycle;
import org.sonatype.nexus.common.app.ManagedLifecycle.Phase;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;
import org.sonatype.nexus.orient.DatabaseInstance;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.StreamSupport.stream;
import static org.sonatype.nexus.orient.DatabaseInstanceNames.COMPONENT;
import static org.sonatype.nexus.orient.transaction.OrientTransactional.inTxRetry;

/**
 * OrientDB implementation for the {@link DeletedBlobIndex}. This will leverage the NXRM database to track deleted
 * blob references (soft-deletes)
 */
@Named(OrientDeletedBlobIndex.NAME)
@Singleton
@ManagedLifecycle(phase = Phase.SCHEMAS)
public class OrientDeletedBlobIndex
    extends StateGuardLifecycleSupport
    implements DeletedBlobIndex
{
  public static final String NAME = "orientDeletedBlobIndex";

  private final OrientDeletedBlobEntityAdapter adapter;

  private Provider<DatabaseInstance> database;

  @Inject
  public OrientDeletedBlobIndex(final OrientDeletedBlobEntityAdapter adapter,
                                @Named(COMPONENT) final Provider<DatabaseInstance> database)
  {
    this.adapter = checkNotNull(adapter);
    this.database = checkNotNull(database);
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    try (ODatabaseDocumentTx db = database.get().connect()) {
      adapter.register(db);
    }
  }

  @Override
  public void add(final BlobId blobId) {
    BlobIdEntity blobIdEntity = adapter.newEntity();
    blobIdEntity.setBlobId(blobId.asUniqueString());

    inTxRetry(database).run(db -> adapter.addEntity(db, blobIdEntity));
  }

  @Override
  public void remove(final BlobId blobId) {
    inTxRetry(database).run((db) -> adapter.delete(db, blobId));
  }

  @Override
  public Stream<BlobId> browse() {
    return inTxRetry(database)
        .call(db -> stream(adapter.browse(db).spliterator(), false).map(entity -> new BlobId(entity.getBlobId())));
  }
}
