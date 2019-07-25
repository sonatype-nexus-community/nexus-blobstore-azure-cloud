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

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.orient.OClassNameBuilder;
import org.sonatype.nexus.orient.OIndexNameBuilder;
import org.sonatype.nexus.orient.entity.IterableEntityAdapter;
import org.sonatype.nexus.orient.entity.action.DeleteEntityByPropertyAction;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Orient entity operations for {@link BlobIdEntity}
 */
@Named
@Singleton
public class OrientDeletedBlobEntityAdapter
    extends IterableEntityAdapter<BlobIdEntity>
{
  static String DB_CLASE_TYPE = "deleted_blob_index";

  private static final String DB_CLASS = new OClassNameBuilder()
      .type(DB_CLASE_TYPE)
      .build();

  private static final String P_BLOB_ID = "blob_id";

  private static final String I_BLOB_ID = new OIndexNameBuilder()
      .type(DB_CLASS)
      .property(P_BLOB_ID)
      .build();

  private final DeleteEntityByPropertyAction delete = new DeleteEntityByPropertyAction(this, P_BLOB_ID);

  @Inject
  public OrientDeletedBlobEntityAdapter()
  {
    super(DB_CLASS);
  }

  @Override
  protected BlobIdEntity newEntity() {
    return new BlobIdEntity();
  }

  @Override
  protected void readFields(final ODocument document, final BlobIdEntity entity) {
    entity.setBlobId(document.field(P_BLOB_ID, OType.STRING));
  }

  @Override
  protected void writeFields(final ODocument document, final BlobIdEntity entity) {
    document.field(P_BLOB_ID, entity.getBlobId());
  }

  @Override
  protected void defineType(final OClass type) {
    type.createProperty(P_BLOB_ID, OType.STRING)
        .setMandatory(true)
        .setNotNull(true);

    // hash index should mean constant-time add and remove operations (O(1))
    type.createIndex(I_BLOB_ID, INDEX_TYPE.UNIQUE_HASH_INDEX, P_BLOB_ID);
  }

  public boolean delete(final ODatabaseDocumentTx db, final BlobId blobId) {
    return delete.execute(db, blobId.asUniqueString());
  }
}
