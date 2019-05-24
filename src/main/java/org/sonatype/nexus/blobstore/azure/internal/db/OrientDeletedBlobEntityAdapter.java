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
  private static final String DB_CLASS = new OClassNameBuilder()
      .type("deleted_blob_index")
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
