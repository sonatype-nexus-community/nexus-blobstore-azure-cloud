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
package org.sonatype.nexus.blobstore.azure.internal;

import java.util.List;

import javax.inject.Named;

import org.sonatype.goodies.i18n.I18N;
import org.sonatype.goodies.i18n.MessageBundle;
import org.sonatype.nexus.blobstore.BlobStoreDescriptor;
import org.sonatype.nexus.formfields.FormField;
import org.sonatype.nexus.formfields.PasswordFormField;
import org.sonatype.nexus.formfields.StringTextFormField;

import static java.util.Arrays.asList;

@Named(AzureBlobStore.TYPE)
public class AzureBlobStoreDescriptor
    implements BlobStoreDescriptor
{
  private interface Messages
      extends MessageBundle
  {
    @DefaultMessage("Azure Cloud Storage")
    String name();

    @DefaultMessage("Account Name")
    String accountNameLabel();

    @DefaultMessage("Account name found under Access keys for the storage account.")
    String accountNameHelp();

    @DefaultMessage("Account Key")
    String accountKeyLabel();

    @DefaultMessage("Account key found under Access keys for the storage account.")
    String accountKeyHelp();

    @DefaultMessage("Container Name")
    String containerNameLabel();

    @DefaultMessage("The name of an existing container to be used for storage.")
    String containerNameHelp();

  }

  private static final Messages messages = I18N.create(Messages.class);

  private final FormField accountName;

  private final FormField accountKey;

  private final FormField containerName;

  public AzureBlobStoreDescriptor() {
    this.accountName = new StringTextFormField(AzureBlobStore.ACCOUNT_NAME_KEY,
        messages.accountNameLabel(),
        messages.accountNameHelp(),
        true);
    this.accountKey = new PasswordFormField(AzureBlobStore.ACCOUNT_KEY_KEY,
        messages.accountKeyLabel(),
        messages.accountKeyHelp(),
        true);
    this.containerName = new StringTextFormField(AzureBlobStore.CONTAINER_NAME_KEY,
        messages.containerNameLabel(),
        messages.containerNameHelp(),
        true);
  }

  @Override
  public String getName() {
    return messages.name();
  }

  @Override
  public List<FormField> getFormFields() {
    return asList(accountName, accountKey, containerName);
  }
}
