/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
/*global Ext, NX*/

/**
 * @since 3.17
 */
Ext.define('NX.azureblobstore.app.PluginStrings', {
  '@aggregate_priority': 90,

  singleton: true,
  requires: [
    'NX.I18n'
  ],

  keys: {
    AzureBlobstore_AccountName_FieldLabel: 'Account Name',
    AzureBlobstore_AccountName_HelpText: 'Account name found under Access keys for the storage account.\n',

    AzureBlobstore_AccountKey_FieldLabel: 'Account Key',
    AzureBlobstore_AccountKey_HelpText: 'Account key found under Access keys for the storage account.',

    AzureBlobstore_ContainerName_FieldLabel: 'Container Name',
    AzureBlobstore_ContainerName_HelpText: 'The name of an existing container to be used for storage.',

    AzureBlobstore_ClientType_FieldLabel: 'Client Type',
    AzureBlobstore_ClientType_HelpText: 'Type of client used for connections.'
  }

}, function(obj) {
  NX.I18n.register(obj);
});
