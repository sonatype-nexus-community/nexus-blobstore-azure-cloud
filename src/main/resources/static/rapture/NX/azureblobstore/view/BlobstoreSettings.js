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
 * Azure Blobstore custom "Settings" panel.
 *
 */
Ext.define('NX.azureblobstore.view.BlobstoreSettings', {
  extend: 'NX.view.SettingsPanel',
  alias: 'widget.nx-blobstore-settings-azure',
  requires: [
    'Ext.data.Store',
    'NX.I18n'
  ],

  settingsForm: [
    {
      xtype: 'combo',
      name: 'property_clientType',
      fieldLabel: NX.I18n.get('AzureBlobstore_ClientType_FieldLabel'),
      helpText: NX.I18n.get('AzureBlobstore_ClientType_HelpText'),
      itemCls: 'required-field',
      displayField: 'name',
      valueField: 'id',
      editable: false,
      forceSelection: true,
      queryMode: 'local',
      triggerAction: 'all',
      emptyText: 'Select...',
      selectOnFocus: false,
      allowBlank: false,
      listeners: {
        added: function() {
          var me = this;
          me.getStore().load();
        },
        afterrender: function() {
          var me = this;
          if (!me.getValue()) {
            me.setValue('Sync');
          }
        }
      },
      store: 'NX.azureblobstore.store.ClientType'
    },
    {
      xtype:'textfield',
      name: 'property_accountName',
      fieldLabel: NX.I18n.get('AzureBlobstore_AccountName_FieldLabel'),
      helpText: NX.I18n.get('AzureBlobstore_AccountName_HelpText'),
      allowBlank: false
    },
    {
      xtype:'textfield',
      name: 'property_accountKey',
      fieldLabel: NX.I18n.get('AzureBlobstore_AccountKey_FieldLabel'),
      helpText: NX.I18n.get('AzureBlobstore_AccountKey_HelpText'),
      allowBlank: true
    },
    {
      xtype:'textfield',
      name: 'property_containerName',
      fieldLabel: NX.I18n.get('AzureBlobstore_ContainerName_FieldLabel'),
      helpText: NX.I18n.get('AzureBlobstore_ContainerName_HelpText'),
      allowBlank: true
    }
  ],

  exportProperties: function(values) {
    var properties = {};
    Ext.Object.each(values, function(key, value) {
      if (key.startsWith('property_')) {
        properties[key.replace('property_', '')] = String(value);
      }
    });
    return properties;
  }
  
});
