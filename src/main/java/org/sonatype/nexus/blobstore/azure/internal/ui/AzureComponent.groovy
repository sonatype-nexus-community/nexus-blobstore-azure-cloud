package org.sonatype.nexus.blobstore.azure.internal.ui

import javax.inject.Named
import javax.inject.Singleton

import org.sonatype.nexus.extdirect.DirectComponentSupport

import com.softwarementors.extjs.djn.config.annotations.DirectAction
import com.softwarementors.extjs.djn.config.annotations.DirectMethod
import org.apache.shiro.authz.annotation.RequiresPermissions

@Named
@Singleton
@DirectAction(action = 'azure_Azure')
class AzureComponent
    extends DirectComponentSupport
{
  /**
   * Azure client type
   */
  @DirectMethod
  @RequiresPermissions('nexus:settings:read')
  List<ClientTypeXO> clientTypes() {
    [
        new ClientTypeXO(order: 0, id: 'sync', name: 'Sync (Default)'),
        new ClientTypeXO(order: 1, id: 'async', name: 'Async (Experimental)')
    ]
  }
}
