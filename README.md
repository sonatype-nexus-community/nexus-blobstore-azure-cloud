<!--

    Sonatype Nexus (TM) Open Source Version
    Copyright (c) 2019-present Sonatype, Inc.
    All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.

    This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
    which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.

    Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
    of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
    Eclipse Foundation. All other trademarks are the property of their respective owners.

-->
Nexus Repository Azure Cloud Storage Blobstore
==============================

[![Join the chat at https://gitter.im/sonatype/nexus-developers](https://badges.gitter.im/sonatype/nexus-developers.svg)](https://gitter.im/sonatype/nexus-developers?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project adds [Azure Cloud Object Storage](https://cloud.Azure.com/storage/) backed blob stores to Sonatype Nexus 
Repository 3.16 and later.  It allows Nexus Repository to store the components and assets in Azure Cloud instead of a
local filesystem.

Contribution Guidelines
-----------------------
Go read [our contribution guidelines](/.github/CONTRIBUTING.md) to get a bit more familiar with how we would like things to flow.

Requirements
------------
* [Apache Maven 3.3.3+](https://maven.apache.org/install.html)
* [Java 8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* Network access to https://repository.sonatype.org/content/groups/sonatype-public-grid

Building
--------
To build the project and generate the bundle use Maven:

    mvn clean package

Creating Docker Image bundled with Azure Storage Plugin
-------------------------------------------------------
To create a docker image and run it with the Azaure Storage plugin baked in, run the following commands: 

    mvn clean package
    docker build -t nexus3_azure .
    docker run -d -p 8081:8081 --name nexus3azure -v nexus3azure-data:/nexus-data nxrm_azure:latest
    

Integration Tests
-----------------
To run integration commands active the `it` profile and include the system properties `nxrm.azure.accountName` and 
`Dnxrm.azure.accountKey`. Integration tests will create temporary storage containers and tests should cleanup when 
complete. If you're running tests often check your storage account because it is likely that some containers may be left
behind and not properly cleanup up by the tests. 

     mvn clean install -P it -Dnxrm.azure.accountName=<accountName> -Dnxrm.azure.accountKey=<accountKey>


Azure Cloud Storage Permissions
--------------------------------
TODO

Azure Cloud Storage Authentication
-----------------------------------
TODO


Installation
------------
While nexus is stopped, copy target/nexus-blobstore-azure-cloud-0.4.0-SNAPSHOT-shaded.jar in the deploy folder in your
nexus 3 distribution. Start nexus and the plugin will be installed.

Log in as admin and create a new blobstore, selecting 'Azure Cloud Storage' as the type.

The Fine Print
--------------

It is worth noting that this is **NOT SUPPORTED** by Sonatype, and is a contribution of ours
to the open source community (read: you!)

Remember:

* Use this contribution at the risk tolerance that you have. 
* There are some incomplete features and known issues.
* Do NOT file Sonatype support tickets related to Azure Cloud support
* DO file issues here on GitHub, so that the community can pitch in

Phew, that was easier than I thought. Last but not least of all:

Have fun creating and using this plugin and the Nexus platform, we are glad to have you here!

Getting help
------------

Looking to contribute to our code but need some help? There's a few ways to get information:

* Chat with us on [Gitter](https://gitter.im/sonatype/nexus-developers)
* Check out the [Nexus3](http://stackoverflow.com/questions/tagged/nexus3) tag on Stack Overflow
* Check out the [Nexus Repository User List](https://groups.google.com/a/glists.sonatype.com/forum/?hl=en#!forum/nexus-users)