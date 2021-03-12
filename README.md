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
This repository is no longer maintained as the Azure blob storage implementation has been moved into the Repository Manager codebase as a supported feature for pro users. Upgrade to the latest version of Repository Manager to use Azure backed blob stores (Pro feature). Also, issues for this project have been disabled. To open new issues relating to Azure blob stores create a new issue in JIRA.


Nexus Repository Azure Blob Storage Blobstore
==============================
This project adds [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) backed blob stores to Sonatype Nexus Repository 3.16 and later. It allows Nexus Repository to store the components and assets in Azure Blobs instead of a local filesystem.


Requirements
------------
* [Apache Maven 3.3.3+](https://maven.apache.org/install.html)
* [Java 8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* Network access to https://repository.sonatype.org/content/groups/sonatype-public-grid

Building
--------
To build the project and generate the bundle use Maven:

    mvn clean package

Creating Docker Image bundled with Azure Blob Storage Plugin
-------------------------------------------------------
To create a docker image and run it with the Azure Blob Storage plugin baked in, run the following commands: 

    mvn clean package
    docker build -t nexus3_azure .
    docker run -d -p 8081:8081 --name nexus3azure -v nexus3azure-data:/nexus-data nxrm_azure:latest
    

Integration Tests
-----------------
To run integration commands, activate the `it` profile and include the system properties `nxrm.azure.accountName` and 
`Dnxrm.azure.accountKey`. Integration tests will create temporary storage containers and tests should cleanup when 
complete. If you're running tests often check your storage account because it is likely that some containers may be left
behind and not properly cleanup up by the tests. 
 
     mvn clean install -P it -Dnxrm.azure.accountName=<accountName> -Dnxrm.azure.accountKey=<accountKey>


Installation
------------
In Nexus Repository Manager 3.30.0+ Azure Blob Store support is available for pro users.
This plugin is not going to be updated or supported anymore. Use at your own risk.

While nexus is stopped, copy target/nexus-blobstore-azure-cloud-0.5.0-SNAPSHOT-shaded.jar in the deploy folder in your
nexus 3 distribution. Start nexus and the plugin will be installed.

Log in as admin and create a new blobstore, selecting 'Azure Cloud Storage' as the type.

The Fine Print
--------------

It is worth noting that this is **NOT SUPPORTED** by Sonatype, and is a contribution of ours
to the open source community (read: you!)

Remember:

* Use this contribution at the risk tolerance that you have. 
* There are some incomplete features and known issues.
* Do NOT file Sonatype support tickets related to Azure blob store support

