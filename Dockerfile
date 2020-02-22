FROM sonatype/nexus3:3.21.1

COPY target/nexus-blobstore-azure-cloud-0.5.0-SNAPSHOT-bundle.kar /opt/sonatype/nexus/deploy
