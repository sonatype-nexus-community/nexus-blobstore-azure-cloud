FROM sonatype/nexus3:3.19.1

COPY target/nexus-blobstore-azure-blob-0.4.0-SNAPSHOT-bundle.kar /opt/sonatype/nexus/deploy
