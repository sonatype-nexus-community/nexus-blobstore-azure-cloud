FROM maven:3.5.4 as nexus-blobstore-azure-cloud
WORKDIR /build
ADD . /build
RUN mvn clean install -D skipTests

FROM sonatype/nexus3:3.16.1
ADD install-plugin.sh /opt/plugins/nexus-blobstore-azure-cloud/
COPY --from=nexus-blobstore-azure-cloud /build/target/nexus-blobstore-azure-cloud-0.4.0-SNAPSHOT-shaded.jar /opt/sonatype/nexus/deploy
