FROM maven as nexus-blobstore-azure-cloud
WORKDIR /build
RUN git clone https://github.com/sonatype-nexus-community/nexus-blobstore-azure-cloud.git .
RUN mvn clean install

FROM sonatype/nexus3:3.13.0
ADD install-plugin.sh /opt/plugins/nexus-blobstore-azure-cloud/
COPY --from=nexus-blobstore-azure-cloud /build/target/ /opt/plugins/nexus-blobstore-azure-cloud/target/
COPY --from=nexus-blobstore-azure-cloud /build/pom.xml /opt/plugins/nexus-blobstore-azure-cloud/

USER root

RUN cd /opt/plugins/nexus-blobstore-azure-cloud/ && \
    chmod +x install-plugin.sh && \
    ./install-plugin.sh /opt/sonatype/nexus/ && \
    rm -rf /opt/plugins/nexus-blobstore-azure-cloud/

RUN chown -R nexus:nexus /opt/sonatype/

USER nexus
