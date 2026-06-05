FROM tomcat:9-jdk8-temurin-jammy

# User configurable arguments
ARG XNAT_VERSION=1.9.3
ARG XNAT_CS_PLUGIN_VERSION=3.7.3
ARG XNAT_BATCH_LAUNCH_PLUGIN_VERSION=0.9.0-xpl
ARG XNAT_ROOT=/data/xnat
ARG XNAT_HOME=/data/xnat/home
ARG JAVA_MS=256m
ARG JAVA_MX=2g
ARG XNAT_DATASOURCE_DRIVER=org.postgresql.Driver
ARG XNAT_DATASOURCE_URL=jdbc:postgresql://localhost:5432/xnat
ARG XNAT_DATASOURCE_USERNAME=xnat
ARG XNAT_DATASOURCE_PASSWORD=xnat
ARG XNAT_HIBERNATE_DIALECT=org.hibernate.dialect.PostgreSQL9Dialect
ARG TOMCAT_XNAT_FOLDER=ROOT
ARG TOMCAT_XNAT_FOLDER_PATH=${CATALINA_HOME}/webapps/${TOMCAT_XNAT_FOLDER}

# Copy config script early (used during build)
COPY make-xnat-config.sh /usr/local/bin/make-xnat-config.sh

# Install dependencies (Ubuntu jammy — no alpine/debian paths)
RUN apt-get update && apt-get install -y --no-install-recommends \
        postgresql \
        postgresql-client \
        supervisor \
        wget \
        vim \
        unzip \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create directory structure
RUN rm -rf ${CATALINA_HOME}/webapps/* && \
    mkdir -p \
        ${TOMCAT_XNAT_FOLDER_PATH} \
        ${XNAT_HOME}/config \
        ${XNAT_HOME}/logs \
        ${XNAT_HOME}/plugins \
        ${XNAT_HOME}/work \
        ${XNAT_ROOT}/build \
        ${XNAT_ROOT}/cache \
        ${XNAT_ROOT}/ftp \
        ${XNAT_ROOT}/pipeline \
        ${XNAT_ROOT}/archive \
        ${XNAT_ROOT}/prearchive \
        /var/log/postgresql \
        /var/log/tomcat

# Generate xnat-conf.properties
RUN chmod +x /usr/local/bin/make-xnat-config.sh && \
    /usr/local/bin/make-xnat-config.sh && \
    rm /usr/local/bin/make-xnat-config.sh

# Download XNAT WAR and plugins from Bitbucket (confirmed reachable)
# --no-check-certificate needed: egress proxy presents its own CA for TLS inspection
RUN wget --no-verbose --no-check-certificate \
        --output-document=/tmp/xnat-web-${XNAT_VERSION}.war \
        https://api.bitbucket.org/2.0/repositories/xnatdev/xnat-web/downloads/xnat-web-${XNAT_VERSION}.war && \
    unzip -o -d ${TOMCAT_XNAT_FOLDER_PATH} /tmp/xnat-web-${XNAT_VERSION}.war && \
    rm -f /tmp/xnat-web-${XNAT_VERSION}.war

RUN wget --no-verbose --no-check-certificate \
        --output-document ${XNAT_HOME}/plugins/container-service-plugin.jar \
        https://api.bitbucket.org/2.0/repositories/xnatdev/container-service/downloads/container-service-${XNAT_CS_PLUGIN_VERSION}-fat.jar

RUN wget --no-verbose --no-check-certificate \
        --output-document ${XNAT_HOME}/plugins/batch-launch-plugin.jar \
        https://api.bitbucket.org/2.0/repositories/xnatx/xnatx-batch-launch-plugin/downloads/batch-launch-${XNAT_BATCH_LAUNCH_PLUGIN_VERSION}.jar

# Set environment
ENV XNAT_HOME=${XNAT_HOME} \
    XNAT_DATASOURCE_USERNAME=${XNAT_DATASOURCE_USERNAME} \
    PGPASSWORD=${XNAT_DATASOURCE_PASSWORD}
ENV CATALINA_OPTS="-Xms${JAVA_MS} -Xmx${JAVA_MX} -Dxnat.home=${XNAT_HOME}"

# Copy runtime scripts
COPY launch-xnat.sh /launch-xnat.sh
COPY set-admin-password.sh /set-admin-password.sh
COPY XNAT.sql /XNAT.sql
RUN chmod +x /launch-xnat.sh /set-admin-password.sh

# Skip new-site setup
COPY xnat-prefs-init.ini ${XNAT_HOME}/config/prefs-init.ini

EXPOSE 8080

# Setup Postgres data directory
# Ubuntu jammy installs postgresql-14; binaries at /usr/lib/postgresql/14/bin/
RUN mkdir -p /run/postgresql /var/lib/postgresql/data && \
    chown postgres:postgres /run/postgresql /var/lib/postgresql/data && \
    chmod 700 /var/lib/postgresql/data

USER postgres
RUN /usr/lib/postgresql/14/bin/initdb -D /var/lib/postgresql/data
USER root

# supervisord config
COPY supervisord.conf /etc/supervisord.conf

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
