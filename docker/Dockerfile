FROM opensearchproject/opensearch:3.4.0

USER root
RUN yum install -y wget
RUN wget -P /tmp/ https://github.com/aryn-ai/aryn-opensearch-plugin/releases/download/3.4.0.0/aryn-opensearch-plugin-3.4.0.0.zip

RUN echo "y" | /usr/share/opensearch/bin/opensearch-plugin install file:///tmp/aryn-opensearch-plugin-3.4.0.0.zip

USER 1000:1000
