#!/usr/bin/env bash
set -euo pipefail

# Amazon Linux 2023 user-data helper: Neo4j 5 in Docker on EC2.
dnf update -y
dnf install -y docker
systemctl enable --now docker
usermod -aG docker ec2-user || true

PASSWORD="${NEO4J_PASSWORD:-changeme}"
mkdir -p /opt/neo4j/{data,logs,conf}

docker pull neo4j:5-community
docker rm -f neo4j || true
docker run -d --name neo4j --restart unless-stopped \
  -p 7474:7474 -p 7687:7687 \
  -v /opt/neo4j/data:/data \
  -v /opt/neo4j/logs:/logs \
  -e NEO4J_AUTH="neo4j/${PASSWORD}" \
  -e NEO4J_server_memory_heap_initial__size=1G \
  -e NEO4J_server_memory_heap_max__size=2G \
  -e NEO4J_server_memory_pagecache_size=1G \
  -e NEO4J_dbms_security_procedures_unrestricted=gds.* \
  neo4j:5-community
