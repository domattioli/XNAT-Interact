# Local XNAT Integration Harness

## Why Ubuntu base

Alpine and Debian package repos are blocked by egress proxy (`/alpine/` and `/debian/` paths → HTTP 403). Ubuntu `archive.ubuntu.com` is reachable. Base image: `tomcat:9-jdk8-temurin-jammy` (Ubuntu jammy). Docker Hub pulls and `api.bitbucket.org` also work, so WAR/plugin downloads are unaffected.

## Versions

- XNAT 1.9.3
- container-service plugin 3.7.3
- batch-launch plugin 0.9.0-xpl
- PostgreSQL 14 (Ubuntu jammy default)

## Build and run

```bash
cd tests/integration/xnat_local
bash build_and_run.sh
```

First boot takes several minutes (DB init + WAR deployment). The script polls until HTTP 200/302.

## Verify

```bash
# Session token — should return a session ID, not 401
curl -s -u admin:admin http://localhost:8080/data/JSESSION
```

## Teardown

```bash
docker rm -f xnat-local-it
```

## Credentials

Throwaway localhost-only defaults:
- XNAT admin: `admin` / `admin`
- DB user: `xnat` / `xnat`

Do NOT use against any production server.
