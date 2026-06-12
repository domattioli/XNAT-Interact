#!/bin/sh
# set-admin-password.sh
# Waits for XNAT to initialize, then resets admin password to "admin".
# XNAT creates the admin user with a random bcrypt password on first boot;
# this script replaces it with the known throwaway password for integration tests.
#
# Runs as a supervisord program (low-priority, after tomcat).

set -e

ADMIN_HASH='{bcrypt}$2a$10$bRnW96rfBzznqttC3Lz/P..pERY1mMLr69bbVn7UQXVcz4e7HtkbO'
# ^ placeholder — we replace with a hash we generated; see below
# Real hash for "admin" (bcrypt, rounds=10):
ADMIN_HASH='{bcrypt}$2b$10$3roDdRzH/fM135zPfPExeeiaXZgbNPQdFKVItQ43QICzD4WvfNOgi'

>&2 echo "set-admin-password: waiting for XNAT to initialize..."

# Wait until XNAT HTTP is up (max 15 min)
elapsed=0
while [ $elapsed -lt 900 ]; do
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080 2>/dev/null || echo "000")
  if [ "$code" = "200" ] || [ "$code" = "302" ]; then
    break
  fi
  sleep 10
  elapsed=$((elapsed + 10))
done

# Wait a little more for DB init to fully settle
sleep 10

>&2 echo "set-admin-password: resetting admin password..."
psql -U postgres -d xnat -c "UPDATE xdat_user SET primary_password='${ADMIN_HASH}', salt='' WHERE login='admin';" || true
>&2 echo "set-admin-password: done."
