#!/usr/bin/env bash
# build_and_run.sh — build and start the local XNAT integration test container.
# Synthetic/localhost only. Credentials are throwaway (admin/admin, xnat/xnat).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="xnat-local-it"
CONTAINER_NAME="xnat-local-it"
MAX_WAIT=900  # 15 minutes — first boot does DB init

# Remove existing container if present
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing container ${CONTAINER_NAME}..."
    docker rm -f "${CONTAINER_NAME}"
fi

echo "Building image ${IMAGE_NAME}..."
docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

echo "Starting container..."
docker run -d --name "${CONTAINER_NAME}" -p 8080:8080 "${IMAGE_NAME}"

echo "Polling http://localhost:8080 (max ${MAX_WAIT}s)..."
elapsed=0
while [ $elapsed -lt $MAX_WAIT ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080 2>/dev/null || true)
    if [ "$code" = "200" ] || [ "$code" = "302" ]; then
        echo "XNAT ready — HTTP $code after ${elapsed}s"
        break
    fi
    sleep 10
    elapsed=$((elapsed + 10))
    echo "  ...${elapsed}s — last code: ${code}"
done

final_code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080 2>/dev/null || echo "000")
echo "Final HTTP status: ${final_code}"

if [ "$final_code" != "200" ] && [ "$final_code" != "302" ]; then
    echo "ERROR: XNAT did not become ready within ${MAX_WAIT}s."
    echo "Container logs:"
    docker logs --tail 50 "${CONTAINER_NAME}"
    exit 1
fi
