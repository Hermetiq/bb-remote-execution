#!/usr/bin/env bash
# Usage: bazel.sh <bazel arguments...>
#
# Runs Bazel inside Hermetiq's pinned CI Go image, which ships Bazelisk, so
# this repository's .bazelversion is honoured. Bazel's output root lives in
# .bkcache, which the step's `cache:` volume keeps across builds.
#
# If REGISTRY_DOCKER_CONFIG is set, that Docker configuration directory is
# made available to Bazel, so that image pushes can authenticate.
set -euo pipefail

GO_IMAGE="${CI_GO_IMAGE:-us-docker.pkg.dev/hermetiq-cloud/hermetiq-public/ci-go:go1.27.4-golangci2.14.0-36c1163}"
REPO_ROOT="$(pwd)"
CACHE_ROOT="${REPO_ROOT}/.bkcache"
mkdir -p "${CACHE_ROOT}"

docker_flags=(
  --rm --platform linux/amd64
  --volume "${REPO_ROOT}:/src" --workdir /src
  # Docker resolves the host-side .bkcache symlink (when a cache volume is
  # attached) at mount time, so mount it directly rather than through /src.
  --volume "${CACHE_ROOT}:/bkcache"
  --env BAZELISK_HOME=/bkcache/bazelisk
  # tools/workspace-status.sh stamps images on CI only, by running git. The
  # container's user doesn't own the checkout, so allow it explicitly.
  --env BUILDKITE
  --env GIT_CONFIG_COUNT=1
  --env GIT_CONFIG_KEY_0=safe.directory
  --env GIT_CONFIG_VALUE_0=/src
)
if [[ -n "${REGISTRY_DOCKER_CONFIG:-}" ]]; then
  docker_flags+=(
    --volume "${REGISTRY_DOCKER_CONFIG}:/registry-auth:ro"
    --env DOCKER_CONFIG=/registry-auth
  )
fi

exec docker run "${docker_flags[@]}" "${GO_IMAGE}" \
  bazel --output_user_root=/bkcache/bazel "$@"
