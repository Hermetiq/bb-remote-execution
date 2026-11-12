#!/usr/bin/env bash
# Builds the bb-scheduler image from this commit and pushes it to Hermetiq's
# private Artifact Registry repository (//hermetiq:bb_scheduler_container_push).
# Runs on merges to main as bk-bb-remote-execution, whose credentials the
# gcp-workload-identity-federation plugin provides.
set -euo pipefail

REGISTRY_HOST="us-central1-docker.pkg.dev"
REPOSITORY="${REGISTRY_HOST}/hermetiq-cloud/cloud/bb-scheduler"
BOOTSTRAP_IMAGE="${BOOTSTRAP_IMAGE:-google/cloud-sdk:slim}"

# The tag that tools/workspace-status.sh stamps into the push target.
commit_time="$(git show -s --format=%ct HEAD)"
tag="$(TZ=UTC date --date "@${commit_time}" +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)"
image="${REPOSITORY}:${tag}"

# Keep registry credentials in a throwaway Docker configuration, so they
# don't outlive this job on the agent.
registry_config="$(mktemp -d)"
trap 'rm -rf "${registry_config}"' EXIT

# Share the WIF credentials written by the plugin with the gcloud container,
# so that it can mint an Artifact Registry access token.
wif_creds_args=(
  -v "${BUILDKITE_OIDC_TMPDIR:-/tmp}":"${BUILDKITE_OIDC_TMPDIR:-/tmp}"
  -e GOOGLE_APPLICATION_CREDENTIALS
  -e CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE
)

echo "--- :gcloud: docker login to ${REGISTRY_HOST}"
token="$(docker run --rm "${wif_creds_args[@]}" "${BOOTSTRAP_IMAGE}" gcloud auth print-access-token)"
printf '%s' "${token}" | DOCKER_CONFIG="${registry_config}" \
  docker login -u oauth2accesstoken --password-stdin "https://${REGISTRY_HOST}"

echo "+++ :bazel: push ${image}"
REGISTRY_DOCKER_CONFIG="${registry_config}" bash .buildkite/scripts/bazel.sh \
  run --stamp //hermetiq:bb_scheduler_container_push

echo "--- :mag: verify ${image}"
# Fails if the stamped tag differs from the one computed above.
digest="$(DOCKER_CONFIG="${registry_config}" docker buildx imagetools inspect "${image}" |
  sed -n 's/^Digest:[[:space:]]*//p')"
if [[ ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "could not resolve the digest of ${image}" >&2
  exit 1
fi

echo "+++ :white_check_mark: pushed ${image}@${digest}"
buildkite-agent annotate --style success --context bb-scheduler-image \
  "### :package: bb-scheduler \`${tag}\`
- \`${image}\`
- \`${digest}\`

Use it from the buildbarn chart with \`images.scheduler.repository: ${REPOSITORY}\`, \`images.scheduler.tag: ${tag}\` and \`images.scheduler.digest: ${digest}\`." || true
