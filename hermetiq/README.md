# Hermetiq fork of bb-remote-execution

This repository is Hermetiq's fork of
[buildbarn/bb-remote-execution](https://github.com/buildbarn/bb-remote-execution).
Fork-specific build and CI files are kept apart from upstream's (this
directory and `.buildkite/`), so that merging upstream rarely conflicts.

## Images

The Buildkite pipeline in `.buildkite/pipeline.yml` (slug
`bb-remote-execution`) tests every non-tag build. On merges to `main` it also
pushes the bb-scheduler image:

```
us-central1-docker.pkg.dev/hermetiq-cloud/cloud/bb-scheduler:<YYYYMMDDThhmmssZ>-<commit>
```

The tag has the same form as upstream's `ghcr.io/buildbarn/bb-scheduler`
tags. The build annotation shows the tag and digest to use in the buildbarn
chart's `images.scheduler` values.

Upstream's GitHub Actions workflows in `.github/` publish to
`ghcr.io/buildbarn` and tag releases. They can't publish from this fork, so
GitHub Actions should stay disabled for this repository.

## One-time setup

1. Apply `cloud-native/pulumi-iac/buildkite-oidc`. It creates the
   `bk-bb-remote-execution` service account, which only `main` builds of this
   pipeline can impersonate.
2. Create the Buildkite pipeline with the slug `bb-remote-execution` for
   `git@github.com:Hermetiq/bb-remote-execution.git`, with the usual
   `buildkite-agent pipeline upload` step.
3. Disable GitHub Actions for the fork.

## Syncing with upstream

```
git fetch upstream
git checkout main
git merge upstream/main
git push origin main
```
