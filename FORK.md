# NymphAI Codex fork

This repository is a source fork of https://github.com/openai/codex. Fork behavior
is implemented and reviewed as ordinary commits in the source tree. Building the
fork does not require applying Codex source patches from the NymphAI monorepo.
Patches for external build dependencies are separate from the Codex source fork.

The current upstream baseline is `rust-v0.166.0`
(`6ff1e1ca3b30fc8b914720681fcf5fe62e95f856`), the latest stable upstream release
verified on 2026-10-29. The Rust workspace declares version `0.166.0`.

Fork changes include Fabric thread identity and catalog refresh, daemon-mediated
MCP OAuth token leasing, shared authentication refresh serialization, display
limits and bounded model-capacity retries. Fabric hosts Codex over ACP and pushes
its inbox in as prompts, so the fork carries no Fabric delivery code; it carries
only changes that make Codex itself better.

## Updating upstream

Run `tools/fork/rebase-upstream.sh` from a clean fork feature branch. It resolves
the latest stable release using GitHub's upstream release metadata, fetches the
release, records a backup branch, and rebases the fork commits. An explicit
stable release tag may be supplied as its only argument. Resolve conflicts in
source; regenerate protocol schemas from the rebased Rust definitions. Do not
restore older generated bundles over upstream's new schemas.

Verify the release is an ancestor with `git merge-base --is-ancestor
rust-v0.166.0 HEAD`. Compile and test through the monorepo's BuildBuddy RBE
workflow; Buildkite qualifies release artifacts. Publish a candidate branch
before promoting the maintained `fabric-patchset` release branch. Never force
push the default branch as part of a rebase.

Source publication, monorepo source pins, operator package publication and daemon
activation are distinct steps. The monorepo must pin the published source commit
and archive checksum; its Nix operator package must name a qualified package
built from that same commit. A local rebase does not upgrade a running daemon.
The historical default branch contains the release-sync workflow and is not the
maintained product branch; consumers must use an immutable qualified fork commit.
