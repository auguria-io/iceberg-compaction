# Validation of the Auguria fork

This public repository uses a private dependency. Do not add private repository
credentials to its Actions workflows or publish dependency sources/build caches.

Public CI checks formatting and static source quality only. A green public check
is **not** evidence that compilation, unit tests, or integration tests passed.

Build, lint and unit/mock validation runs in the private `auguria-io/rust-pkgs`
repository, in the **Fork Dependency Validation** workflow. Its reviewed
`.github/fork-validation.json` records immutable revisions. It checks that the
open PR heads match those revisions before and after the tests. It does not
follow arbitrary contributor branches or upload artifacts to this repository.

## Before merging

1. Update the revision in the existing rust-pkgs remediation PR when this PR changes.
2. Wait for all private validation jobs to pass for that revision.
3. Record the private run link and tested revision in this PR's description.
4. The human reviewer must compare that revision to this PR's current head before
   approving/merging. Public formatting checks alone are insufficient.

This is a review requirement, not an automatically enforced cross-repository
GitHub status gate. The private run fails if heads change during its execution;
changes after it finishes require another run. Integration tests against a tenant
remain a separate release check, after approved merges and creation of the RC.
