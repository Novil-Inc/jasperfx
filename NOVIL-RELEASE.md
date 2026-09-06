# Novil maintained queue backport

This MIT-licensed fork preserves the upstream authors and LICENSE. It is not an
upstream JasperFx release. Maintained branch: `novil/1.19`; upstream baseline:
`57c178af5db500193a592a261816264717b0869a` (tag plan `upstream/1.19.0`).
Reviewed queue implementation: `9fd89bb54ee689b39793c70af8f90ff8f79d60ef`. Release `1.19.1-novil.1` changes only
release metadata, CI/documentation on top of that implementation.

## Scope and limitations

Use JasperFx `1.19.1-novil.1` and WolverineFx `5.16.3-novil.1` together. The core
assembly versions remain `1.19.0.0` and `5.16.2.0`. Other packages are not released
by this procedure: Events `1.21.0`, RuntimeCompiler `4.4.0`, Marten `8.22.0`, and
Wolverine adapters `5.16.2` remain upstream packages.

Local buffered and retry queues intentionally use unbounded storage. Sustained
excess input can exhaust memory. Finite delivery/memory tests do not establish a
production memory budget, universal deadlock freedom, durable/broker behavior,
or drain-during-backlog shutdown guarantees. Existing lifecycle behavior remains.

## Repeatable, explicit release procedure

1. Check GitHub tags/releases, NuGet.org and local caches for version conflicts.
   Never overwrite a consumed or published version. Corrections require a new version.
2. Update only this core project's PackageVersion/InformationalVersion (retain assembly
   version), paired dependency minimum as needed, and this documentation. Commit source
   **before** building so repository commit, informational version and SourceLink agree.
3. Install .NET 8, 9 and 10. From a clean committed checkout run the commands in
   `.github/workflows/novil-core.yml`; frameworks run sequentially. Build/pack only
   the core package with `-p:ContinuousIntegrationBuild=true`, not the solution-wide
   publishing targets. Inspect nuspec repository commit, dependency groups, assembly
   versions, SourceLink and API compatibility against the upstream core package.
4. Record SHA-256 checksums for the exact tested nupkg and extracted DLLs. Run paired
   queue delivery and consumer compatibility validation on those bytes. Never rebuild
   a replacement under the same consumed version; CI artifacts are validation outputs,
   not permission to replace a release asset.
5. Obtain explicit maintainer approval for exact commits, tags and checksums. Preserve
   existing main refs, create the maintained branch and immutable upstream-baseline tag,
   and tag the source commit `v1.19.1-novil.1`. Publish the **already tested** nupkg plus
   `SHA256SUMS` as GitHub release assets. No NuGet.org or authenticated feed publishing.
   Never force push, clobber release assets, or auto-deploy. On ambiguous publication
   results inspect remote state before retrying.

Inherited workflow jobs are restricted to their exact upstream repository. Fork CI
has read-only contents permission and builds/tests/uploads artifacts only. It has no
release/tag trigger, publishing credentials, package push or deployment step.

## Baseline test qualifications

Prior upstream/candidate comparison found an unchanged EventTests failure in
`SliceGroupTests.enrich_with_using_entity_query` (collection modified), and unchanged
EventStoreTests compilation errors for missing event APIs. These are outside this
core queue backport. Scoped CI runs CoreTests, CodegenTests and CommandLineTests;
it does not claim the full solution is green. A pre-existing RetryBlock timing test
has also been intermittent; do not suppress, skip or retry it to manufacture success.
