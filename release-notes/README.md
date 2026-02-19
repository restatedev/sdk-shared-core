# Release Notes Process

This directory contains release notes for `restate-sdk-shared-core`, organized to track changes between releases.

## Structure

```
release-notes/
├── README.md          # This file
├── v0.9.0.md          # Consolidated release notes for v0.9.0
├── v1.0.0.md          # (future releases follow the same pattern)
└── unreleased/        # Release notes for changes not yet released
    └── *.md           # Individual release note files
```

## Adding Release Notes

When making a significant change, create a release note file in the `unreleased/` directory:

1. **Create a new file** in `unreleased/` with a descriptive name:
   - Format: `<pr-number>-<short-description>.md`
   - Example: `68-upgrade-assert2.md`

2. **Structure your release note** with the following sections:
   ```markdown
   # <Title>

   ## Breaking Change / Improvement / Bug Fix / Dependency Update

   ### What Changed
   Brief description of what changed.

   ### Impact on Downstream SDKs
   - Which SDKs are affected (TypeScript, Python, Rust)
   - Whether SDK code changes are needed
   - Any build or dependency implications

   ### Migration Guidance
   Steps downstream SDK maintainers should take, if any.

   ### Related Issues
   - PR #XXX: Description
   ```

3. **Commit the release note** with your changes:
   ```bash
   git add release-notes/unreleased/<your-file>.md
   git commit -m "Add release notes for <change>"
   ```

## Release Process

When creating a new release:

1. **Review all unreleased notes**: Check `unreleased/` for all pending release notes.

2. **Create a consolidated release notes file** (`v<version>.md`) with the following structure:

   ```markdown
   # restate-sdk-shared-core v<version> Release Notes

   ## Highlights
   - 2-4 bullet points summarizing the most important changes

   ## Breaking Changes
   (Sorted by impact, most impactful first)

   ## Improvements

   ## Bug Fixes

   ## Dependency Updates
   ```

3. **Consolidation guidelines**:
   - Sort items by impact within each category (most impactful first)
   - Preserve all migration guidance and related links
   - For items spanning multiple categories, place in the primary category with full context

4. **Delete the individual unreleased files** after consolidation:
   ```bash
   rm release-notes/unreleased/*.md
   ```

5. **Update the compatibility matrix** in `README.md`.

6. **Use `cargo release <VERSION>`** to bump the version in `Cargo.toml` and create the release commit/tag.

## Guidelines

### When to Write a Release Note

Write a release note for:
- **Breaking changes**: Any change to the public API or behavior that affects downstream SDKs
- **Protocol changes**: Updates to the service protocol or message format
- **Security fixes**: Dependency updates addressing vulnerabilities (RUSTSEC advisories, CVEs)
- **Significant dependency changes**: Changes that affect the build or runtime characteristics of downstream consumers (e.g., removing native dependencies)
- **Bug fixes**: Fixes that affect correctness of SDK behavior
- **New features**: New capabilities exposed to downstream SDKs

### When NOT to Write a Release Note

Skip release notes for:
- Internal refactoring with no downstream impact
- Test-only changes
- Documentation-only changes
- Minor dev-dependency updates
- Build system or CI changes

### Writing Style

- **Audience**: SDK maintainers integrating this crate — be precise about what changed and why
- **Be clear about impact**: State explicitly whether downstream SDKs need code changes
- **Link to advisories**: For security-related dependency updates, link to the RUSTSEC or CVE advisory
- **Link to PRs/issues**: Reference related pull requests and issues
