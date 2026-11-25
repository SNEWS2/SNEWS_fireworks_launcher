# Vendored GCN schema (test fixture)

These are copies of the GCN Unified Schema files that the SNEWS2 notice
depends on. The bridge's output is validated against them in
`tests/test_snews2_gcn_schema.py`.

Source: `gcn/notices/snews2/` and the `gcn/notices/core/` files it `$ref`s, from
https://github.com/nasa-gcn/gcn-schema, release tag `v7.3.0` (commit `388eb34`).
Released tags carry versioned `$id`s (`.../schema/v7.3.0/...`), which must match
`GCN_SNEWS2_SCHEMA_ID` in `snews2_gcn_schema.py`.

When bumping the pinned version, re-copy these files from the new tag and
update `GCN_SNEWS2_SCHEMA_ID` to match. The directory
layout matches gcn-schema, so the relative `$ref`s resolve without changes.
