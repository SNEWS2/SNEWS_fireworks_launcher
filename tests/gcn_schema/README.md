# Vendored GCN schema (test fixture)

These are copies of the GCN Unified Schema files that the SNEWS2 notice
depends on. The bridge's output is validated against them in
`tests/test_snews2_gcn_schema.py`.

Source: `gcn/notices/snews2/` and the `gcn/notices/core/` files it `$ref`s, from
https://github.com/medhansh29/gcn-schema, branch
`feature/add-snews2-coincidence-schema`, commit `143d349`.

Update these files whenever the SNEWS2 schema changes upstream. The directory
layout matches gcn-schema, so the relative `$ref`s resolve without changes.
