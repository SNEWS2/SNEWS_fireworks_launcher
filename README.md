# SNEWS Fireworks Launcher

A production-ready pipeline for converting **SNEWS 2.0 messages** into standardized NASA GCN formats. This package acts as the central software bridge for real-time supernova neutrino alerts, propagating them from the SCiMMA/Hopskotch network to the global astronomical community via GCN.

```
detector observations ──► SNEWS coincidence server ──► Hopskotch alert topic
                                                              │
                                    snews2-gcn-bridge (this package)
                                    parse → transform to GCN schema
                                                              │
                                                              ▼
                                        GCN Kafka: gcn.notices.snews2.alert
```

## 🌟 Data Schema Interoperability

This package inherits its core data schemas from **[`snews-data-formats`](https://github.com/SNEWS2/snews-data-formats)** to ensure strict, typed validation of messages.
*Note: As an interim step, we are currently integrating with an unmerged branch/PR of `snews-data-formats`. The Fireworks Launcher gracefully handles these upstream schemas as we push through PR validation.*

---

## 🚀 Installation

Ensure you have Python 3.10+ installed.

### Quick Install (For Users)
If you only want to run the software and bridge tools, you can install the published package directly from PyPI:
```bash
pip install snews-fireworks-launcher
```

### Developer Install (For Contributors)
If you are developing the codebase, or wish to use the local Docker test environment, you should clone the repo:
```bash
git clone git@github.com:SNEWS2/SNEWS_fireworks_launcher.git
cd SNEWS_fireworks_launcher
python -m venv venv && source venv/bin/activate
pip install -e ".[test]"
```

> **Stale `snews-data-formats`:** the fork branch pinned in `pyproject.toml` does not bump its version string, so `pip install --upgrade` may keep an older copy. If `from snews_data_formats.data import CoincidenceTierAlert` fails, force a reinstall:
> ```bash
> pip install --force-reinstall --no-deps "snews-data-formats @ git+https://github.com/medhansh29/snews-data-formats.git@feat/add-coincidence-tier-alert"
> ```

---

## 🛠 Configuration & Credentials

The bridge behavior is controlled via environment variables. Copy `.env.example` to `.env` in the directory you run the CLI from and fill it in.

* **SCiMMA / Hopskotch (input):** authenticate once with `hop auth add` using your SCiMMA credentials. Your account needs read access to the SNEWS alert topics, and write access to `snews.experiments-test` if you want to trigger test alerts.
* **NASA GCN (output):** set `USE_GCN_CREDENTIALS=true`. GCN issues **separate** client credential pairs for producing and consuming, and they are not interchangeable:

  | Variable | Purpose |
  |---|---|
  | `GCN_PRODUCER_CLIENT_ID` / `GCN_PRODUCER_CLIENT_SECRET` | Used by the bridge and `gcn-produce-test` to publish |
  | `GCN_CONSUMER_CLIENT_ID` / `GCN_CONSUMER_CLIENT_SECRET` | Used by `gcn-consume` / `gcn-list-topics` to verify |
  | `GCN_DOMAIN` | `test.gcn.nasa.gov` (default), `dev.gcn.nasa.gov`, or `gcn.nasa.gov` (production). Credentials are domain-specific. |
  | `SNEWS2_GCN_TOPIC` | GCN topic to publish to (default `gcn.notices.snews2.alert`) |
  | `GCN_CONSUMER_GROUP` | Consumer group for `gcn-consume` (default `snews2-gcn-verify`) |

  Without `USE_GCN_CREDENTIALS=true`, the bridge publishes to a mock Kafka broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`).

---

## 💻 Running the Software

The installation exposes the `snews-fireworks-launcher` CLI. Run `snews-fireworks-launcher --help` for all options.

| Command | What it does |
|---|---|
| `snews2-gcn-bridge` | Subscribe to a SNEWS alert topic via `snews_pt subscribe` and forward each alert, transformed to the GCN schema, to GCN (or the mock broker) |
| `gcn-consume [--count N]` | Read notices back from the GCN topic to verify delivery |
| `gcn-list-topics` | List the GCN topics your consumer credentials can see |
| `gcn-produce-test` | Publish a hand-built notice straight to GCN (skips SNEWS and the schema pipeline) |
| `snews2-transform --tier <tier>` | Print a sample SNEWS2 message and its GCN transform (no Kafka) |
| `snews2-produce` / `snews2-consume` | Produce/consume mock SNEWS2 messages on the local Kafka broker |

### The Live GCN Bridge

`snews2-gcn-bridge` chooses which Hopskotch alert topic to listen to:

| Flags | Topic | Use for |
|---|---|---|
| `--test` (with either firedrill flag) | `snews.connection-testing` | **Test alerts** (`is_test=true`), i.e. e2e testing |
| `--firedrill` (default) | `snews.alert-firedrill` | Firedrill alerts |
| `--no-firedrill` | `snews.alert-test` | Real alerts (`snews_pt`'s default config) |

> The SNEWS coincidence server publishes every alert built from `is_test=true` observations to `snews.connection-testing`, **not** to `snews.alert-test`. For e2e testing, always pass `--test`.

---

## 🔁 End-to-End Test: SNEWS → GCN schema → GCN Kafka

This exercises the whole live chain. You send two synthetic test observations to SNEWS. The SNEWS dev coincidence server matches them and publishes a test alert. The bridge consumes that alert, converts it to the GCN schema, and publishes it to GCN. You don't need a real supernova: every message is flagged `is_test=true`, and all of it runs on SNEWS and GCN test topics.

### Prerequisites
1. Install the package (see above), and check that `CoincidenceTierAlert` imports.
2. Run `hop auth add` with your SCiMMA credentials.
3. Set up `.env` with `USE_GCN_CREDENTIALS=true`, `GCN_DOMAIN`, and both GCN producer and consumer credential pairs for that domain.
4. Sanity-check GCN on its own (optional, but it tells GCN problems apart from SNEWS problems):
   ```bash
   snews-fireworks-launcher gcn-list-topics     # should include gcn.notices.snews2.alert
   snews-fireworks-launcher gcn-produce-test
   snews-fireworks-launcher gcn-consume --count 1
   ```

### Run it (three terminals, all with the venv activated, from the directory holding `.env`)

**Terminal 1: watch GCN for the final notice**
```bash
snews-fireworks-launcher gcn-consume
```

**Terminal 2: start the bridge on the test-alert topic**
```bash
snews-fireworks-launcher snews2-gcn-bridge --no-firedrill --test
```
Wait until it prints `Broker:kafka://kafka.scimma.org/snews.connection-testing`.

**Terminal 3: trigger a test coincidence**
```bash
python scripts/trigger_test_coincidence.py
```
This sends two `is_test=true` observations (Super-K and IceCube, same neutrino time) to `snews.experiments-test`.

### What you should see
* **Terminal 2**, within a few seconds: an `ALERT MESSAGE` block with `alert_type: TEST ...` from a `server_tag` like `coincidence-server-dev-...`. The bridge logs nothing on a successful send; it prints only on failure (`Failed to process hopskotch plugin message: ...`).
* **Terminal 1:** a `GCN Notice [TEST] ...` block with `SNEWS2 Tier: CoincidenceTier` and the detector names.

The dev coincidence server keeps observations in its cache. Your alert may therefore arrive as an `UPDATE` that also lists detectors from other people's tests.

### Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Bridge never prints an alert | Check that it is on `snews.connection-testing` (`--test`). Also check that the SNEWS dev coincidence server is running: the bridge has nothing to receive without it. |
| `gcn-kafka not installed but USE_GCN_CREDENTIALS is True` / `NoBrokersAvailable` | `snews_pt` runs the plugin with a bare `python`. The CLI puts its own interpreter first on `PATH`; if you call `snews_pt subscribe -p .../gcn_bridge.py` directly, activate the venv first. |
| `gcn-consume` prints nothing, though the bridge didn't error | Another `gcn-consume` in the same consumer group holds the partition. Stop it, or run with a fresh group: `GCN_CONSUMER_GROUP=verify-$RANDOM snews-fireworks-launcher gcn-consume`. |
| `ImportError: CoincidenceTierAlert` | Stale `snews-data-formats`; see the install note above. |

---

## 🧪 CI & Testing 

The test suite is verified exactly as it runs in the GitHub Actions pipeline by utilizing an isolated Docker environment. This mirrors tests configured across other SNEWS packages like `snews_pt`.

To execute the tests in the container natively:
```bash
docker compose run test
```
*This command will automatically spin up the mock Kafka environment locally, evaluate the PyTest suite inside an isolated Docker container, and report the exit status.*

To run locally instead, use `pytest tests`. `tests/test_integration.py` needs the mock Kafka broker running (`docker compose up kafka`).

---

## 📂 Project Structure
* `src/snews_fireworks_launcher/`: The core installable python package.
  * `cli.py`: the `snews-fireworks-launcher` CLI.
  * `gcn_bridge.py`: the bridge, run as a `snews_pt subscribe` plugin.
  * `schemas/`: SNEWS2 message parsing and the SNEWS2 → GCN schema transform.
  * `utils/`: GCN producer/consumer and mock SNEWS2 Kafka helpers.
* `scripts/trigger_test_coincidence.py`: sends a synthetic test coincidence to SNEWS for e2e testing.
* `tests/`: unit and integration tests routing internal formats to GCN.
* `docker-compose.yml`: Local mock pipeline components and CI testing services.
* `Dockerfile.test`: Blueprint for the GitHub Actions verification container.
