# SNEWS Fireworks Launcher

A production-ready pipeline for converting **SNEWS 2.0 messages** into standardized NASA GCN formats. This package acts as the central software bridge for real-time supernova neutrino alerts, propagating them from the SCiMMA/Hopskotch network to the global astronomical community via GCN.

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
pip install -e .[test]
```

---

## 💻 Running the Software

The installation exposes the `snews-fireworks-launcher` CLI command globally on your system.

### The Live GCN Bridge
To start listening to the SCiMMA network and translating to GCN:
```bash
# Listen to the firedrill network (testing)
snews-fireworks-launcher snews2-gcn-bridge --firedrill

# Listen to the real scientific network (production watch)
snews-fireworks-launcher snews2-gcn-bridge --no-firedrill
```

### Local Simulation & Verification
You can also run commands to locally simulate the SNEWS pipeline:
```bash
# Produce a mock coincidence alert
snews-fireworks-launcher snews2-produce --tier coincidence --test

# Consume and view the mock alert
snews-fireworks-launcher snews2-consume
```

---

## 🛠 Configuration & Credentials

The bridge behavior is controlled via environment variables. Create a `.env` file in your execution directory based on `.env.example`.

* **SCiMMA Network (Ingest):** To listen to Hopskotch, authenticate your machine by running `hop auth add`.
* **NASA GCN (Output):** Set `USE_GCN_CREDENTIALS=true` and provide your `GCN_CLIENT_ID` and `GCN_CLIENT_SECRET` in the `.env` to publish to the actual NASA GCN. Otherwise, mocked output bridges to `localhost:9092`.

---

## 🧪 CI & Testing 

The test suite is verified exactly as it runs in the GitHub Actions pipeline by utilizing an isolated Docker environment. This mirrors tests configured across other SNEWS packages like `snews_pt`.

To execute the tests in the container natively:
```bash
docker compose run test
```
*This command will automatically spin up the mock Kafka environment locally, evaluate the PyTest suite inside an isolated Docker container, and report the exit status.*

---

## 📂 Project Structure
* `src/snews_fireworks_launcher/`: The core installable python package containing mapping tools, the CLI execution scripts, and the primary bridge runner.
* `tests/`: End-to-end integration tests routing internal formats to GCN.
* `docker-compose.yml`: Local mock pipeline components and CI testing services.
* `Dockerfile.test`: Blueprint for the GitHub Actions verification container.
