# Run on your Mac and publish on GitHub

## Unpack and build

```bash
mkdir -p ~/Projects
unzip ~/Downloads/market-data-pipeline.zip -d ~/Projects
cd ~/Projects/market-data-pipeline
export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
make -j2 test
make demo
make faults
```

That SDK selection matches the installed compatible SDK used to fix the earlier matching-engine build. On another Mac without that directory, omit the export and use its normal compiler toolchain.

The offline demo writes a journal and checks exact replay. The fault command writes five scenario journals and a metrics report under a fresh `data/faults-*` directory. The WebSocket integration test is skipped until the dependency is installed.

## Live feed

```bash
make setup
.venv/bin/python -m mdpipe capture --product BTC-USD --seconds 60
```

To include the real local WebSocket transport test:

```bash
make test PYTHON=.venv/bin/python
```

Try a live lost-message recovery:

```bash
.venv/bin/python -m mdpipe capture --product BTC-USD --seconds 60 --drop-at 7 --quiet
```

Inspect the printed `invalidations`, `reconnects`, `snapshots`, and final `last_trusted.generation`. A gap should close publication, then a higher generation's snapshot should restore it.

## Replay and research CSV

Choose unused output filenames:

```bash
.venv/bin/python -m mdpipe capture --seconds 60 --output data/btc-first.sqlite --quiet
.venv/bin/python -m mdpipe replay data/btc-first.sqlite --speed 20 --features data/btc-first.csv
.venv/bin/python -m mdpipe replay data/btc-first.sqlite --speed 0 --quotes
```

## Measure your machine

```bash
mkdir -p benchmarks
./build/core-bench --events 100000 --trials 5 > benchmarks/mac_core.csv
make sanitize PYTHON=.venv/bin/python
```

The benchmark measures only C++ book updates. Use live capture metrics for full receive/parse/IPC/commit/publication latency. Commit a concise benchmark report together with the raw CSV and machine/compiler information.

## Publish a public repository

If the GitHub CLI is not installed, install it with Homebrew:

```bash
brew install gh
```

If you have not already signed in:

```bash
gh auth login
```

From the project directory:

```bash
git init -b main
git add .
git commit -m "Build fault-tolerant market data capture and verified replay"
gh repo create market-data-pipeline --public --source=. --remote=origin --push
gh repo view --web
```

The ZIP contains source files, documentation, tests, benchmark reports, and two compressed live examples. New captures, decompressed example journals, virtual environments, binaries, and build directories are ignored by Git. GitHub Actions will run the Linux/macOS test matrix after the push.

If you already created a repository with this name, use its existing remote instead of creating another repository.
