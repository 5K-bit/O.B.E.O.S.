## OBEOS (v0.1 scaffold)

OBEOS is a **command OS above the kernel**: a long-running engine is the one authority, and clients (CLI/desktop/scripts) connect over a UNIX socket to issue intent and observe outcomes.

This repo contains a minimal working scaffold aligned to the slab:

- **engine**: sqlite state store, internal bus, job scheduler, unix-socket IPC server
- **modules**: `build`, `control`, `observe`, `memory`
- **clients**: `bf` CLI, optional GTK console
- **systemd**: unit file for boot start

### Repo layout (mirrors `/opt/obeos`)

- `opt/obeos/engine/`: engine runtime
- `opt/obeos/modules/`: modules
- `opt/obeos/cli/bf`: CLI client
- `opt/obeos/desktop/`: GTK console + `.desktop`
- `opt/obeos/storage/`: sqlite DB + artifacts directory
- `opt/obeos/config.yaml`: config (socket path, db path, enabled modules)
- `opt/obeos/VERSION`: version

### Local quickstart (runs from this repo)

Install deps:

```bash
python3 -m pip install -r requirements.txt
```

Run the engine using a dev-safe root + socket path:

```bash
export OBEOS_ROOT="/workspace/opt/obeos"
export OBEOS_SOCKET="/tmp/obeos.sock"
python3 "/workspace/opt/obeos/engine/engine.py"
```

In another shell, talk to it:

```bash
export OBEOS_SOCKET="/tmp/obeos.sock"
python3 "/workspace/opt/obeos/cli/bf" status
python3 "/workspace/opt/obeos/cli/bf" build obeos amd64 minimal --follow
python3 "/workspace/opt/obeos/cli/bf" jobs 20
python3 "/workspace/opt/obeos/cli/bf" logs last 1h
python3 "/workspace/opt/obeos/cli/bf" memory summary
python3 "/workspace/opt/obeos/cli/bf" shutdown
```

### systemd

The unit file is at `packaging/systemd/blackfong-engine.service` and matches the slab’s ExecStart:

- `ExecStart=/usr/bin/python3 /opt/obeos/engine/engine.py`

To actually install it on a host OS, you’d copy `opt/obeos` to `/opt/obeos` and the unit file into `/etc/systemd/system/`, then `systemctl enable --now blackfong-engine`.