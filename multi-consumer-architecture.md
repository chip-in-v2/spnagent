# Architecture: Dynamic Multi-Process Consumer Container

## Overview
This design allows running multiple consumer processes with different configurations within a single container. 
The scaling of consumers and their respective settings (environment variables, configuration files, and certificates) are injected externally.

**Motivation:** 
In Kubernetes, there is a need to run multiple consumers as "sidecars" (for direct communication via localhost) where dynamic scaling is required. Since K8s Pod specs are static (adding a container requires a Pod restart), this approach uses a single container that manages multiple processes dynamically to avoid restarts.

## Container Architecture

### Process Hierarchy
```
Container (Multi-Consumer)
PID 1: tini
|-- Process Manager: supervisord
|-- Watcher Loop: inotifywait + config-reloader.sh (Shell Script)
|-- Consumer 1: ENV1=a /opt/consumer-binary --config=/etc/consumers/a.yaml
|-- Consumer 2: ENV1=b /opt/consumer-binary --config=/etc/consumers/b.yaml
`-- (Dynamic addition/removal of files in supervisor/conf.d -> Auto start/stop)
```

### Directory Structure

```
/etc/supervisor/supervisord.conf
 `-- supervisor/conf.d/*.conf 
     `-- 00-default.conf (Dummy file for directory existence)

/etc/ssl/
 `-- workers/
     |-- ca-certificates/ (CA certs for verifying target URLs (ConfigMap))
     |   `-- private-ca.crt
     `-- client-certs/ (Client certs for authentication (Secret))
         |-- worker-a.crt
         `-- worker-a.key
```

**K8s Implementation Details:**
 - **Volume 1 (ConfigMap):** Mounted to `/etc/supervisor/conf.d/` (Controls dynamic process scaling).
 - **Volume 2 (Secret):** Mounted to `/etc/ssl/workers/` (Secure communication).

## Configuration Files

### Config Reloader Script (`config-reloader.sh`)
```config-reloader.sh
#!/bin/bash
WATCH_DIR="/etc/supervisor/conf.d"

echo "[$(date)] Initial config sync on startup..."
supervisorctl reread
supervisorctl update

echo "[$(date)] Starting supervisord config watcher for $WATCH_DIR..."

# Monitor directory events. 
# K8s ConfigMap updates trigger a 'moved_to' event on the '..data' symlink directory.
inotifywait -m -e moved_to -e create "$WATCH_DIR" | while read -r path action file; do
    
    # [K8s Symlink Caveat & inotifywait Impact]
    # K8s updates ConfigMaps atomically by swapping the '..data' symlink pointing to the timestamped directory.
    # Individual file 'modify' events cannot be detected directly due to this symlink swap.
    # Therefore, we filter specifically for the '..data' directory event to ensure reliable detection.
    if [ "$file" = "..data" ]; then
        echo "[$(date)] ConfigMap update detected via '..data' symlink swap."
        
        # Debounce/Stabilization wait to allow K8s to complete file swapping and cleanup safely
        sleep 1
        
        # Reload supervisord configurations and apply dynamic worker process changes (start/stop)
        echo "[$(date)] Executing supervisorctl update..."
        supervisorctl reread
        supervisorctl update
    fi
done
```

## Dockerfile

multi-sidecar Container
```Dockerfile
FROM alpine:latest

RUN apk add --no-cache \
    tini \
    supervisor \
    inotify-tools \
    bash

COPY --chmod=755 ./consumer /opt/consumer
COPY --chmod=755 ./config-reloader.sh /opt/config-reloader.sh

COPY ./supervisord.conf /etc/supervisord.conf

RUN mkdir -p /etc/workers /etc/supervisor/conf.d

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
```

## supervisord Configuration (supervisord.conf)

```supervisord.conf
[supervisord]
nodaemon=true               ; Run in foreground (essential for container)
user=root                   ; Run as root (or appropriate user)
logfile=/dev/stdout         ; Redirect supervisord logs to stdout for k8s
logfile_maxbytes=0          ; Disable log rotation since we use stdout
pidfile=/var/run/supervisord.pid

[supervisorctl]
serverurl=unix:///var/run/supervisor.sock

[unix_http_server]
file=/var/run/supervisor.sock

; Required to activate supervisorctl commands
[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

; -------------------------------------------------------------------------
; Core Component: Config Reloader (Our Shell Script)
; -------------------------------------------------------------------------
[program:config-reloader]
command=/bin/bash /opt/config-reloader.sh
autostart=true
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0

; -------------------------------------------------------------------------
; Dynamic Workers Inclusion
; /etc/supervisor/conf.d/ will be mounted via K8s ConfigMap
; -------------------------------------------------------------------------
[include]
files = /etc/supervisor/conf.d/*.conf
```

# Considerations

## K8s Recommendations
```
volumeMounts:
  - name: worker-configs
    mountPath: /etc/supervisor/conf.d
    readOnly: true # prevent accidental modification from within the container
```

### To Be Investigated
- Certificate Hot-Reloading:If certificates in `/etc/ssl/workers/` are updated, the Rust consumer must either detect the file change internally or the process must be restarted. Since `supervisord` only watches `.conf` files via `config-reloader.sh`, a change in a Secret (certificate) will not automatically trigger a worker restart unless the `.conf` file is also touched.
- Log Tagging: Consider implementing structured logging in the Rust binary to distinguish between different worker instances in centralized logging.