# spnagent

Component of the SPN (Service Provider Network) infrastructure: a secure, virtualized distribution network for enterprise applications. `spnagent` is the edge component that connects services to the network.

## Overview
`spnagent` runs alongside your services to facilitate communication with the `spnhub`. It establishes UDP-based tunnels to the hub, acting as either a **Provider** (exposing a service) or a **Consumer** (accessing a service).

## Key Roles
- **Service Provider**: Registers local services to the SPN.
- **Service Consumer**: Maps remote SPN services to local interfaces.
- **Sidecar Support**: Optimized for sidecar patterns in containerized environments.

## Configuration (Environment Variables)
- `SPN_HUB_URL`: URL/Address of the target `spnhub` server.
- `RUST_LOG`: Log level (e.g., `info`, `debug`).

## Public Interface
- **UDP (Outbound)**: Established tunnels to `spnhub` for service traffic.
- **Local Access**: Maps remote services to local loopback or specific interfaces as defined.

## Deployment
- **Binary**: Static `musl` binaries for both **Provider** and **Consumer** roles.
- **Container**:
  - **provider**: Standard `scratch-based` images for service registration.
  - **single-consumer**: Standard `scratch-based` images for simple consumer deployment.
  - **multi-consumer**: `supervisord-managed` images for dynamic multi-process consumer management.

## Multi-Consumer Features
The `multi-consumer` architecture uses `inotify` to watch for ConfigMap updates in Kubernetes, allowing you to add, remove, or reconfigure consumers at runtime by simply updating the mounted configuration files.
