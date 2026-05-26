# spnagent

Component of the SPN (Service Provider Network) infrastructure: a secure, virtualized distribution network for enterprise applications. `spnagent` is the edge component that connects services to the network.

## Overview
`spnagent` runs alongside your services to facilitate communication with the `spnhub`. It establishes UDP-based tunnels to the hub, acting as either a **Provider** (exposing a service) or a **Consumer** (accessing a service).

## Key Roles
- **Service Provider**: Registers local services to the SPN.
- **Service Consumer**: Maps remote SPN services to local interfaces.
- **Sidecar Support**: Optimized for sidecar patterns in containerized environments.

## Configuration (Environment Variables)
- `SPN_HUB_HOSTNAME`: Hostname of the target `spnhub` server.
- `SPN_HUB_PORT`: Port of the target `spnhub` server (default: `4433`).
- `SPN_AGENT_TRUST_CERTIFICATE_ROOT`: Path to the CA certificate file (PEM) used to verify the hub's identity.
- `SPN_AGENT_CLIENT_CERTIFICATE`: Path to the client certificate file (PEM format) for mTLS authentication.
- `SPN_AGENT_CLIENT_CERTIFICATE_KEY`: Path to the client private key file.
- `RUST_LOG`: Log level (e.g., `error`, `warn`, `info`, `debug`).
- **Provider (Service Registration)**:
  - `FORWARD_ADDRESS`: The local address and port of the backend service to be exposed (e.g., `127.0.0.1:8080`).
- **Consumer (Service Mapping)**:
  - `BIND_ADDRESS`: The local address and port to bind for providing access to the remote service (e.g., `127.0.0.1:9090`).

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
