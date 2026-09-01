# Notes and Caveats

[简体中文](notes.zh-CN.md) | [Docs Index](README.md)

## Operational Notes
- This repository is intentionally narrow. Cross-module behavior should be assembled in applications, recipes, examples, or benchmark harnesses.
- Public APIs should remain Go-native and explicit; avoid runtime scanning, hidden global registries, and reflection-heavy behavior in hot paths.
- Treat network input as untrusted. Configure parser limits and return typed errors instead of panics.
- Keep benchmark claims tied to a concrete host, operating system, protocol, payload, concurrency, warmup, and repetition count.

## Security Notes

- Keep parser limits and timeout boundaries explicit when peers are untrusted.
- Do not log raw payloads, credentials, tokens, private keys, or full user-controlled identifiers from hot paths.
- Validate TLS, ALPN, proxy, and DNS behavior in the final application stack, because those policies cross module boundaries.

## Performance Notes

- Optimize only with measurements from the target scenario. Microbenchmarks identify candidates; pressure tests prove end-to-end impact.
- Preserve low-allocation ownership. Copy only when a module boundary or lifetime rule requires it.
- Keep throughput and latency reports separate, especially p99 latency under sustained concurrency.

## Release Notes

No stable release tag is assumed by this documentation. Consumers should pin `@dev` or a specific pseudo-version until a tagged release is published.

## Troubleshooting

- If a package appears missing in a multi-module checkout, run the module standalone with `GOWORK=off` before changing dependencies.
- If a platform feature is unavailable, check whether the module intentionally returns an unsupported error on that operating system.
- If a benchmark result changes, rerun with the same host, payload, concurrency, warmup, and repetition count before changing thresholds.
