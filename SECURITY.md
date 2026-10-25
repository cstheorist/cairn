# Security policy

## Status and supported versions

Cairn is a research prototype. Only the latest commit on `main` receives fixes.
PostgreSQL password authentication currently has no TLS, and the supplied
credentials are for loopback development deployments. See
[operations](docs/operations.md) for the current security boundary.

## Reporting a vulnerability

Report security issues and correctness bugs that could corrupt data through
[GitHub private vulnerability reporting](https://github.com/cstheorist/cairn/security/advisories/new).
Include the commit, topology, reproduction and expected impact. Avoid publishing
credentials or sensitive data in a public issue.

Ordinary compatibility bugs with a safe reproduction can use the public issue
tracker. Durability, atomicity, replay and isolation failures receive the same
attention as security bugs.
