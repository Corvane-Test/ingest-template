# Task

This service runs in production and is unreliable. You have 3 hours to make it production-ready. You won't finish everything, so prioritize, and tell us what you skipped and why. AI tools are allowed. Deliver your commits plus a WRITEUP.md.

This service ingests billable usage. Invoices are built from what it stores.

## Prerequisites

- Docker
- k3d
- kubectl
- Python 3.11 or newer

## Getting started

```sh
make up
make load
```

`make up` creates a local k3d cluster and deploys everything. `make load` runs
the load script and prints its report.

Grafana: http://localhost:3000

## Getting your copy

1. On this repository's page, click "Use this template", then "Create a new
   repository". Do not fork: a fork of a public repository is public.
2. Create it under your own account and set the visibility to **Private**.
3. In your new repository, open Settings, Collaborators, and add the GitHub
   account named in your invitation email. That is how we see your work.

## Submitting

Work on a branch. When you are done, open a pull request from that branch into
`main` in your private repository. The pull request is your delivery and tells
us you are finished, so open it once, at the end.

## Rules

- You inherited this service from another team. Its docs are in `README.md` and `docs/`.
- The upstream service belongs to a third party and cannot be changed.
- The load script defines how clients behave. You may add to it, but do not change its behavior.
- Everything else is yours to change: code, manifests, dashboards, and docs.
- Tell us the date you will deliver before you start.
