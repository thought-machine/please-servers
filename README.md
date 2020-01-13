Please Servers
==============

This repo contains a set of servers for use with [Please](https://github.com/thought-machine/please), primarily
for working with the [remote execution APIs](https://github.com/bazelbuild/remote-apis).

---
**These are not supported by Thought Machine; we are making them public for illustrative purposes only. Caveat usor!**
---

There are two different servers in this repo; Mettle and Elan. Together they form a complete
server-side implementation of the remote execution API. Both depend on specific features of
Google Cloud Platform (Cloud Pub/Sub and Cloud Storage); they can be configured to work without
for local testing but that has significant limitations (e.g. replication is impossible).


Mettle
------

Mettle is responsible for execution of build actions. It can be configured to run in two modes - an
API server or a worker node - and at least one of each is required for it to do anything useful.

The API server is the server that handles communication with clients (i.e. plz). It submits jobs to
the workers via Cloud Pub/Sub and receives their results back in the same way. The API servers can be
scaled for resiliency although each receives a copy of all build actions so scaling will not increase
capacity (but it should be fully capable of managing a large fleet of workers regardless).

The workers are typically a much larger fleet; they receive jobs over Pub/Sub and communicate results
back as they complete them. They also communicate directly to the CAS server (typically Elan, but
conceptually it can be any server compatible with the API) to retrieve & store blobs.

Pub/Sub topics and subscriptions must be created manually prior to starting the servers. There is
one topic for requests (API -> worker) with a single subscription, and a topic for responses
(worker -> API) with one subscription per API server.

It can also be configured in a "dual" mode for local testing where a single server performs both
roles using an in-memory queue. This is obviously not useful as a production setup.

The workers must be deployed into some appropriate environment with the tools they need; this should
mirror the environment plz clients are running in. Docker containers are one way of managing this,
but we don't provide a prebuilt one since we can't know what tools are needed.


Elan
----

Elan is an implementation of the Content Addressable Storage service (including the auxiliary
services like action cache, bytestream etc). It is designed to be backed by a GCS bucket and
hence has no concept of replication, garbage collection, etc.

For local development it can be configured to use either file or memory-based storage; neither
is useful in a production setup.


TLS
---

Both servers support TLS for their gRPC connections. However the SDK library does not currently allow
specifying a CA file so the certs used must be absolutely trusted by the client and are hence not easy
to set up for local development.


Future direction
----------------

We don't intend to maintain these servers in the long run; part of our original motivation for
using the remote execution API was to leverage existing servers and save effort writing and
maintaining our own. Hence we would like to use a public implementation once the ecosystem
is more mature.
