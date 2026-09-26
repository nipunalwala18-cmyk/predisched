# Authentication, rate limits and quotas (F9)

With `auth.enabled: true`, every gRPC call must say who is calling. Clients present an API key or a
JWT and are held to a request rate and a quota of unfinished tasks; cluster nodes present a node key
and are never limited. TLS can be switched on for every server and channel. Everything is off by
default (`configs/local.yaml`), so local development needs no keys.

Built by `spec/prompts/09-workflows-auth.md`. Spec sections: §4 FR30, §6 F9, and the tech stack (jjwt).

## Credentials

The `authorization` header carries one of:

| Header | Checked against | Caller |
| --- | --- | --- |
| `ApiKey <key>` | SHA-256 of the key in `configs/clients.yaml` | that client |
| `ApiKey <node key>` | `nodeKeySha256` in `configs/clients.yaml` | a cluster node |
| `Bearer <jwt>` | HS256 signature with `$PREDISCHED_JWT_SECRET` (32+ bytes), not expired, subject a known client id | that client |

`AuthInterceptor` (in `predisched-common`) does the check on every server and puts the caller into the
gRPC `Context`; missing, unknown, forged or expired credentials end the call with `UNAUTHENTICATED`.
Workers run it with clients refused (`PERMISSION_DENIED`): only nodes may call a worker. JWTs are
refused when no secret is set.

Keys are stored only as hashes, so `configs/clients.yaml` can be committed. The dev keys in it
(`dev-key-1`, `dev-key-2`) and the dev node key file `configs/dev-node.key` are for local demos only;
anywhere else, set `PREDISCHED_NODE_KEY` (which takes precedence) and replace the clients file.

## Limits (`predisched-scheduler`, `auth` package)

- **Rate** (`RateLimiter`): a token bucket per client, `burst` tokens, refilled at `ratePerSecond`.
  Every call takes a token; an empty bucket ends the call with `RESOURCE_EXHAUSTED`. The clock is
  injected, so the test drives time. `ClientLimits` applies it as an interceptor that runs after
  authentication.
- **Quota** (`ClientLimits.reserve`): at most `maxQueued` unfinished tasks per client. A submit
  reserves one slot (a workflow reserves one per task), a finished task gives it back through the
  terminal-state listener, and a submit over the quota is answered `accepted=false` with the reason.
- **Nodes** are exempt from both, so heartbeats, elections and replication are never throttled.
- Every task records the submitting client (`TaskRecord.clientId`), for per-client fair share later (FR37).

```yaml
# configs/clients.yaml
nodeKeySha256: "8bbcb21b..."
clients:
  - id: dev-client-1          # key: dev-key-1
    keySha256: "1bcefe22..."
    ratePerSecond: 10
    burst: 10
    maxQueued: 200
```

## How nodes carry credentials

`NodeSecurity.install` runs at start-up in the scheduler and the worker. It installs the process-wide
`Transport` (`predisched-common`, `net` package), which every channel and server in the system is now
built through, and returns the interceptor. With auth on, the transport adds `ApiKey <node key>` to
every outgoing call, so the election, replication, dispatch, heartbeat and clock traffic all
authenticate without any of that code knowing about auth. The CLI installs a transport with the
client's key instead (`--api-key` or `PREDISCHED_API_KEY`, `--token` or `PREDISCHED_TOKEN`).

## TLS (optional, off by default)

```yaml
tls:
  enabled: true
  certChain: certs/node.crt     # PEM chain this node serves
  privateKey: certs/node.key    # PEM, PKCS#8
  trustCert: certs/ca.crt       # CA every channel trusts
```

With `tls.enabled`, `Transport.server` uses `TlsServerCredentials` and `Transport.channel` uses
`TlsChannelCredentials` trusting `trustCert`; the CLI connects over TLS with `--tls-trust ca.crt`.
A self-signed pair for local use:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" -keyout certs/node.key -out certs/node.crt
```

and `trustCert: certs/node.crt`. The TLS path is not covered by an automated test yet; the
acceptance checks below ran in plaintext.

## Real output

Scheduler and worker started with `configs/secure.yaml` (auth on, TLS off):

```
$ predisched --api-key wrong submit --type SLEEP_TASK --input ms=10 --priority 5
submit failed: UNAUTHENTICATED: unknown API key
$ predisched submit --type SLEEP_TASK --input ms=10 --priority 5
submit failed: UNAUTHENTICATED: missing credentials: send 'authorization: ApiKey <key>' or 'Bearer <jwt>'
$ predisched --api-key dev-key-1 submit --type SLEEP_TASK --input ms=10 --priority 5 --repeat 50
50 submits in 2642 ms:
  37 RESOURCE_EXHAUSTED
  13 accepted
```

`dev-key-1` allows 10 requests/s with a burst of 10. The 50 submits took 2.6 s rather than one
second (the first calls include JVM and connection warm-up), so 13 got through: the burst of 10 plus
about three tokens refilled during the run. The workflow demo in `workflows.md` ran in the same
cluster, so dispatch to the worker authenticated with the node key.

## Tests

`AuthInterceptorTest`: a valid key passes; a wrong key and a missing key are `UNAUTHENTICATED`; a
fresh JWT passes, an expired one is refused as `expired`, one signed with another secret is refused;
a node-only server accepts the node key and refuses a client key with `PERMISSION_DENIED`.
`RateLimiterTest`: a burst of 5 at 2/s is allowed, the sixth refused, half a second refills one token,
a long idle refills only up to the burst, clients have separate buckets; the quota allows 3 unfinished
tasks, refuses the fourth, and allows one again after a release.
