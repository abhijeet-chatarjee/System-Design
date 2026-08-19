# Design WhatsApp — System Design Interview Guide

A unified revision doc covering a real-time 1:1 and group chat system at WhatsApp scale. Synthesized from [System Design Interview: Design WhatsApp (Part 1)](https://newsletter.systemdesign.one/p/whatsapp-system-design) and [Design a Chat App System (Part 2)](https://newsletter.systemdesign.one/p/design-a-chat-system), plus the standard interview deep-dives those parts outline (presence, offline delivery, sharding, reliability, multi-region, abuse).

Use this as a 45-minute talk track: frame the problem, draw the box diagram, then go deep on the paths that actually fail in production.

---

## 0. How to run the interview (45 minutes)

| Minutes | What you do | What they are scoring |
| --- | --- | --- |
| 0–5 | Clarify functional + non-functional requirements. Cut scope out loud. | Do you know what matters? |
| 5–10 | Back-of-envelope numbers. State assumptions. | Can you size the system? |
| 10–18 | High-level architecture + protocol choice. | Can you draw a working system? |
| 18–28 | Online path, ACK, offline inbox, presence. | Do you understand the hard path? |
| 28–36 | Horizontal scale: Redis Pub/Sub, Cassandra sharding, groups. | Does it survive a billion users? |
| 36–45 | E2E encryption, failures, multi-device, follow-ups. | Senior judgment and tradeoffs |

**Say this up front:** “I’ll design 1:1 and small groups first, with guaranteed delivery and <500ms online latency. I’ll treat media, E2E, and multi-region as explicit follow-ups so we don’t skip the message path.”

**The trap most people miss:** designing only for two online users. Real chat is mostly disconnected mobile clients, retries, and duplicate delivery.

---

## 1. Requirements

### Functional

- 1:1 text messages
- Group chats, **cap at ~100 members** in v1 (fan-out cost is the reason)
- Offline users: queue undelivered messages for **30 days**
- Media: images, video, audio (not through chat servers)
- Status: sent → delivered → read
- Presence: online / offline / last seen
- Multi-device: phone + laptop (mention; deep-dive if asked)
- Push notifications when the app is backgrounded or offline

### Non-functional

- Online delivery **p99 < 500ms**
- **At-least-once** delivery, client-idempotent (messages never silently disappear)
- Billions of users, hundreds of thousands of messages/sec at peak
- Servers are not the source of truth for chat history — devices are. Server stores undelivered + optional cloud backup
- Survive single-component failure
- End-to-end encryption (WhatsApp’s defining constraint — call it out even if you defer the crypto)

### Explicitly out of scope unless they ask

- Channels / broadcast lists with 100k members
- Voice/video calls (WebRTC is a different design)
- Status / stories
- Payments
- Full-text search of ciphertext (you cannot do this with true E2E)

---

## 2. Capacity estimation

State assumptions out loud. Interviewers care about the method, not the exact number.

### Users

| Metric | Value |
| --- | --- |
| Registered users | 1B |
| Daily active users | 500M |
| Peak concurrent WebSocket connections | 50M |
| Messages per DAU per day | 20 |

### Throughput

```
Daily messages     = 500M × 20 = 10B / day
Average            = 10B / 86400 ≈ 115,000 msg/s
Peak (3–5×)        = 350,000–500,000 msg/s
```

Group fan-out multiplies **delivery** QPS, not ingest QPS. If 10% of messages are groups of size 20, delivery QPS ≈ ingest × (0.9×1 + 0.1×20) ≈ 2.9× ingest.

### Storage

Assume ~1 KB per message including metadata (ciphertext + ids + timestamps + status).

```
Raw daily write    = 10B × 1KB = 10 TB/day
Naive annual       = ~3.6 PB/year
```

Do **not** provision 3.6 PB of hot storage. Most messages are delivered in seconds and deleted from the server. Active store ≈

- 30-day undelivered inbox
- small fraction of users with cloud history (~10%)

**Working number: 400–500 TB active**, not petabytes.

### Connections and bandwidth

50M idle-ish WebSockets. Heartbeats are tiny. WhatsApp’s historical lesson: don’t send what you don’t need. If you naively assume 10 KB/s per connection you get 500 GB/s — that is the **upper bound you must not hit**, not the design target.

### Chat-server fleet (sanity check)

If one process holds ~200k connections:

```
50M / 200k = 250 chat servers
```

Round to ~300–400 for headroom, rolling deploys, and regional spread.

### What to write on the whiteboard

- 1B users, 50M concurrent
- ~100k avg / ~500k peak msg/s ingest
- ~1 KB/msg, ~10 TB/day raw, ~0.5 PB hot
- Hundreds of sticky WebSocket servers

---

## 3. High-level architecture

```
                    ┌──────────────┐
   media upload     │ Blob store   │◄──── signed URL (S3 / GCS)
   media download   │ + CDN        │
                    └──────────────┘
                           ▲
                           │ REST (not chat path)
┌────────┐   WSS    ┌──────────────┐    sticky     ┌──────────────┐
│ Client ├─────────►│ L4 load      ├──────────────►│ Chat servers │
│ (local │          │ balancer     │               │ (WebSocket)  │
│  SQLite)│◄────────┤ + health     │               └──────┬───────┘
└────────┘  push    └──────────────┘                      │
     ▲                                                    │
     │ APNs / FCM                                         │
┌────┴─────────┐     ┌──────────────┐    pub/sub          │
│ Notification │◄────┤ Presence +   │◄────────────────────┤
│ service      │     │ connection   │    routing          │
└──────────────┘     │ cache (Redis)│                     │
                     └──────────────┘                     │
                                                          │
                     ┌──────────────┐   durable log       │
                     │ Kafka        │◄────────────────────┘
                     │ (messages)   │
                     └──────┬───────┘
                            │
                     ┌──────▼───────┐
                     │ Message      │  batch writes
                     │ storage svc  ├──────────────► Cassandra / DynamoDB
                     └──────────────┘                (inbox + metadata)
```

### Why each box exists

| Component | Job | If you skip it |
| --- | --- | --- |
| Client local DB | Source of truth for history | Server becomes a petabyte chat archive |
| L4 LB + sticky sessions | Hold WebSockets on one box | Connections bounce; you lose in-memory session state |
| Chat servers | Long-lived sockets, ACK, route | You are building email |
| Redis connection map | `user → {server, device, last_seen}` | Cross-server delivery needs a linear scan |
| Redis Pub/Sub | Deliver to whichever server holds B | Chat servers must mesh with each other |
| Kafka | Durable ingest, decouple ACK from DB | A slow Cassandra stalls every sender |
| Storage service | Batch, retry, index, TTL | Chat servers wait on disk |
| Cassandra | High write ingest, query by inbox/conversation | Postgres dies at 500k writes/s |
| Blob + CDN | Media out of the hot path | Chat servers melt on video |
| Notification service | Wake offline devices | Offline users see nothing until they open the app |
| Presence service | Heartbeats, last seen, fan-out to watchers | Chat servers become a presence monolith |
| Service registry (Consul) | Ephemeral chat boxes | LB routes to dead IPs |

### Single-server → scaled path (tell this story)

1. One chat process + Postgres. Works for thousands.
2. Split connections (chat) from durability (queue + DB).
3. Many chat servers → **routing problem** (A and B on different boxes).
4. Redis Pub/Sub for online fan-out; Kafka + Cassandra for durability.
5. Media leaves the chat cluster entirely.

---

## 4. Why WebSockets (and why not the alternatives)

| Approach | How it works | Why it loses at this scale |
| --- | --- | --- |
| Short polling | Client asks every 2s | Wasted QPS, +2s latency, battery death |
| Long polling | Hold HTTP until event or timeout | Handshake per message; servers babysit HTTP lifecycles |
| SSE | Server → client stream | Fine for one-way; you still need POST for sends |
| WebSocket | One persistent bidirectional channel | **This is the choice** |

**Why WebSocket wins:** connection setup cost is paid once. After that, User A’s send is a frame to server 1, a Pub/Sub hop, a frame out server 2. No extra HTTP round trips.

**Tradeoffs you should name:**

- Need **L4 / WebSocket-aware** load balancing, not naive L7 HTTP LB
- Memory per idle connection (socket + buffers). Heartbeat to kill zombies
- Sticky sessions: once A is on `chat-7`, keep A on `chat-7`
- Mobile OS will freeze the app; you still need **push** as a second path

### Idle connections

- Client ping every **30s**, server pong
- No heartbeat for **60s** → close, drop from Redis, mark last seen
- This is presence **and** connection GC

REST remains for: auth bootstrap, media signed URLs, profile, key bundle fetch, pagination of history.

---

## 5. Service discovery

Chat servers are cattle. They crash, autoscale, and deploy.

**Pattern:** Consul / Cloud Map.

1. Chat server boots on `10.0.1.45:8080`
2. Registers `{name: chat-server, id, /health, region, capacity}`
3. Heartbeat every 10s; 30s silence → unhealthy
4. LB watches the healthy set and stops sending new connections to the dead

Chat servers also discover Redis, Kafka, and storage — never hardcode IPs.

**Tradeoff:** you now operate a registry cluster. The alternative is outages from stale configs.

When a chat server dies: in-memory sockets die with it. Clients reconnect (exponential backoff), land on a new box, re-auth, drain inbox. **Do not** try to live-migrate WebSockets.

---

## 6. Data models

Split stores by access pattern. Do not put everything in one SQL database.

### Users — PostgreSQL / user service

```
users
  user_id PK
  phone_hash
  display_name
  created_at
  identity_key_pub          -- E2E public identity
  prekeys[]                 -- signed + one-time prekeys
```

### Groups — SQL or a group service

```
groups
  group_id PK
  title
  created_by
  created_at

group_members
  group_id
  user_id
  role                    -- admin | member
  joined_at
  PRIMARY KEY (group_id, user_id)
```

Member list of 100 fits in one query. Cache hot groups in Redis.

### Messages — Cassandra / DynamoDB (write-heavy)

Partition for **inbox / conversation fetch**, not for global scans.

```
messages
  PK: (conversation_id, message_id)
  sender_id
  recipient_ids           -- or a per-recipient inbox table
  ciphertext
  media_id nullable
  client_message_id       -- idempotency
  sent_at
  expires_at              -- 30d for undelivered
  status                  -- optional; often a side table
```

**Two valid inbox shapes — pick one and defend it:**

1. **Conversation partition:** `PK = conversation_id`, clustering `message_id` descending. Great for opening a chat. Extra work to find “all undelivered for user B”.
2. **Per-user inbox (WhatsApp-like):** `PK = recipient_id`, clustering `message_id`. Great for reconnect drain. Group send writes N inbox rows (fan-out on write).

For v1 with groups ≤100, **fan-out on write into per-user inboxes** is the interview-friendly choice.

### Connection registry — Redis

```
conn:{user_id} → {
  devices: [{ device_id, server_id, platform, connected_at }],
  last_seen,
  status
}
```

TTL slightly above heartbeat interval so crashes self-heal.

### Online inbox buffer — Redis list (optional)

Hot undelivered for users who flick in and out: `inbox:{user_id}` list with cap + Cassandra as source of truth.

### Message IDs

Use a **server snowflake** (time + shard + seq) so IDs sort roughly by time. Always also keep `client_message_id` for retries.

---

## 7. APIs and message envelopes

WebSocket JSON (or protobuf in production). Every client payload carries `client_message_id`.

### Auth (WSS handshake)

```json
{ "type": "auth", "payload": {
  "user_id": "123", "auth_token": "...",
  "device_id": "phone-abc", "platform": "ios", "app_version": "2.24.5"
}}
```

```json
{ "type": "auth_success", "payload": {
  "session_id": "sess_...", "server_time": 1699189200000, "unread_count": 47
}}
```

Clock: send `server_time` so clients can correct skew.

### Send

```json
{ "type": "message", "client_message_id": "client_abc123",
  "payload": {
    "receiver_id": "456", "content": "<ciphertext>",
    "media_id": null, "reply_to": null, "timestamp": 1699189200000
}}
```

```json
{ "type": "message_ack", "client_message_id": "client_abc123",
  "payload": { "message_id": "20241105120000000001", "status": "sent", "timestamp": 1699189200123 }}
```

`sent` means **the server has accepted durability responsibility**, not that B has seen it.

### Receive + delivery ACK

Server → B: `type: message` with `message_id`.
B → server: `type: delivery_ack`.
Server → A: status update `delivered`.

### Read receipts

```json
{ "type": "read_receipt", "message_ids": ["..."], "timestamp": 1699189260000 }
```

Fan this back only to the sender’s devices (privacy + cost).

### Heartbeat

Client every 30s (article sample uses 5s — say 30s to save battery, 5s if they want snappier last-seen).

### Media (REST)

`POST /api/media/upload` → `{ upload_url, media_id, expires_in }`
Client PUTs bytes to object storage. Chat message then references `media_id` (and the encryption key inside E2E ciphertext).

### Groups

`type: group_message` with `group_id`. Server expands membership and fans out.

---

## 8. Online 1:1 path (the happy path)

```
A                    chat-1                 Redis                 chat-2                 B
│  message              │                    │                     │                    │
│──────────────────────►│                    │                     │                    │
│                       │ persist to Kafka   │                     │                    │
│                       │ (async)            │                     │                    │
│  message_ack/sent     │                    │                     │                    │
│◄──────────────────────│                    │                     │                    │
│                       │ GET conn:B         │                     │                    │
│                       │───────────────────►│                     │                    │
│                       │ B on chat-2        │                     │                    │
│                       │ PUBLISH user:B     │                     │                    │
│                       │───────────────────►│  message            │                    │
│                       │                    │────────────────────►│  WS frame          │
│                       │                    │                     │───────────────────►│
│                       │                    │                     │  delivery_ack      │
│                       │                    │                     │◄───────────────────│
│  status=delivered     │                    │                     │                    │
│◄──────────────────────│◄───────────────────┼─────────────────────│                    │
```

**Order of operations (say this):**

1. Authenticate and validate (size, rate limit, blocked users).
2. Assign `message_id`.
3. **ACK to A quickly** after the message is in Kafka (or after a local write-ahead log). Do not wait for Cassandra.
4. Look up B in Redis.
5. If online: Pub/Sub → B’s chat server → WebSocket.
6. If offline: inbox + push (next section).
7. Storage service writes Cassandra asynchronously.
8. Delivery/read receipts are separate events, also durable.

**Latency budget against the 500ms SLO**

| Hop | Budget |
| --- | --- |
| Device encrypt | 5–10ms |
| A → edge/server | 50–150ms |
| Chat server + Redis lookup/publish | 5–15ms |
| Server → B | 50–150ms |
| Device decrypt + render | 5–10ms |
| **Typical total** | **~150–300ms** |

Kafka + Cassandra are **not** on the online latency path.

---

## 9. Offline path, inbox, and push

If `conn:{B}` is missing or heartbeat expired:

1. Write the message to B’s durable inbox (Cassandra), TTL 30 days.
2. Notification service sends APNs/FCM. **Payload is opaque** under E2E (“New message”), not the plaintext.
3. B’s device wakes, opens a socket, auths.
4. Server streams pending inbox (paginated).
5. B ACKs `message_id`s.
6. Server deletes those inbox rows (or marks delivered and lets TTL finish).

**Inbox drain details interviewers like:**

- Bound the first fetch (e.g. 500 messages), then paginate
- Use `last_message_id` cursor, not offset
- Same `client_message_id` / `message_id` idempotency as live path
- If ACK is lost, redeliver; client dedups
- After 30 days, drop. Say this explicitly — “guaranteed delivery” is not “forever”

**Push is best-effort.** The inbox is the guarantee. Never make APNs the only copy of a message.

---

## 10. Presence and last seen

Dedicated **presence service**, not ad-hoc flags on chat servers.

- Heartbeats update Redis: `status=online`, `last_seen=now`, `server_id`
- Missed heartbeat → `offline` and freeze `last_seen`
- **Grace period** (30–60s) so flaky mobile networks don’t flicker
- Do **not** broadcast presence to all 1B users

**Who gets updates:** subscribers — people with the chat open, or a bounded contact set that opted into last seen.

Implementation: Redis Pub/Sub channel `presence:{user_id}` plus a server-side watch list. When A opens a chat with B, A’s chat server subscribes to B’s presence for that session.

**Privacy:** last seen and read receipts are user settings. The system must honor “nobody” / “contacts” / “nobody except…” — this is a product rule, not a cache optimization.

---

## 11. Sent, delivered, read

| Status | Meaning | Trigger |
| --- | --- | --- |
| Pending (client) | On device, not ACK’d | Local insert |
| Sent (1 check) | Server durable | `message_ack` |
| Delivered (2 checks) | At least one of B’s devices ACK’d receive | `delivery_ack` |
| Read (blue ticks) | B opened the chat | `read_receipt` |

Store receipts as events, not as a hot update on every message row if that becomes a write storm. A compact `receipts` table keyed by `(conversation_id, user_id, last_read_id)` is enough to render ticks.

Multi-device: delivered if **any** device got it; read if the user opened it on any device (product choice — state it).

---

## 12. Groups (why 100 members)

**Fan-out on write:** one send → N inbox writes + N online publishes.

```
Cost ≈ O(members)
100 members is a deliberate cap: 500k ingest/s × 100 would be apocalyptic
if every message were a max-size group. In reality mix is small.
```

**Algorithm**

1. Authz: sender ∈ group
2. Load member list (cache)
3. Persist one canonical message + N inbox copies **or** N inbox copies only
4. For each online member/device: Pub/Sub
5. For each offline member: leave in inbox + push
6. Exclude sender’s sending device; still fan out to sender’s other devices

**Sender keys (E2E, see §17):** encrypt once with a group sender key, not N times with N pairwise session keys. Distribution of the sender key is pairwise; the chat payload is not.

**If they raise the cap to 1k–10k:** switch to fan-out on read / hybrid (store once per group, members pull; or multicast trees). Don’t pretend fan-out on write still works.

---

## 13. Multi-device sync

Treat **device**, not user, as the connection grain.

```
conn:{user_id}.devices = [
  { device_id: phone,  server_id: chat-2 },
  { device_id: laptop, server_id: chat-9 }
]
```

- Publish to **every** device channel
- Each device has its own Signal session (or a linked device identity)
- Message sent from laptop must appear on phone: fan-out includes sibling devices
- Unread counts can be per-device until a read receipt unifies them

**WhatsApp evolution (good talking point):** originally the phone was the primary and companions were linked. Multi-device without the phone online required pairing, per-device keys, and sender-key distribution to all devices. In an interview, “session per device + fan-out to all of B’s devices” is enough.

---

## 14. Media without killing chat servers

Never stream 8 MB videos through WebSocket workers.

1. Client: `POST /api/media/upload` with type + size + conversation_id
2. API: authz, virus/size limits, return **time-limited signed PUT URL**
3. Client encrypts media locally (E2E), PUTs ciphertext to S3
4. Chat message carries `media_id` + media key inside the encrypted envelope
5. Receiver gets the message, then GETs via CDN/signed URL, decrypts on device
6. Optional: thumbnail as a second small blob for chat list

**Why this works:** chat servers handle ~1 KB control messages; object storage + CDN handle GB/s of blobs; chat WS memory stays for sockets.

Retention: unlink blob when no remaining references; lifecycle rules on the bucket.

---

## 15. Scaling chat servers: how A reaches B

A is on server 1, B on server 2. This is the core scaling question.

### Option 1 — Hope the LB helps

Fails. LB only terminates A’s connection. Server 1 has no socket to B.

### Option 2 — Kafka topic per user

Fails at 1B users. ~50 KB overhead per topic → tens of TB of **metadata**. Kafka wants thousands of fat topics, not billions of tiny ones.

### Option 3 — Consistent hashing (user → server)

Pros: direct server-to-server, no broker.
Cons: all-to-all TCP mesh at 1k servers; rehash during scale-out drops or misroutes; you keep servers huge to keep N small.

Use hashing for **data** sharding, not for live sockets.

### Option 4 — Redis Pub/Sub (the choice)

```
B connects to chat-2  →  chat-2 SUBSCRIBE user:B
A sends to B          →  chat-1 PUBLISH user:B
Redis                 →  chat-2  →  B's WebSocket
```

- Few bytes per channel vs Kafka’s 50 KB/topic
- Chat servers never know each other
- Add/remove servers with zero mesh rewiring
- **At-most-once** is OK: durability is Kafka + DB. Pub/Sub is only the live hint. If B is offline or the publish misses, inbox still has the message

**Cost:** a few extra ms; you must run Redis clustered and highly available. Split **Pub/Sub** from **cache** if a noisy neighbor is a concern (two clusters).

**Sharded channels:** `user:{id}` for 1:1, `group:{id}` only if you want group multicast; at 100 members, publishing N user channels is simpler and matches per-device routing.

---

## 16. Message storage service

Chat server: **accept + ACK + live route**. Storage service: **durable write**.

```
Kafka  →  storage workers (stateless)  →  Cassandra
              ↳ conversation index
              ↳ analytics topic
              ↳ DLQ
```

### Pipeline

1. Each worker owns some Kafka partitions; poll ~100ms, batches of hundreds
2. Validate schema, size (text ciphertext < ~4 KB)
3. Hash for dedup; attach indexes
4. Buffer until **1000 messages or 5s**, group by shard, batch insert
5. **Why batch:** 1000 × 5ms serial inserts = 5s; one batch ≈ 50ms (~100×)
6. Update conversation preview / unread (or emit to a side topic)
7. Failures:
   - transient → retry with exponential backoff
   - poison → DLQ
   - duplicate key → skip
8. Commit Kafka offset **after** successful write
9. Crash before commit → replay; `message_id` / `client_message_id` makes it safe

### Scale

Workers are stateless. Add instances; Kafka rebalances partitions. No coordination.

### Alerts

| Metric | Page if |
| --- | --- |
| Consumer lag | > 10k messages (or lag time > 30s) |
| Batch write p99 | > 500ms |
| Error rate | > 0.1% |
| Throughput | pinned at stall; scale workers |

**Tradeoff:** extra durability latency vs Kafka. User-facing ACK does not wait for this.

---

## 17. Database sharding and replication

### Partitioning

- **Inbox table:** partition key `recipient_id` (or `recipient_id % N` + clustering `message_id`)
- **Conversation index:** `conversation_id` → last_message, preview, updated_at
- Avoid partitioning on time alone (hot partitions at “now”)
- Snowflake `message_id` gives time-ordered clustering inside a partition

### Hot users / hot groups

A celebrity group of 100 is fine. A 10M-follower “chat” is a channel — different design. For a hot 1:1 (two bots talking), local buffers + batching absorb it.

### Replication

- RF=3 in Cassandra, quorum writes/reads for inbox drain
- Or DynamoDB with the same idea: PK = recipient, SK = message_id
- Chat **history on device** means you can be slightly looser on multi-region history than on **undelivered inbox** (losing inbox is data loss)

### SQL vs NoSQL (say it)

| Store | Use |
| --- | --- |
| Postgres | users, groups, authz, billing |
| Cassandra/Dynamo | messages, inbox, high write QPS |
| Redis | connections, presence, rate limits, Pub/Sub |
| S3 | media ciphertext |
| Elasticsearch | **only if you are not E2E**, or search metadata/sender — not body |

---

## 18. Dedup, ordering, retries, edge cases

### Delivery semantics

- Network is at-least-once
- **Exactly-once is a lie** across mobile clients
- Build **at-least-once + idempotent consumers**

Idempotency key: `(sender_id, device_id, client_message_id)`.

Client retries on no ACK (exponential backoff, same id). Server returns the original `message_id`.

### Ordering

- Total global order: not required
- Per-conversation, per-sender monotonic `message_id` / seq: yes
- Concurrent sends from A and B: interleaving by server time is fine
- Don’t block A→B on C→B

### Failure matrix (memorize)

| Failure | What happens |
| --- | --- |
| Chat server dies | Clients reconnect, drain inbox. In-flight un-ACK’d sends retry from client |
| Kafka unavailable | Stop ACK’ing new sends (fail closed) or spill to local WAL with backpressure |
| Cassandra slow | Online path still works; lag alert; don’t block WS ACK if Kafka has the record |
| Redis down | Live routing degrades; **fall back to inbox-only** (treat everyone as offline). Worse latency, no loss |
| Push provider down | Inbox still full; user sees messages on next open |
| Duplicate delivery | Client drops duplicate `message_id` |
| Clock skew | Server timestamps win; snowflake ids |
| Partial group fan-out | Per-recipient inbox writes are independent; retry missing members; don’t fail the whole group after ACK to sender if durability of canonical copy succeeded — **be explicit about this product choice** |
| User blocked | Check block list **before** persist |
| App backgrounded | OS kills WS; presence goes offline; push takes over |

### Exactly-once ACK hole

If server writes Kafka then crashes before `message_ack`, client retries. Dedup saves you. If ACK is sent before Kafka, you can **lie** to A and then lose the message — **never ACK before durability**.

---

## 19. End-to-end encryption (Signal protocol)

WhatsApp: ciphertext on the wire and at rest. Servers route blobs they cannot read.

### Properties to name

- E2E confidentiality
- **Forward secrecy:** compromising today’s key doesn’t decrypt yesterday
- **Future secrecy / break-in recovery:** compromising today’s key doesn’t decrypt tomorrow
- Replay protection via counters

### Key inventory

| Key | Lifetime | Where |
| --- | --- | --- |
| Identity key pair | Long-term | Private on device; public on server |
| Signed prekeys | Medium; rotated | Server stores public bundle |
| One-time prekeys | Single use | Server hands out and deletes |
| Ephemeral / ratchet keys | Per message | Device only |

### X3DH session setup (A messages B the first time)

1. A fetches B’s identity key, signed prekey, and one one-time prekey from the **key server** (plaintext metadata; this is allowed)
2. A generates an ephemeral key, runs DH combinations, derives a session
3. A sends ciphertext + its ephemeral public key
4. B reconstructs the same session, decrypts, starts the ratchet

### Per-message

```
ciphertext = AES-256(plaintext, message_key)
mac        = HMAC-SHA256(ciphertext, mac_key)
message_key derived from chain key + counter
```

Ratchet: after a message, derive a new chain key, drop the old one.

### Multi-device and groups

- **1:1:** session **per device**. A encrypts once per B-device (and for A’s other devices so the laptop sees what the phone sent)
- **Groups:** **sender key** — A distributes a group sender key pairwise to each member/device, then encrypts the payload once per message with that sender key. Rotation when members leave

### Interview tradeoffs (this is the point)

You **cannot**: server-side search, useful push previews, server-side spam content classification, or “restore my chat on a new phone from our DB” without a separate encrypted backup key.

You **can**: store ciphertext, route, rate-limit on metadata (sender, size, fan-out), and take abuse reports.

Key server availability is now on the critical path for **first** message to a new user. Cache prekey bundles at the edge.

---

## 20. Rate limiting and abuse

E2E means you police **behavior**, not words.

| Limit | Example | Where |
| --- | --- | --- |
| Messages / user / minute | 30–60 | Redis token bucket on chat server |
| New chats / day | anti-spam | User service |
| Group creates / day | | Groups API |
| Max media size | 16–64 MB | Upload API |
| Connections / user | a handful of devices | Auth |
| Fan-out cost | groups ≤100 | Product cap |

Bucket key: `rl:{user_id}:{action}`. Return a first-class error; don’t drop silently.

**Graph / reputation:** new numbers, burst fan-out to non-contacts, report rate. Ban or delay. Content inspection is not available.

**Authn:** short-lived tokens, device binding, drop sockets on logout/password change.

---

## 21. Scheduled jobs and cleanup

- TTL / compaction on inbox rows older than 30 days
- Media GC for unreferenced blobs
- Prekey refill: notify clients when one-time prekeys run low
- Cassandra tombstone / compaction watch
- Conversation index repair
- Drop zombie Redis keys (TTL should already do this)
- Analytics rollups off the hot path (separate Kafka topic)

---

## 22. Multi-region

Users are global; light is not.

**Practical v1:**

- Pin a user to a **home region** (account metadata)
- Chat servers + Redis + Kafka + inbox in that region
- Edge POP terminates TLS and forwards to home
- Cross-region 1:1 (A in US, B in IN): forward the payload to B’s home region Kafka/Pub/Sub; don’t require all chat servers worldwide in one mesh

**Data:**

- Inbox must be in the region that will drain it (B’s home)
- Media in a global bucket with CDN
- Identity/prekeys globally readable, strongly consistent on rotate

**Failover:** regional disaster → promote replica, accept delayed last-seen and a reconnect storm. State the RPO: undelivered messages need async replication or you will lose inbox on region death — this is the real multi-region cost.

---

## 23. Observability and SLOs

| SLO | Target | Signal |
| --- | --- | --- |
| Online send-to-receive | p99 < 500ms | client traces + server timestamps |
| ACK latency | p99 < 100ms | chat server |
| Inbox drain on reconnect | p99 < 2s for ≤500 msgs | storage |
| Consumer lag | < 30s | Kafka |
| Connection success | > 99.9% | LB + chat |
| Push send | best effort; track separately | APNs/FCM errors |

Also: WS count per server (capacity), Redis pub/sub rate, disk on Cassandra, DLQ depth, rate-limit rejects, unique `message_id` vs duplicates (retry health).

Log **ids not bodies**. Ciphertext in logs is still user data; prefer `message_id`, `conversation_id`, sizes.

---

## 24. Schema and client versioning

- Envelope `version` field
- Servers ignore unknown fields; clients must
- Additive changes only during rolling deploys
- WebSocket subprotocol or `app_version` from auth for gated features
- Key bundles and media URLs versioned independently of chat payload

---

## 25. What to draw, in order

1. Clients with local DB
2. LB + sticky chat servers
3. Redis (conn + pub/sub + presence)
4. Kafka → storage → Cassandra inbox
5. Push + blob/CDN on the side
6. Then zoom into A→B sequence with ACK **before** DB, **after** Kafka
7. Then “B offline” branch
8. Then “A and B on different servers” + Pub/Sub
9. Then group fan-out and why 100
10. Then E2E if there’s time

---

## 26. Follow-up questions (have a 30-second answer)

**How do you search messages?** You don’t, server-side, under E2E. Search on device. Optional encrypted cloud backup with a user key.

**Read receipts in groups?** Per-member last-read cursor. UI usually “seen by 12” not N ticks.

**Message edit/delete?** New event type referencing `message_id`; tombstone; fan-out the same path. Delete-for-everyone is a signed event, not a silent DB hide.

**Disappearing messages?** `expires_at` on the envelope; clients honor it; server TTL as a backstop for inbox.

**Calls?** Separate: signaling over WS, media over WebRTC, not Cassandra.

**Moderation?** Reports, metadata, graph. No plaintext.

**Why not gRPC streams?** Viable; WebSocket is the lingua franca for browsers and the expected interview answer. Same architecture.

**Kafka vs Pulsar vs Kinesis?** Durable partitioned log. Don’t bikeshed unless they care.

**Would you store messages in Postgres?** Not at 500k writes/s. Maybe partitioned Postgres at much smaller scale.

---

## 27. One-page cheat sheet

```
REQ     1:1 + groups≤100, offline 30d, ticks, presence, media, E2E
NUMBERS 500M DAU, 50M sockets, 115k/s avg, 500k/s peak, 10TB/day raw, ~0.5PB hot
PROTO   WebSocket + sticky L4; REST for media/keys; push for wake-ups
ACK     After Kafka (durable), not after Cassandra
ROUTE   Redis conn map + Pub/Sub; inbox if offline
STORE   Per-user inbox in Cassandra, RF=3, batch writers
SCALE   Stateless chat boxes; never topic-per-user; never mesh-all-servers
GROUP   Fan-out on write; sender keys for E2E
MEDIA   Signed URL → S3/CDN; chat carries media_id only
E2E     Signal: identity + prekeys + ratchet; server cannot read
FAIL    Redis down → treat offline; never ACK before durability; client dedup
```

---

## Sources and how this doc was built

- [WhatsApp System Design — Part 1](https://newsletter.systemdesign.one/p/whatsapp-system-design): requirements, capacity, component map, WebSocket vs polling, service discovery, API envelopes. Remaining Part 1 sections (presence, offline, multi-device, media) are completed here from the published outline and standard interview practice.
- [Design a Chat App System — Part 2](https://newsletter.systemdesign.one/p/design-a-chat-system): Redis Pub/Sub vs Kafka-per-user vs hashing, storage-service pipeline, Signal/E2E. Remaining Part 2 sections (sharding, cleanup, dedup, rate limits, analytics, multi-region, schema versioning) are completed the same way.

Numbers (1B users, 10B msgs/day, 50M connections, 100-member groups, 30-day inbox, 500ms SLO) follow those articles so revision stays consistent.
