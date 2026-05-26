# eMudi — Grocery Delivery Platform Architecture

A comprehensive system design for a scalable grocery delivery platform operating in Dhaka, Bangladesh. This project showcases a production-grade architecture handling order management, payment processing, real-time delivery tracking, and offline-resilient operations.

## Overview

**eMudi** is a distributed system designed to scale from 500 to 10,000+ active delivery persons. The platform emphasizes:

- **Event-driven architecture** with Kafka as the integration backbone
- **Database per service** pattern for microservices independence
- **Payment system** with multiple provider integrations (bKash, Nagad, Card, Bank Transfer)
- **Real-time tracking** via WebSocket
- **Offline-first delivery app** with automatic sync on reconnection
- **Geospatial operations** for zone management and delivery assignment

## Key Architectural Patterns

### Core Principles

1. **Database Per Service** — Each service owns its data exclusively; no shared databases
2. **Event-Driven Communication** — Asynchronous Kafka messaging between services
3. **Pre-Pay Model** — Payment confirmed before delivery person assignment
4. **Zone-Based Operations** — Delivery persons assigned to geographic zones
5. **Offline Resilience** — Core workflows function without connectivity

### Technology Stack

| Layer | Technologies |
|-------|---|
| **Frontend** | React Native (Mobile), React.js (Web) |
| **Backend Services** | Java (Spring Boot) |
| **Message Broker** | Apache Kafka (3-broker cluster) |
| **Databases** | MySQL, PostgreSQL 16 + TimescaleDB + PostGIS |
| **Caching** | Redis Cluster (3P + 3R) |
| **Tracking** | WebSocket (STOMP/SockJS) |
| **Mobile Storage** | Encrypted SQLite |

## System Components

### Services Overview

| Service | Purpose | Key Responsibility |
|---------|---------|---|
| **Customer API** | Customer-facing REST API | Order submission, customer account management |
| **Auth Service** | Authentication & authorization | JWT tokens, refresh token rotation, revocation |
| **Order Receiver** | Event ingestion | Deduplicate and validate incoming orders |
| **Order Service** | Order lifecycle management | State machine, delivery assignment via SKIP LOCKED |
| **Payment Service** | Payment orchestration | Multi-provider adapter pattern, webhook validation |
| **Location Service** | Geographic operations | Zone management, delivery person tracking, geocoding |
| **Order Tracking** | Real-time updates | WebSocket push to customers (< 3s latency) |
| **Delivery App** | Delivery person interface | Order collection, offline mode, GPS heartbeat |

### Key Workflows

#### Order Lifecycle
```
DRAFT → SUBMITTED → PAYMENT_PENDING → PAYMENT_CONFIRMED 
→ PENDING → ASSIGNED → COLLECTING → COLLECTED 
→ OUT_FOR_DELIVERY → DELIVERED → SETTLED
```

#### Payment Flow
1. Customer submits order with payment method
2. `orders.payment-requested` published to Kafka
3. Payment Service initiates provider transaction
4. Customer completes payment (OTP, redirect, or reference)
5. Provider sends webhook callback (HMAC-validated)
6. `payments.completed` or `payments.failed` published
7. Order Service advances order status

## Architecture Decisions

### ADRs (Architecture Decision Records)

Key architectural choices documented in the architecture document:

- **ADR-01**: Kafka as owned infrastructure (not managed service)
- **ADR-02**: Database per service (no cross-service DB access)
- **ADR-03**: PostgreSQL + TimescaleDB + PostGIS for location data
- **ADR-04**: SKIP LOCKED for contention-free order assignment at scale
- **ADR-05**: Order Receiver decoupling via `orders.ingested` event
- **ADR-06**: PgBouncer for connection pooling
- **ADR-07**: React Native for delivery app (offline + cross-platform)
- **ADR-08**: JWT with encrypted offline token caching
- **ADR-09**: WebSocket with sticky sessions for tracking
- **ADR-10**: Kafka partitioned by zoneId for order ordering
- **ADR-11**: Payment provider adapter pattern
- **ADR-12**: Async-first payment with pre-pay model
- **ADR-13**: HMAC webhook validation for provider callbacks

## Scaling Strategy

### Capacity Timeline

| Metric | Launch | Growth | Scale |
|--------|--------|--------|-------|
| Active Persons | 500 | 2,000 | 10,000 |
| Daily Orders | 10,000 | 40,000 | 200,000 |
| Location Writes/sec | ~33 | ~133 | ~667 |

### Scaling by Component

- **Location Service**: 2 → 4 → 8 instances
- **Order Service**: 3 → 3 → 6 instances
- **Order Tracking**: 2 → 4 → 6 instances
- **Kafka Partitions**: 24 → 36 → 48 (location.updates)
- **Database**: Single → Read Replica → Distributed

## Security Highlights

| Aspect | Implementation |
|--------|---|
| **Transport** | TLS 1.2+ on all communication |
| **Authentication** | JWT RS256 (15 min tokens, 7-day refresh) |
| **Authorization** | RBAC per endpoint |
| **Token Revocation** | Redis blacklist with TTL |
| **Data at Rest** | AES-256 encryption |
| **Webhook Validation** | HMAC-SHA256 with constant-time comparison |
| **Secrets Management** | Environment-injected secrets vault |
| **API Rate Limiting** | Redis sliding window (100 req/60s) |

## Observability & Monitoring

### Key Metrics

- Kafka consumer lag per group
- Payment success rate per provider
- Order assignment latency (p99 < 500ms)
- Tracking push latency (< 3 seconds)
- WebSocket connection count
- Database replica lag

### Alerts

- DLQ depth > 0
- TIMED_OUT payments > 0 (immediate)
- HMAC validation failures > 5/min
- Payment success rate < 95% per provider

## Offline Strategy

The Delivery App supports core workflows without connectivity:

✅ **Offline Capable**
- View assigned order
- Mark items collected/unavailable
- Confirm delivery (queued for sync)
- Cached authentication

❌ **Requires Connectivity**
- Claim new order
- GPS heartbeat
- New session login

**Sync Mechanism**: Queued local changes sent as idempotent requests on reconnection.

## Extensibility

The architecture supports adding:

### New Payment Providers
1. Implement `PaymentProvider` interface (adapter only)
2. Register with `PaymentRouter`
3. Add shared secret to vault
4. Add webhook endpoint

**No changes needed** to Kafka, Order Service, DB schema, or other services.

### New Delivery Categories
1. Add `orderType` enum value
2. Implement type-specific strategy in Order Service
3. Add screen template to Delivery App

**No infrastructure changes** required.

## File Structure

```
emudi/
├── README.md (this file)
└── emudi-architecture-v1.2.md
    ├── System overview & principles
    ├── Functional & non-functional requirements
    ├── Complete system architecture
    ├── Database inventory & design
    ├── Zone & delivery model
    ├── Component design (7 services + Kafka + Redis)
    ├── API reference
    ├── Kafka topic design
    ├── Redis caching strategy
    ├── Security implementation
    ├── Offline mode strategy
    ├── Scaling strategy & roadmap
    ├── Observability & monitoring
    ├── Deployment architecture
    ├── Extensibility patterns
    └── Architecture Decision Records (ADR-01 through ADR-13)
```

## Performance Targets

| Metric | Target | Implementation |
|--------|--------|---|
| Order Assignment Latency | < 500ms p99 | SKIP LOCKED |
| Tracking Update Latency | < 3 seconds | WebSocket + Kafka |
| Payment Initiation | < 2s p99 | Async provider APIs |
| Webhook Processing | < 500ms | Immediate state machine |
| Uptime SLA | 99.9% | Multi-instance redundancy |
| RTO | 30 minutes | Kafka replayability |
| RPO | 5 minutes | Event sourcing + DB backup |

## Learning Value

This project demonstrates:

- **Microservices at scale** — Independent services with eventual consistency
- **Event-driven architecture** — Kafka as integration backbone
- **Payment system design** — Multi-provider async payment processing
- **Geospatial operations** — PostGIS for zone management
- **Real-time systems** — WebSocket for live tracking
- **Offline-first mobile** — Sync strategies for disconnected environments
- **SKIP LOCKED** — Database-native concurrency without distributed locks
- **Security patterns** — HMAC validation, JWT, encryption
- **Scaling strategies** — Horizontal scaling without re-architecture

## Document Version

- **Version**: 1.2
- **Status**: Published
- **Last Updated**: 2026
- **Changes from v1.1**: Payment Service promoted to owned component; provider adapters; HMAC validation; ADRs 11-13 added

---

For detailed component specifications, API definitions, Kafka topic configuration, and architecture decision rationales, refer to `emudi-architecture-v1.2.md`.

**Questions?** Review the architecture document or raise an Architecture Review Board (ARB) change request.
