# Model adapters

An adapter answers `state + questions`. It does not generate free text.

```text
health_sync / health
infer / infer_blocking
infer_many          optional
shutdown
```

Capabilities are declarative. A model without native multi-state batching simply leaves `supports_native_batch` false. Choice, score, and noul are the types Keziah validates. A future adapter can decline a type by failing the call with a non-retryable error; the queue will not invent an answer.

## Mock

`MockAdapter` hashes the model id, the state, and each question. The same input returns the same probabilities. It needs no network, no key, and no weights. Two mock model ids produce different answers for the same state, which is enough to compare provenance without a real model. Tests and `keziah benchmark` use it.

## Laya

Laya 0.3.x exposes `Router.predict(state, questions)` and `Router.predict_batch`. The result is already a System-1 payload: `answers` for choice, score, and noul, plus `usage` and a `routing` block that names the checkpoint (`english`, `multilingual`, or `typed-decisions`).

The adapter builds one `Router` and reuses it. Health checks import the package and do not download weights. `preload: true` (or `warm()`) loads checkpoints at startup. `predict` runs under a lock. Default concurrency is 1, matching Laya's own HTTP server, which uses a single inference worker because one forward pass is what a CPU or GPU checkpoint wants.

Optional request parameters passed through to Laya: `max_len`, `head_max_len`, `lang`, `task`, `checkpoint`.

If the `laya` package is missing, the model is listed and `available` is false. Jobs that name `laya` fail with `model_unavailable`. They are not sent elsewhere.

## Jev

Jev is TypeSafe's hosted System-1 model.

```text
POST https://api.typesafe.ai/v1/systemone
Authorization: Bearer <key>
{"model": "jev-latest", "state": ..., "questions": ...}
```

The key is read from the configured environment variable, then `TYPESAFE_API_KEY`, then `JEV_API_KEY`. It is not logged and it is not copied into errors. Health in 0.1 means "a key is configured". Keziah does not spend a billed call to probe Jev.

| Status | Treatment |
| --- | --- |
| 401, 403 | terminal authentication failure |
| 400, 422 | terminal invalid request |
| 429, 529 | retry, honouring `Retry-After` when it is a number of seconds |
| 5xx | retry |
| timeout, connection error | retry |

Jev's choice questions use a `criteria` object. Keziah accepts a choice `options` list and normalises it to `{option: null}` before the call. Score criteria, when present, must be a list of 2 to 10 levels. Jev requires them; a score question without criteria is accepted by the queue and rejected by Jev as an invalid request. The mock invents a short rubric only for its own answers.

There is no documented multi-state HTTP batch for Jev, so the adapter does not set `supports_native_batch`.

## Generic HTTP

`adapter: http` posts the same body to `{endpoint}/v1/systemone`. Use it for another service that already speaks this protocol, including a separately run `laya-serve`. It is not an OpenAI chat adapter.

## Discovery

`GET /v1/models` and `keziah models` list configured models with adapter name, availability, local versus remote, version when known, capabilities, concurrency, active slots, health, and endpoint. `GET /v1/models/{id}` and `keziah show <id>` add the health detail. Credentials are not included.

A model being down does not fail `/health/ready`. Readiness means the process and the queue are usable. Liveness means the process answered. Model health is on the model object.

## Aliases and groups

```yaml
aliases:
  local: laya
model_groups:
  emotion:
    primary: laya
    fallbacks: [jev]
```

`local` always means Laya. `emotion` means Laya when Laya is available, otherwise Jev, and the result says fallback was used. A job that asks for `laya` does not use the group.
