# Migration Failure Agent v2.6 local run

## 1. Python environment

From `agents/migration-failure`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Always use `python -m pip` so pip and python cannot silently point at different environments.

## 2. PostgreSQL

The bundle assumes PostgreSQL is already installed locally.

Create the database if needed:

```bash
psql -h 127.0.0.1 -U postgres -c "CREATE DATABASE migration_agent;"
```

If it already exists, continue.

Verify pgvector:

```bash
psql -h 127.0.0.1 -U postgres -d migration_agent \
  -c "SELECT name, default_version FROM pg_available_extensions WHERE name='vector';"
```

Apply schema:

```bash
export DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'
python scripts/seed_postgres.py
```

`seed_postgres.py` applies `sql/mfa_postgres.sql`, seeds SRE Tracker records and loads the common Red Hat knowledge records into `knowledge.documents`/`knowledge.chunks`.

## 3. Optional Red Hat ingestion

For public/authorized sources:

```bash
python scripts/ingest_redhat.py \
  --urls 'https://access.redhat.com/products/red-hat-openshift-virtualization/' \
         'https://access.redhat.com/articles/7119411' \
  --output datasets/redhat_knowledge_scraped.yaml
```

For restricted Knowledgebase content, only ingest content you are authorized to access. The script does not bypass Red Hat authentication or licensing controls.

Then seed that dataset explicitly:

```bash
python scripts/seed_postgres.py --dataset datasets/redhat_knowledge_scraped.yaml
```

## 4. Real pgvector embeddings

The default embedding path is an OpenAI-compatible local endpoint, llama.cpp:

```bash
export EMBEDDING_BASE_URL='http://127.0.0.1:8080/v1'
export EMBEDDING_MODEL='nomic-embed-text-v1.5'
export EMBEDDING_DIMENSION=768
export EMBEDDING_DOCUMENT_PREFIX='search_document: '
export EMBEDDING_QUERY_PREFIX='search_query: '
python scripts/embed_knowledge.py
```

The model's returned vector dimension must equal the database vector dimension. Do not change embedding models without checking their dimension and migrating the vector column/index if necessary.

## 5. LLM selection

OpenRouter:

```bash
export LLM_PROVIDER=openrouter
export OPENROUTER_API_KEY='...'
export OPENROUTER_MODEL='openrouter/free'
```

Local OpenAI-compatible endpoint:

```bash
export LLM_PROVIDER=local
export LOCAL_LLM_BASE_URL='http://127.0.0.1:11434/v1'
export LOCAL_LLM_MODEL='qwen2.5:7b'
```

The LLM is not a safety boundary. Evidence policy, deterministic gates, approval and verification remain outside the model.

## 6. Test

```bash
PYTHONPATH=. pytest -q
python run_simulator.py storage-csi-timeout
python run_simulator.py storage-csi-backend-healthy
python run_simulator.py network-nad-missing
python run_simulator.py vmware-cbt-retry
python run_simulator.py insufficient-evidence
```

The simulator represents Splunk/Prometheus observations. It does not create fake Kubernetes, MTV, VMware or storage services.

## 7. First real local path

```text
Kafka event
  -> failure case
  -> procedural skill
  -> SRE history / periodic memory
  -> Splunk / Prometheus evidence
  -> knowledge/RAG context
  -> diagnosis
  -> deterministic safety gate
  -> approval
  -> EDA/AAP
  -> verification
  -> SRE Tracker outcome
```

## 8. Kafka local E2E

Install the Python dependency from the project root:

```bash
python -m pip install -e .
```

Start a real local Kafka broker. If Kafka is already installed, expose its listener at `127.0.0.1:9092`. Then verify:

```bash
python -c "from confluent_kafka import Producer; Producer({'bootstrap.servers':'127.0.0.1:9092'}).list_topics(timeout=5); print('Kafka OK')"
```

Create the MFA topics:

```bash
python scripts/create_kafka_topics.py
```

Start the agent in terminal 1:

```bash
python scripts/run_kafka_agent.py
```

Read results in terminal 2:

```bash
python scripts/read_agent_results.py
```

Publish the `MigrationFailed` event in terminal 3:

```bash
cat <<'JSON' | python scripts/publish_migration_failed.py
{"event_id":"evt-local-001","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","severity":"critical","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
JSON
```

The complete path is:

```text
mfa.migration.failed
  -> KafkaIngress
  -> MigrationFailureEngine.run(request)
  -> failure case / evidence / diagnosis
  -> mfa.agent.result
```

For the first E2E, the local registry supplies fixture Splunk/Prometheus/RAG data. PostgreSQL is the durable SRE Tracker when `DATABASE_URL` is set. There is no fake Kubernetes, VMware, MTV or storage server.
