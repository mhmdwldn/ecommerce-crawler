# TLS/SSL Configuration Guide

Panduan mengaktifkan encryption untuk semua service-to-service communication.

## Quick Reference

| Service | TLS Support | Config Method | Complexity |
|---|---|---|---|
| Kafka | SASL_SSL | broker config + client properties | Medium |
| Postgres | SSL | postgresql.conf + certificate | Low |
| ClickHouse | Native TLS | config.xml + certificate | Low |
| MinIO | TLS | MINIO_OPTS env var | Low |
| Elasticsearch | TLS | elasticsearch.yml + xpack | Medium |
| Airflow | HTTPS | webserver config + certificate | Low |
| Caddy | Automatic | Let's Encrypt (production) | Low |

## 1. Postgres SSL

```bash
# Generate self-signed cert (dev)
openssl req -new -x509 -days 365 -nodes \
  -out server.crt -keyout server.key \
  -subj "/CN=postgres"

# Mount ke container
# compose.yaml:
#   volumes:
#     - ./certs/server.crt:/etc/ssl/certs/server.crt:ro
#     - ./certs/server.key:/etc/ssl/private/server.key:ro
#   command: >
#     -c ssl=on
#     -c ssl_cert_file=/etc/ssl/certs/server.crt
#     -c ssl_key_file=/etc/ssl/private/server.key

# Connection string:
#   POSTGRES_DSN=host=postgres sslmode=require ...
```

## 2. ClickHouse TLS

Mount custom config.xml:

```xml
<clickhouse>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
        </server>
    </openSSL>
    <https_port>8443</https_port>
</clickhouse>
```

```yaml
# compose.yaml:
#   volumes:
#     - ./clickhouse-tls.xml:/etc/clickhouse-server/config.d/tls.xml:ro
#     - ./certs:/etc/clickhouse-server/certs:ro
```

## 3. Kafka SASL_SSL

```properties
# broker config
listeners=SASL_SSL://kafka:9093
sasl.enabled.mechanisms=PLAIN
ssl.keystore.location=/etc/kafka/secrets/kafka.keystore.jks
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks

# client config (airflow/spark)
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret";
```

## 4. MinIO TLS

```yaml
# compose.yaml:
#   environment:
#     MINIO_OPTS: "--certs-dir /root/.minio/certs"
#   volumes:
#     - ./certs:/root/.minio/certs:ro
```

## 5. Caddy Auto-HTTPS (Production)

```caddyfile
your-domain.com {
    reverse_proxy airflow:8080
}
# Caddy auto-requests Let's Encrypt for your-domain.com
```

## Dev vs Production

| Environment | Certificates | Verification |
|---|---|---|
| **Dev** | Self-signed, `verify=false` | Skip verification |
| **Staging** | Internal CA | CA cert trusted |
| **Production** | Let's Encrypt / Public CA | Full verification |

## Notes

- Self-signed certs adequate for dev/testing
- Production: use cert-manager (K8s) or Let's Encrypt (Caddy)
- Rotate certificates every 90 days (automate via Vault PKI engine)
- Store certificates in Vault `secret/certs/` path, not in repo
