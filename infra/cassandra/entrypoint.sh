#!/usr/bin/env bash
set -euo pipefail

sed -i 's/^authenticator: .*/authenticator: PasswordAuthenticator/' /etc/cassandra/cassandra.yaml
sed -i 's/^authorizer: .*/authorizer: CassandraAuthorizer/' /etc/cassandra/cassandra.yaml
exec /usr/local/bin/docker-entrypoint.sh "$@"
