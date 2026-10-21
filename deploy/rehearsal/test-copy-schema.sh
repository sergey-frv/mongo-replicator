#!/usr/bin/env bash
# Проверка copy-schema.js на compose разработки (make test-copy-schema):
#   1. фикстура it_copy_schema на mongo-src — всё, что есть и в 4.4;
#   2. copy-schema.js в одноразовом контейнере mongo:8.0 (ради mongosh) на mongo-dst;
#   3. mongo-replicator verify --schema-only в контейнере app — код 0;
#   4. удаление it_copy_schema на обеих сторонах.
set -euo pipefail

cd "$(dirname "$0")/../.."

db=it_copy_schema
src_uri='mongodb://mongo-src:27017/?replicaSet=rs8'
dst_uri='mongodb://mongo-dst:27017/?replicaSet=rsdst'

# mongosh в контейнере сервиса: аргументы — после имени сервиса
mongosh_in() {
	local svc=$1
	shift
	docker compose exec -T "$svc" mongosh --quiet "$@"
}

cleanup() {
	mongosh_in mongo-src "$db" --eval 'db.dropDatabase()' >/dev/null
	mongosh_in mongo-dst "$db" --eval 'db.dropDatabase()' >/dev/null
}

docker compose up -d --wait mongo-src mongo-dst
# остатки прерванного прогона: copy-schema.js на непустую цель не пишет
cleanup
trap cleanup EXIT

echo '--- фикстура на mongo-src'
mongosh_in mongo-src "$db" --eval '
db.createCollection("capped", {capped: true, size: 1048576, max: 1000});
db.createCollection("validated", {
  validator: {$jsonSchema: {bsonType: "object", required: ["name"], properties: {name: {bsonType: "string"}}}},
  validationLevel: "moderate",
  validationAction: "warn",
});
db.createCollection("collated", {collation: {locale: "ru", strength: 2}});
db.collated.createIndex({title: 1});
db.events.createIndex({createdAt: 1}, {expireAfterSeconds: 3600});
db.events.createIndex({status: 1, createdAt: -1}, {partialFilterExpression: {status: {$exists: true}}});
db.users.createIndex({email: 1}, {unique: true});
db.users.createIndex({phone: 1}, {sparse: true});
db.users.createIndex({name: 1}, {hidden: true});
db.createView("active_names", "active_users", [{$project: {name: 1}}]);
db.createView("active_users", "users", [{$match: {active: true}}]);
'

echo '--- copy-schema.js на mongo-dst'
docker compose run --rm --no-deps -T \
	-v "$PWD/deploy/rehearsal:/rehearsal:ro" \
	-e SRC_URI="$src_uri" -e DST_URI="$dst_uri" -e DB="$db" \
	--entrypoint mongosh mongo-src --nodb --quiet /rehearsal/copy-schema.js

echo '--- verify --schema-only'
docker compose run --rm -T app go run ./cmd/mongo-replicator verify \
	--db "$db" --schema-only --state-dir /tmp/verify-state --log-level debug

echo '--- copy-schema: OK'
