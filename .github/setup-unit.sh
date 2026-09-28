docker compose -f docker-compose-ci.yml up -d

for i in $(seq 1 60); do
  if curl -sf http://localhost:51525/v1/.well-known/ready > /dev/null; then
    exit 0
  fi
  sleep 1
done

echo "Weaviate did not become ready in time" >&2
docker compose -f docker-compose-ci.yml logs
exit 1
