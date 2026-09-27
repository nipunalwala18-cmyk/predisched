# docker

The Docker images and the Compose stack (prompt 24, [docs/components/deployment.md](../docs/components/deployment.md)).

- `java.Dockerfile` + `entrypoint.sh`: every Java service in one image; the first argument picks
  the role (`scheduler`, `worker`, `dashboard-api`, `client`, `benchmark`).
- `prediction.Dockerfile`: the prediction server; trains m1-m3 from the committed dataset at build.
- `dashboard.Dockerfile` + `nginx.conf`: the React build behind nginx, which proxies the API.
- `spark.Dockerfile`, `mpi.Dockerfile`: the Spark and MPI jobs of prompts 12-14 (`tools` profile).
- `docker-compose.yml`: the whole stack, with `configs/docker.yaml`.

```bash
docker compose -f docker/docker-compose.yml up --build -d --wait
```
