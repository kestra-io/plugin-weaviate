<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 4 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 4 minutes.</i></p>

# Kestra Weaviate Plugin

## Why

- What user problem does this solve? Teams need to manage and query vectors in Weaviate from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Weaviate steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Weaviate.

## What

- Provides plugin components under `io.kestra.plugin.weaviate`.
- Includes `SchemaCreate`, `BatchCreate`, `Query` and `Delete`, built on the Weaviate Java client v6 (REST and gRPC). Requires Weaviate 1.32 or later.
- See the [plugin documentation](src/main/resources/doc/io.kestra.plugin.weaviate.md) for connection setup, search types, filters, a full example flow, and how to upgrade from the GraphQL-based plugin.
- To create a collection with any Weaviate option, generate its JSON schema with the [Weaviate collection builder](https://weaviate.github.io/weaviate-add-collection/) and pass it to `SchemaCreate` as `definition`.

## Run it locally

You need Docker with Compose, and Java 21 or later to build the plugin.

### 1. Build the plugin

```bash
git clone https://github.com/kestra-io/plugin-weaviate.git
cd plugin-weaviate
./gradlew clean shadowJar
```

This creates `build/libs/plugin-weaviate-<version>.jar`. Kestra loads every jar in that folder, and `clean` makes sure only the new one is there.

### 2. Start Weaviate and Kestra

The example flow vectorizes text with Cohere. Set `COHERE_API_KEY` in your shell, if it isn't already set, and start the stack:

```bash
export COHERE_API_KEY=your-cohere-api-key
docker compose -f docker-compose-dev.yml up -d
```

The compose file gives the key to Kestra as the `COHERE_API_KEY` secret, so flows use it with `{{ secret('COHERE_API_KEY') }}`. Don't set `SECRET_COHERE_API_KEY` yourself. Kestra expects that variable to be base64-encoded, and a raw key gives Weaviate an unusable key ("no api key found").

This starts:

- Weaviate 1.39.7: REST on `localhost:8080`, gRPC on `localhost:50051`.
- Kestra with the plugin you just built: <http://localhost:8081>. Log in as `admin@kestra.io` with password `Admin1234`. Kestra takes about a minute to start.

Both keep their data in Docker volumes, so it survives restarts. If you skip the Cohere key, flows that don't use a Cohere collection still work. To change the key, set it again and run `docker compose -f docker-compose-dev.yml up -d`, which recreates Kestra with the new value.

### 3. Run the example flow

1. Open <http://localhost:8081>, go to **Flows**, and click **Create**.
2. Paste the [example flow](src/main/resources/doc/io.kestra.plugin.weaviate.md#example-load-and-search-a-collection) as is. It connects to `http://weaviate:8080`, the Weaviate service of this stack.
3. Click **Save**, then **Execute**. The logs of the `report` task show the result of each search.

In your own flows on this stack, also use `http://weaviate:8080`, not `localhost`. Inside the Kestra container, `localhost:8080` is Kestra itself, and the tasks fail with `HTTP 404: GET /v1/meta`.

To check the data directly, run `curl "http://localhost:8080/v1/objects?class=Article"`.

You can also do it with the API, after saving the flow as `flow.yml`:

```bash
curl -u admin@kestra.io:Admin1234 -X POST http://localhost:8081/api/v1/main/flows \
  -H 'Content-Type: application/x-yaml' --data-binary @flow.yml
curl -u admin@kestra.io:Admin1234 -X POST \
  http://localhost:8081/api/v1/main/executions/company.team/weaviate_load_and_search
```

### 4. Change the code

After changing the plugin, rebuild it and restart Kestra to load the new jar:

```bash
./gradlew clean shadowJar
docker compose -f docker-compose-dev.yml restart kestra
```

The tests run against their own Weaviate on ports 51525 and 51526. That Weaviate has the model2vec module, which the `NEAR_TEXT` tests need. It runs alongside the stack above:

```bash
./.github/setup-unit.sh   # starts the test Weaviate and waits until it is ready
./gradlew check           # tests, formatting and docs checks
```

### 5. Stop everything

```bash
docker compose -f docker-compose-dev.yml down       # add -v to also delete the data
docker compose -f docker-compose-ci.yml down
```

## Documentation
* Full documentation can be found under [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/).


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
