# Dingo's Cluster Watcher

**PRIVATE: this is proprietary source code for the https://hub.dingotiles.com service.**

This microservice has the job of watching Etcd for changes to cluster nodes - new, updates or missing - and notifying the Hub, which in turn will store the new changes and then update any live dashboards.

## Deployment

Staging:

```
cf t -o dingotiles -s dingo-api-staging
cf push
```

Production:

```
cf t -o dingotiles -s dingo-api-prod
cf push
```

Required environment variables:

* `ETCD_URI` - the backbone Etcd cluster used by all Docker containers/agents/patroni.
* `HUB_URI` - an HTTP uri to the staging/prod hub API

Optional:

* `WATCH_ROOT_PATH` - by default watch for changes under `/service/` key path; if using etcd-cf-service-broker then change to `/service_instances/`
