# idle
idle is a simple stateful stream processing framework

## use-cases
idle hopes to integrate in the following ways:

1. kafka to {kafka, s3, ?}
2. ? to {kafka, s3, ?}

with point #1 as priority.

### limitations
1. we rely on jq and json for schema processing. this can be slow.
2. sql queries are baked into an 'evaluation tree/frame' but we still have to evaluate in-memory for each step
3. opinionated parallelism structure means there is limited opportunity for fan-out or re-processing of events

## roadmap
this is a rough outline of what is next in no particular order:

- [x] sql queries are parsed on start and baked in as an Eval Tree
  - [ ] schema automation:
    - [ ] column aliases are handled and inserted into the output schema
    - [ ] column type casts are handled and inserted into the output schema
- [ ] protobuf is supported out of the box
  - [ ] how do we specify schemas?
- [x] error handling with side-outputs
- [ ] streams mode to load multiple pipelines in one process
  - [ ] SPIKE: SQL handling to join on another stream in 'streams mode'
- [ ] graceful termination of processes
- [ ] SQL compat (this will unlikely be >80% coverage of the postgres sql spec)
- [ ] sinks
  - [ ] kafka producer
  - [ ] s3 writer
- [ ] sources
  - [ ] kafka consumer
- [ ] watermarking and orderliness