# GnoSch
GnoSch is a gnostic scheduler -- its role is to schedule a workload, represented by a DAG enriched with estimates for memory and time consumption, upon a set of hosts, in a performant fashion.

The knowledge of memory and time consumptions gives it an advantage over traditional agnostic schedulers such as Spark or Dask.
Consider a case where we launch jobs A and B, each producing an output -- should the scheduler put them to a same host, or not?
In case there is a third job C which consumes their output, and all three fit to a single host, then yes, otherwise no.
GnoSch operates with this information, but is able to flexibly re-schedule in case the original memory/time estimates did not hold.

GnoSch should be, however, viewed more as a toolkit for writing distributed jobs, rather than a full-fledged framework.
It does not provide any distributed dataset abstractions (like Spark/Dask distributed dataframes).
The user instead manages all datasets directly, with a few primitives for explicit moving between workers.
This, in turn, gives the user more control and allows for developing the jobs in a more frugal fashion.

## Features
* Frugal memory management -- the distributed datasets are handled via SharedMemory. Thus when two processes running at the same worker use the same dataset (a situation which the scheduler actively encourages), that dataset resides in RAM only once (compared to three times if implemented naively). Sending a dataset over to other workers has no memory overhead.
* Language-independent GRPC contract between workers and controller (scheduler), allowing for greater modularity -- such as custom controllers or workers, or a completely different paradigms such as multi-controller or no-controller.

## Status
This is an experimental project.
Don't use it.

## TODOs
- cicd, best practices, etc
  - add in tests
  - add cicd gh actions
  - add in config
  - add in docker
- basic controller functionality
  - controller tracking job worker assignment
  - controller worker lookup
  - controller thread-safe? Probably locks in managers
- basic scheduler functionality
  - [✓] api and model
  - [✓] simulator naive impl
  - [ ] simulator basic test
  - [ ] api naive impl
  - [ ] api basic test
  - introduce dag abstraction and full fledged controller process
  - etc
- design improvements
  - job server -- replace the big loop with some better pattern, clean the api
  - grpc api -- clean the api, make the contract more intelligible
- reliability and recoverability
  - worker heartbeat/ping, explicit removal, try out multiple workers joining in and out
    - maybe support dataset resilience parameter
  - ...
- performance improvements
  - rewrite internals to rust
