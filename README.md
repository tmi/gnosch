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

## Status
This is an experimental project.
Don't use it.

## TODOs
- phase1
  - add in tests
  - add in config
  - add in docker
  - add precommit
  - add cicd gh actions
- phase2
  - split controller and worker into two standalone processes
  - introduce bidirectional grpc event/command stream
- phase3
  - introduce dag abstraction and full fledged controller process
  - etc
- phaseR
  - rewrite internals to rust
