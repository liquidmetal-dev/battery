# ADR: Place a VM by the memory reserved on each host, across all pools

Related: [issue #163](https://github.com/liquidmetal-dev/battery/issues/163),
[issue #18](https://github.com/liquidmetal-dev/battery/issues/18).
Changes how `PlaceVM` ranks hosts, from
[the PlaceVM ADR](2026-11-16-place-vm.md).

- Status: Accepted
- Date: 2026-11-17

## Context

`PlaceVM` picks the eligible host with the fewest VMs of the pool it
places for. It does not see two things:

- Other pools' VMs. Two pools that share hosts each spread their own VMs,
  but together they can put most of their VMs on one host.
- VM size. A host with two 8 GiB VMs counts the same as a host with two
  1 GiB VMs.

Memory is what a host runs out of first. A Firecracker guest keeps the
memory it touches, so a host must hold close to the sum of its VMs'
`memory_in_mb`. We saw a 32 GiB host run out of memory with four 5 GiB
VMs and a few small ones, while other hosts had room.

## Decision

`PlaceVM` ranks the eligible hosts by the memory reserved on each:

1. The memory reserved on a host is the sum of the pool template's
   `memory_in_mb` over every VM record on the host that is not `DELETING`
   or `FAILED`, and over every placement on the host, of any pool.
2. The host with the least memory reserved wins.
3. A tie goes to the host with the fewest of the pool's own VMs and
   placements, then to the host that comes first in `hosts`.

The store reads each VM's memory from its pool's stored template
(`json_extract(microvm_template, '$.memoryInMb')`). A VM whose pool record
is gone, or whose template has no `memory_in_mb`, counts as 0 MiB.

Nothing else changes: the transaction, the eligibility checks, the
placement's life and the `PlaceVM` signature stay as they were. The new VM
adds the same memory to any host, so `PlaceVM` does not need its size.

## Consequences

Positive:

- Pools that share hosts spread over them together, by memory.
- A large VM no longer counts the same as a small one.
- With one pool, or with pools of one size, rule 3 gives the same spread
  as before.

Negative:

- A VM counts at its pool's current template. After `UpdatePool` changes
  `memory_in_mb`, the pool's older VMs count at the new size until they
  are replaced.
- The query joins `pools` for every VM and placement. With the store's
  sizes, tens to hundreds of rows, this costs little.
- Hosts of different sizes still rank by absolute memory, so a small host
  fills as fast as a large one.

## Alternatives considered

- **Store each VM's memory on its row.** Exact after an `UpdatePool`, but
  it needs a schema change, and the `VMRecord` in the API has no memory
  field to fill it from.
- **A capacity per host, and rank by free memory.** This handles hosts of
  different sizes. It needs a new `Host` field, `poolmgrctl` and Operator
  changes, and a rule for hosts with no capacity set. It can build on this
  change later, as part of #18.
- **Refuse a placement that would overcommit a host.** With balloon free
  page reporting and kernel samepage merging, some overcommit is what we
  want, so this needs a capacity and an overcommit ratio first.

## Out of scope

- Host capacity, CPU, and bin-packing (#18).
