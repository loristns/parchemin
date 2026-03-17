# Resource planner modelization: current vs action-graph approach

## Goal

The current planner uses two phases:

1. delete (reverse topological order of the **current** resource graph)
2. create/update (topological order of the **desired** resource graph)

This is incomplete when a resource can be **updated to remove a dependency** before that dependency is deleted.

---

## Graph primitives already available

`Graph[K, V]` supports:

- nodes (`K -> V`)
- directed edges (`from -> to`)
- topological sort (Kahn)
- cycle detection

This is enough to model a new **action graph** with explicit operation-level ordering.

---

## Current approach model

Let each resource be one node in:

- `G_current`: dependencies in current state
- `G_desired`: dependencies in desired state

Edge direction is:

`dependency -> dependant`

### Current scheduling

- Delete phase:
  - iterate `toposort(G_current)`
  - schedule deletions in reverse by doing `insert(0, Delete(...))`
- Create/update phase:
  - iterate `toposort(G_desired)`
  - append `Create(...)` and `Update(...)`

### Why this fails

If `A -> B` exists in current state (B depends on A), and:

- A is deleted
- B is updated so that desired B no longer depends on A

then valid order is:

1. `Update(B)` (detach B from A)
2. `Delete(A)`

The two-phase scheduler cannot express this interleaving because all deletes happen before all updates.

### Failure schema

Current:

```text
A ---> B
```

Desired:

```text
B   (no edge from A)
```

Needed steps:

```text
Update(B) -> Delete(A)
```

Current algorithm rejects this because it validates deletion only against current graph state.

---

## Proposed approach: add action graph

Keep resource graphs for reasoning, but build a third graph:

- `G_actions`: each node is one concrete step
  - `DeleteStep(resource)`
  - `CreateStep(resource)`
  - `UpdateStep(resource)`

Then run `toposort(G_actions)` to obtain executable step order.

## Action node schema

Per resource decision:

- `create`  -> `C(r)`
- `delete`  -> `D(r)`
- `update`  -> `U(r)`
- `replace` -> `D(r)` and `C(r)` with `D(r) -> C(r)`
- `noop`    -> no action node

## Ordering rules encoded as edges

### 1) Intra-resource replace order

`D(r) -> C(r)`

### 2) Current dependency constraints (detach before provider deletion)

For each current edge `dep -> res`:

- if `dep` will be deleted/replaced (has `D(dep)`):
  - if `res` is deleted/replaced: `D(res) -> D(dep)`
  - if `res` is updated and desired no longer depends on `dep`: `U(res) -> D(dep)`
  - otherwise: planning error (cannot delete `dep` safely)

### 3) Desired dependency constraints (provider available before consumer mutation)

For each desired edge `dep -> res`:

- let `ready(dep)` be `C(dep)` if dep is create/replace, otherwise none
- if `res` has desired-state action (`U(res)` or `C(res)`):
  - add `ready(dep) -> action(res)` when `ready(dep)` exists
- if dep is deleted while desired res depends on it: planning error

---

## Worked example

Resources:

- ingredient `milk`
- recipe `pancake` currently depends on `milk`

Desired:

- delete `milk`
- update `pancake` ingredients to remove `milk`

Action graph:

```text
U(pancake) ---> D(milk)
```

Toposort gives:

1. `UpdateStep(pancake)`
2. `DeleteStep(milk)`

This is valid and previously impossible with strict two-phase ordering.

---

## Complexity

Let:

- `R` = number of resources
- `E_current` = number of current dependency edges
- `E_desired` = number of desired dependency edges
- `A` = number of action nodes (`A <= 2R`)
- `E_actions` = number of action graph edges

### Time

- Build resources/decisions: `O(R)`
- Build `G_current` + `G_desired`: `O(R + E_current + E_desired)`
- Build action nodes: `O(R)`
- Build action edges from dependency rules: `O(E_current + E_desired)`
- Toposort actions: `O(A + E_actions)`

Total: linear in graph size  
`O(R + E_current + E_desired + A + E_actions)`

With `A <= 2R` and `E_actions` derived from dependency edges, this stays linear in practice.

### Memory

- resource maps + graphs + action graph: `O(R + E_current + E_desired + A + E_actions)`

---

## Readability guidance

To keep code easy to read and low-abstraction:

- keep `Resource` decision logic explicit
- add small helper maps for actions (`delete_action`, `create_action`, `update_action`)
- keep edge-construction rules in one function with direct conditionals
- avoid generic scheduling frameworks beyond existing `Graph`
- remove debug prints from planner output
