"""The ProcessCall runtime graph — and nothing else (#144).

This module must NOT import from ``..process_ir``. That is enforced by a
source-level scan in the graph-namespace test, and it is not stylistic: the
whole acceptance criterion is that a topology runtime edge cannot be confused
with a ProcessIR CFG edge or a ComponentPlan build dependency, and the cheapest
way for that to quietly stop being true is for this module to start reusing the
compiler's ordering code.

Only ``process_call`` relations become arcs. Cache use, property use, schedule
binding, deployment binding and API routing are all real relations that are
deliberately not edges here: none of them means "A must run before B", and
folding them in would yield an order that looks authoritative and means nothing.
"""

from __future__ import annotations

from typing import Dict, List, Set, Tuple

from ...errors import TOPOLOGY_DEPENDENCY_CYCLE
from ...models.system_topology import SystemTopologySpecV1
from .contracts import TopologyDiagnosticV1, TopologyRuntimeOrderV1
from .findings import topology_finding


class _TopologyRuntimeArcV1:
    """One caller→callee arc. Private: arcs never leave this module."""

    __slots__ = ("caller", "callee", "relation_index")

    def __init__(self, caller: str, callee: str, relation_index: int) -> None:
        self.caller = caller
        self.callee = callee
        self.relation_index = relation_index


def _arcs(spec: SystemTopologySpecV1) -> Tuple[_TopologyRuntimeArcV1, ...]:
    return tuple(
        _TopologyRuntimeArcV1(rel.caller_process, rel.callee_process, index)
        for index, rel in enumerate(spec.relations)
        if rel.kind == "process_call"
    )


def _process_keys(spec: SystemTopologySpecV1) -> Tuple[str, ...]:
    return tuple(sorted({obj.key for obj in spec.objects if obj.kind == "process"}))


def derive_runtime_process_order(
    spec: SystemTopologySpecV1,
) -> TopologyRuntimeOrderV1:
    """Lexically tie-broken Kahn ordering over the ProcessCall graph.

    Callee before caller: a sub-process must be available before the process
    that invokes it. Ties are broken lexically rather than by authored order —
    authored order is a caller's formatting choice, and letting it leak into the
    plan would make two documents that mean the same thing produce different
    bytes.

    A cyclic graph yields the empty order. The cycle itself is reported by
    :func:`collect_dependency_findings`; emitting a partial order alongside a
    cycle finding would give a caller something to act on that is not actually a
    valid ordering.
    """
    nodes = _process_keys(spec)
    node_set = set(nodes)
    arcs = [a for a in _arcs(spec) if a.caller in node_set and a.callee in node_set]

    # Edge callee -> caller, deduplicated: the same edge declared twice must not
    # double the in-degree and strand a node forever.
    successors: Dict[str, Set[str]] = {node: set() for node in nodes}
    indegree: Dict[str, int] = {node: 0 for node in nodes}
    seen: Set[Tuple[str, str]] = set()
    for arc in arcs:
        if arc.caller == arc.callee or (arc.callee, arc.caller) in seen:
            continue
        seen.add((arc.callee, arc.caller))
        successors[arc.callee].add(arc.caller)
        indegree[arc.caller] += 1

    ready = sorted(node for node in nodes if indegree[node] == 0)
    order: List[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for nxt in sorted(successors[current]):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                # Insert in sorted position rather than append-then-sort: the
                # result is the same, and this keeps the invariant ("ready is
                # always sorted") local to the one place that can break it.
                _insert_sorted(ready, nxt)

    if len(order) != len(nodes):
        return TopologyRuntimeOrderV1(order=())
    return TopologyRuntimeOrderV1(order=tuple(order))


def _insert_sorted(items: List[str], value: str) -> None:
    low, high = 0, len(items)
    while low < high:
        mid = (low + high) // 2
        if items[mid] < value:
            low = mid + 1
        else:
            high = mid
    items.insert(low, value)


def _cyclic_sccs(
    nodes: Tuple[str, ...], arcs
) -> Tuple[frozenset, ...]:
    """Strongly connected components that actually contain a cycle.

    Tarjan, iterative so a deep call graph cannot blow the Python stack. A
    single-node component counts only when it has a self-loop.

    The first implementation pruned sources and sinks instead, which is not the
    same thing: two cycles joined by an acyclic bridge leave every node with
    both an in-edge and an out-edge, so nothing prunes and the BRIDGE survives
    as if it were cyclic. Pointing a caller at a relation that is in no cycle
    means following the remediation removes neither.
    """
    successors: Dict[str, Set[str]] = {node: set() for node in nodes}
    for arc in arcs:
        successors[arc.caller].add(arc.callee)

    index_of: Dict[str, int] = {}
    low: Dict[str, int] = {}
    on_stack: Set[str] = set()
    stack: List[str] = []
    counter = 0
    components: List[frozenset] = []

    for root in nodes:
        if root in index_of:
            continue
        work: List[Tuple[str, List[str]]] = [(root, sorted(successors[root]))]
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, pending = work[-1]
            if pending:
                nxt = pending.pop(0)
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, sorted(successors[nxt])))
                elif nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index_of[node]:
                component = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                if len(component) > 1 or node in successors[node]:
                    components.append(frozenset(component))
    return tuple(components)


def collect_dependency_findings(
    spec: SystemTopologySpecV1,
) -> Tuple[TopologyDiagnosticV1, ...]:
    """Report a ProcessCall cycle at its canonical earliest authored relation.

    "Canonical" matters: a cycle has no natural first element, so pointing at
    whichever node the traversal happened to reach would move the diagnostic when
    an unrelated relation is added. The pointer is the lowest authored index
    among the relations that lie INSIDE a cyclic component — an edge merely
    touching one, such as a bridge between two separate cycles, is not a
    relation whose removal breaks anything.
    """
    nodes = _process_keys(spec)
    node_set = set(nodes)
    arcs = [a for a in _arcs(spec) if a.caller in node_set and a.callee in node_set]
    if not arcs:
        return ()

    cyclic = _cyclic_sccs(nodes, arcs)
    if not cyclic:
        return ()

    # An arc is INTERNAL to a cycle when both endpoints sit in the same
    # strongly connected component. A bridge from one cycle to another has its
    # ends in two different components and is excluded.
    internal = [
        arc.relation_index
        for arc in arcs
        if any(arc.caller in scc and arc.callee in scc for scc in cyclic)
    ]
    if not internal:
        return ()

    return (
        topology_finding(
            TOPOLOGY_DEPENDENCY_CYCLE,
            severity="error",
            phase="dependency",
            path=f"/relations/{min(internal)}",
            subject="process_call",
        ),
    )


__all__: List[str] = [
    "collect_dependency_findings",
    "derive_runtime_process_order",
]
