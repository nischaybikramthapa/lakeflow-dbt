"""Dependency graph over models, plus dbt-style node selection."""
from __future__ import annotations

from typing import Dict, Iterable, List, Set

from .errors import GraphError
from .model import Model


class Graph:
    """Models only: sources are leaf reads handled by the pipeline engine."""

    def __init__(self, models: Dict[str, Model]):
        self.models = {n: m for n, m in models.items() if m.config.enabled}
        self.parents: Dict[str, Set[str]] = {}
        self.children: Dict[str, Set[str]] = {n: set() for n in self.models}
        for name, model in self.models.items():
            ps = {r for r in model.refs if r in self.models}
            missing = [r for r in model.refs if r not in self.models]
            if missing:
                raise GraphError(
                    f"model {name!r} refs unknown model(s): {', '.join(sorted(missing))}"
                )
            self.parents[name] = ps
            for p in ps:
                self.children[p].add(name)

    def topo_order(self, names: Iterable[str] = None) -> List[str]:
        """Kahn's algorithm; raises GraphError listing the cycle on failure.

        When ``names`` is a subset, its ancestors are pulled in too — a
        compiled pipeline must be self-contained because datasets resolve
        by name at pipeline validation time.
        """
        names = set(names) if names is not None else set(self.models)
        needed = set(names)
        frontier = list(names)
        while frontier:
            n = frontier.pop()
            for p in self.parents.get(n, ()):
                if p not in needed:
                    needed.add(p)
                    frontier.append(p)

        indeg = {n: len(self.parents[n] & needed) for n in needed}
        ready = sorted(n for n, d in indeg.items() if d == 0)
        order: List[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for c in sorted(self.children.get(n, ())):
                if c in indeg:
                    indeg[c] -= 1
                    if indeg[c] == 0:
                        ready.append(c)
        if len(order) != len(needed):
            cycle = sorted(set(needed) - set(order))
            raise GraphError(f"dependency cycle among models: {', '.join(cycle)}")
        return order

    def ancestors(self, name: str) -> Set[str]:
        out, stack = set(), [name]
        while stack:
            for p in self.parents.get(stack.pop(), ()):
                if p not in out:
                    out.add(p)
                    stack.append(p)
        return out

    def descendants(self, name: str) -> Set[str]:
        out, stack = set(), [name]
        while stack:
            for c in self.children.get(stack.pop(), ()):
                if c not in out:
                    out.add(c)
                    stack.append(c)
        return out

    def select(self, selector: str) -> List[str]:
        """dbt-flavored selection: `name`, `+name`, `name+`, `+name+`, `tag:t`, `tag:t+`."""
        if not selector:
            return self.topo_order()
        chosen: Set[str] = set()
        for tok in selector.split():
            parents = tok.startswith("+")
            children = tok.endswith("+")
            core = tok.strip("+")
            if core.startswith("tag:"):
                tag = core[4:]
                seeds = [n for n, m in self.models.items() if tag in (m.config.tags or [])]
                if not seeds:
                    raise GraphError(f"no models with tag {tag!r}")
            elif core.startswith("resource_type:") or core.startswith("type:"):
                rt = core.split(":", 1)[1]
                seeds = [n for n, m in self.models.items() if m.resource_type == rt]
                if not seeds:
                    raise GraphError(f"no nodes with resource_type {rt!r}")
            else:
                if core not in self.models:
                    raise GraphError(f"selector {core!r} matches no model")
                seeds = [core]
            for s in seeds:
                chosen.add(s)
                if parents:
                    chosen |= self.ancestors(s)
                if children:
                    chosen |= self.descendants(s)
        # strict dbt semantics: only the selected nodes, in dependency order
        return [n for n in self.topo_order() if n in chosen]
