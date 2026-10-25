"""Dependency graph over models, plus dbt-style node selection."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from .errors import GraphError
from .model import Model


class Graph:
    """Models only: sources are leaf reads handled by the pipeline engine."""

    def __init__(self, models: Dict[str, Model],
                 state: Optional[Dict[str, str]] = None):
        self.models = {n: m for n, m in models.items() if m.config.enabled}
        # name -> checksum from a previous manifest, for state: selectors
        self._state = state or {}
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
        """dbt-flavored selection.

        Whitespace separates unions; commas inside a token intersect, e.g.
        `tag:gold,marts` = marts models also tagged gold. `+` edges work per
        sub-token. Kinds: `name`, `dir`, `path:p`, `fqn:a.b`, `tag:t`,
        `source:s.t`, `resource_type:rt`, `config:k[:v]`, `state:x`.
        """
        if not selector:
            return self.topo_order()
        chosen: Set[str] = set()
        for tok in selector.split():
            group: Optional[Set[str]] = None
            for sub in tok.split(","):
                seeds = self._seeds(sub)
                group = seeds if group is None else (group & seeds)
            chosen |= group or set()
        # strict dbt semantics: only the selected nodes, in dependency order
        return [n for n in self.topo_order() if n in chosen]

    def _seeds(self, tok: str) -> Set[str]:
        """One selection atom -> matched node set (with +/- expansion)."""
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
        elif core.startswith("source:"):
            key = core[7:]
            seeds = [n for n, m in self.models.items() if key in m.sources]
            if not seeds:
                raise GraphError(f"no models read source {key!r}")
        elif core.startswith("path:"):
            pref = core[5:].strip("/")
            seeds = [
                n for n, m in self.models.items()
                if m.fqn_dir == pref or m.fqn_dir.startswith(pref + "/")
                or str(m.rel_path).startswith(pref + "/")
                or f"/{pref}/" in str(m.rel_path)
            ]
            if not seeds:
                raise GraphError(f"no models under path {pref!r}")
        elif core.startswith("fqn:"):
            pref = core[4:].strip(".")
            seeds = [n for n, m in self.models.items()
                     if self._fqn(m) == pref or self._fqn(m).startswith(pref + ".")]
            if not seeds:
                raise GraphError(f"no models under fqn {pref!r}")
        elif core.startswith("config:"):
            spec = core[7:]
            key, _, val = spec.partition(":")
            seeds = [
                n for n, m in self.models.items()
                if hasattr(m.config, key) and (
                    not val or str(getattr(m.config, key)) == val
                ) and (not val or getattr(m.config, key) is not None)
            ]
            if not seeds:
                raise GraphError(f"no models with config {spec!r}")
        elif core.startswith("state:"):
            kind = core[6:]
            if not self._state:
                raise GraphError(
                    f"state: selector needs a manifest — pass --state <dir> "
                    f"(or compile once so target/manifest.json exists)"
                )
            seeds = [n for n in self.models if self._state_match(n, kind)]
            if not seeds:
                raise GraphError(f"no models matched state:{kind}")
        else:
            if core in self.models:
                seeds = [core]
            else:
                # bare directory name: dbt fqn semantics — `staging` selects
                # everything under models/staging/
                seeds = [
                    n for n, m in self.models.items()
                    if m.fqn_dir == core or m.fqn_dir.startswith(core + "/")
                    or f"/{core}" in m.fqn_dir
                ]
                if not seeds:
                    raise GraphError(f"selector {core!r} matches no model")
        out = set(seeds)
        for s in seeds:
            if parents:
                out |= self.ancestors(s)
            if children:
                out |= self.descendants(s)
        return out

    @staticmethod
    def _fqn(m: Model) -> str:
        return ((m.fqn_dir or "") + "." + m.name).strip(".").replace("/", ".")

    def _state_match(self, name: str, kind: str) -> bool:
        cur = self.models[name].checksum
        old = self._state.get(name)
        if kind == "new":
            return old is None
        if kind == "modified":
            return old != cur
        if kind == "unmodified":
            return old == cur
        raise GraphError(
            f"unknown state:{kind}; expected new|modified|unmodified"
        )
