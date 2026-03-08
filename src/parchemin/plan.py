from collections.abc import Hashable, Iterable, Iterator
from typing import Literal

from parchemin.graph import Graph
from parchemin.step import CreateStep, DeleteStep, Step, UpdateStep
from parchemin.target import Target


class Plan:
    def __init__(self, steps: Iterable[Step]):
        self.steps = tuple(steps)

    def __iter__(self) -> Iterator[Step]:
        return iter(self.steps)

    def __len__(self) -> int:
        return len(self.steps)

    def __bool__(self) -> bool:
        return bool(self.steps)

    def __getitem__(self, index: int) -> Step:
        return self.steps[index]

    def apply(self) -> None:
        for step in self.steps:
            step.apply()

    def __repr__(self) -> str:
        if not self.steps:
            return "Plan([])"

        rendered_steps = "".join(f"{step!r},\n" for step in self.steps)
        rendered_steps = "\n".join([
            f"    {line}" for line in rendered_steps.split("\n")
        ])

        return f"Plan([\n{rendered_steps}\n])"


type GraphKey = tuple[Literal["deleted", "present"], Hashable]


def plan(desired: Iterable[object], targets: Iterable[Target]) -> Plan:
    steps = Graph[GraphKey, Step]()
    desired_keys: set[Hashable] = set()

    targets = list(targets)

    for desired_item in desired:
        for target in targets:
            if target.matches(desired_item):
                break
        else:
            # ignore desired items that don't match any target
            continue

        key = target.key(resource=desired_item)
        desired_keys.add(key)

        current_item = target.get(key)

        dependencies = target.depends_on(resource=desired_item)

        if current_item is None:
            steps.add_node(
                ("present", key), CreateStep(target=target, resource=desired_item)
            )
            for dependency_key in dependencies:
                steps.add_edge(("present", dependency_key), ("present", key))

        elif current_item != desired_item:
            if target.can_update(current=current_item, desired=desired_item):
                steps.add_node(
                    ("present", key),
                    UpdateStep(
                        target=target, current=current_item, desired=desired_item
                    ),
                )
            else:
                steps.add_node(
                    ("deleted", key), DeleteStep(target=target, resource=current_item)
                )
                steps.add_node(
                    ("present", key), CreateStep(target=target, resource=desired_item)
                )
                steps.add_edge(("deleted", key), ("present", key))

            for dependency_key in dependencies:
                steps.add_edge(("present", dependency_key), ("present", key))
        else:
            continue

    for target in targets:
        try:
            current_items = target.get_all()
        except NotImplementedError:
            continue

        for current_item in current_items:
            key = target.key(resource=current_item)
            if key in desired_keys:
                continue

            dependencies = target.depends_on(resource=current_item)

            steps.add_node(
                ("deleted", key),
                DeleteStep(target=target, resource=current_item)
            )
            for dependency_key in dependencies:
                steps.add_edge(("deleted", key), ("deleted", dependency_key))

    return Plan(steps.nodes[key] for key in steps.topological_sort())


if __name__ == "__main__":
    from dataclasses import dataclass
    from datetime import datetime

    billing_entries = []

    class BillingTarget(Target[dict]):
        def __init__(self, server_id):
            self.server_id = server_id

        def matches(self, resource):
            return (
                isinstance(resource, dict) and resource["server_id"] == self.server_id
            )

        def key(self, resource):
            return (resource["server_id"], resource["kind"])

        def get_all(self):
            return [
                e
                for e in billing_entries
                if e["server_id"] == self.server_id and e["ended_at"] is None
            ]

        def create(self, resource):
            billing_entries.append({
                **resource,
                "started_at": datetime.now(),
                "ended_at": None,
            })

        def delete(self, resource):
            resource["ended_at"] = datetime.now()

    @dataclass
    class Server:
        id: str
        flavor: str
        running: bool = False

        def desired_billing(self):
            if not self.running:
                return []

            return [
                {
                    "server_id": self.id,
                    "kind": "compute",
                    "flavor": self.flavor,
                }
            ]

        def sync(self):
            p = plan(
                self.desired_billing(),
                [BillingTarget(self.id)],
            )
            print(p)
            p.apply()

        def start(self):
            self.running = True
            self.sync()

        def stop(self):
            self.running = False
            self.sync()

        def change_flavor(self, flavor):
            self.flavor = flavor
            self.sync()

    server = Server(id="server-1", flavor="m1.small")
    server.start()
    server.change_flavor("m1.medium")
    server.stop()
