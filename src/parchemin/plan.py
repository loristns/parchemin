from collections.abc import Hashable, Iterable, Iterator
from dataclasses import dataclass
from typing import Literal, TypeGuard, cast

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


@dataclass(slots=True)
class Resource[R]:
    key: Hashable
    target: Target[R]
    current: R | None
    desired: R | None

    _decision: Literal["create", "delete", "update", "replace", "noop"] | None = None

    @property
    def decision(self) -> Literal["create", "delete", "update", "replace", "noop"]:
        if self._decision is not None:
            return self._decision

        match self:
            case Resource(current=None, desired=desired):
                self._decision = "create"
            case Resource(current=current, desired=None):
                self._decision = "delete"
            case Resource(current=current, desired=desired) if current != desired:
                if self.target.can_update(
                    current=cast(R, current), desired=cast(R, desired)
                ):
                    self._decision = "update"
                else:
                    self._decision = "replace"
            case _:
                self._decision = "noop"

        return self._decision

    @decision.setter
    def decision(
        self, decision: Literal["create", "delete", "update", "replace", "noop"]
    ) -> None:
        self._decision = decision


def plan(desired: Iterable[object], targets: Iterable[Target]) -> Plan:
    targets = tuple(targets)

    resources: dict[Hashable, Resource] = {}

    current_graph: Graph[Hashable, Resource] = Graph()
    desired_graph: Graph[Hashable, Resource] = Graph()
    current_dependencies: dict[Hashable, set[Hashable]] = {}
    desired_dependencies: dict[Hashable, set[Hashable]] = {}

    for desired_item in desired:
        for target in targets:
            if target.matches(desired_item):
                break
        else:
            # ignore desired items that don't match any target
            continue

        key = target.key(desired_item)
        current_item = target.get(key)

        if key in resources:
            raise ValueError(f"Resource {key!r} is declared multiple times")

        resources[key] = Resource(
            key=key, target=target, current=current_item, desired=desired_item
        )

        if current_item is not None:
            current_graph.add_node(key, resources[key])
        desired_graph.add_node(key, resources[key])

    for target in targets:
        try:
            current_items = target.get_all()
        except NotImplementedError:
            continue

        for current_item in current_items:
            key = target.key(current_item)

            resources.setdefault(
                key,
                Resource(key=key, target=target, current=current_item, desired=None),
            )
            if key not in current_graph.nodes:
                current_graph.add_node(key, resources[key])

    for resource in resources.values():
        if resource.current is not None:
            current_dependencies[resource.key] = set(
                resource.target.depends_on(resource.current)
            )
            for dependency_key in current_dependencies[resource.key]:
                current_graph.add_edge(dependency_key, resource.key)

        if resource.desired is not None:
            desired_dependencies[resource.key] = set(
                resource.target.depends_on(resource.desired)
            )
            for dependency_key in desired_dependencies[resource.key]:
                desired_graph.add_edge(dependency_key, resource.key)

    for key in current_graph.topological_sort():
        resource = resources[key]
        for dependency_key in current_graph.edges.get(key, []):
            dependency = resources[dependency_key]
            if resource.decision in [
                "create",
                "replace",
            ] and dependency.decision not in ["create", "replace", "delete"]:
                dependency.decision = "replace"

    # Build an action graph where each node is an executable step.
    # The topological order of this graph is the final execution plan.
    ActionKey = tuple[Literal["create", "update", "delete"], Hashable]
    action_graph: Graph[ActionKey, Step] = Graph()
    delete_actions: dict[Hashable, ActionKey] = {}
    create_actions: dict[Hashable, ActionKey] = {}
    update_actions: dict[Hashable, ActionKey] = {}

    for key, resource in resources.items():
        if resource.decision == "delete":
            if resource.current is None:
                raise ValueError(f"Resource {key!r} cannot be deleted: missing current")
            action_key: ActionKey = ("delete", key)
            delete_actions[key] = action_key
            action_graph.add_node(action_key, DeleteStep(resource.target, resource.current))
            continue

        if resource.decision == "create":
            if resource.desired is None:
                raise ValueError(f"Resource {key!r} cannot be created: missing desired")
            action_key = ("create", key)
            create_actions[key] = action_key
            action_graph.add_node(action_key, CreateStep(resource.target, resource.desired))
            continue

        if resource.decision == "update":
            if resource.current is None or resource.desired is None:
                raise ValueError(f"Resource {key!r} cannot be updated")
            action_key = ("update", key)
            update_actions[key] = action_key
            action_graph.add_node(
                action_key, UpdateStep(resource.target, resource.current, resource.desired)
            )
            continue

        if resource.decision == "replace":
            if resource.current is None or resource.desired is None:
                raise ValueError(f"Resource {key!r} cannot be replaced")

            delete_key: ActionKey = ("delete", key)
            create_key: ActionKey = ("create", key)
            delete_actions[key] = delete_key
            create_actions[key] = create_key
            action_graph.add_node(delete_key, DeleteStep(resource.target, resource.current))
            action_graph.add_node(create_key, CreateStep(resource.target, resource.desired))
            action_graph.add_edge(delete_key, create_key)

    # Current dependency edges: dependency -> dependant
    # If dependency is deleted, dependant must first stop depending on it.
    for dependency_key, dependant_keys in current_graph.edges.items():
        if dependency_key not in delete_actions:
            continue

        dependency_delete_action = delete_actions[dependency_key]

        for dependant_key in dependant_keys:
            dependant = resources[dependant_key]

            if dependant.decision in ["delete", "replace"]:
                detach_action = delete_actions[dependant_key]
                action_graph.add_edge(detach_action, dependency_delete_action)
                continue

            if dependant.decision == "update":
                still_depends_after_update = dependency_key in desired_dependencies.get(
                    dependant_key, set()
                )
                if still_depends_after_update:
                    raise ValueError(
                        f"Resource {dependency_key!r} is scheduled for deletion, "
                        f"but resource {dependant_key!r} still depends on it after update."
                    )

                detach_action = update_actions[dependant_key]
                action_graph.add_edge(detach_action, dependency_delete_action)
                continue

            if dependant.decision == "noop":
                raise ValueError(
                    f"Resource {dependency_key!r} is scheduled for deletion, "
                    f"but resource {dependant_key!r} still depends on it."
                )

    # Desired dependency edges: dependency -> dependant
    # If dependency is created/replaced, dependant action that realizes desired state
    # must run after dependency creation.
    for dependency_key, dependant_keys in desired_graph.edges.items():
        dependency = resources[dependency_key]
        if dependency.decision == "delete":
            raise ValueError(
                f"Resource {dependency_key!r} is scheduled for deletion, "
                "but desired state still depends on it."
            )

        dependency_ready_action = create_actions.get(dependency_key)
        if dependency_ready_action is None:
            continue

        for dependant_key in dependant_keys:
            dependant = resources[dependant_key]

            if dependant.decision == "update":
                dependant_action = update_actions[dependant_key]
            elif dependant.decision in ["create", "replace"]:
                dependant_action = create_actions[dependant_key]
            else:
                continue

            action_graph.add_edge(dependency_ready_action, dependant_action)

    steps = [action_graph.nodes[action_key] for action_key in action_graph.topological_sort()]
    return Plan(steps)


if __name__ == "__main__":
    from typing import TypedDict

    class KindDict[K](TypedDict):
        kind: K
        name: str

    class KindTarget[K, T: KindDict](Target[T]):
        def __init__(self, kind: K, store: list[T]):
            self.kind = kind
            self.store = store

        def matches(self, resource: object) -> TypeGuard[T]:
            return isinstance(resource, dict) and resource["kind"] == self.kind

        def key(self, resource: T) -> Hashable:
            return resource["name"]

        def get_all(self) -> Iterable[T]:
            return self.store

        def create(self, resource: T) -> None:
            self.store.append(resource)

        def delete(self, resource: T) -> None:
            self.store.remove(resource)

    class Ingredient(KindDict[Literal["ingredient"]]):
        pass

    class Recipe(KindDict[Literal["recipe"]]):
        ingredients: list[str]

    class IngredientTarget(KindTarget[Literal["ingredient"], Ingredient]):
        def __init__(self, store: list[Ingredient]):
            super().__init__("ingredient", store)

    class RecipeTarget(KindTarget[Literal["recipe"], Recipe]):
        def __init__(self, store: list[Recipe]):
            super().__init__("recipe", store)

        def depends_on(self, resource: Recipe) -> Iterable[Hashable]:
            return resource["ingredients"]

    ingredients: list[Ingredient] = [
        {"kind": "ingredient", "name": "egg"},
        {"kind": "ingredient", "name": "flour"},
        {"kind": "ingredient", "name": "butter"},
        {"kind": "ingredient", "name": "milk"},
        {"kind": "ingredient", "name": "sugar"},
        {"kind": "ingredient", "name": "salt"},
    ]

    recipes: list[Recipe] = [
        {
            "kind": "recipe",
            "name": "cake",
            "ingredients": ["egg", "flour", "sugar", "salt"],
        },
        {"kind": "recipe", "name": "bread", "ingredients": ["flour", "salt"]},
        {
            "kind": "recipe",
            "name": "toast",
            "ingredients": ["bread", "butter"],
        },
    ]

    ingredient_target = IngredientTarget(ingredients)
    recipe_target = RecipeTarget(recipes)

    p = plan(
        desired=[
            {"kind": "ingredient", "name": "egg"},
            {"kind": "ingredient", "name": "flour"},
            {"kind": "ingredient", "name": "butter"},
            # {"kind": "ingredient", "name": "milk"},
            {"kind": "ingredient", "name": "sugar"},  # delete cause error
            {"kind": "ingredient", "name": "salt"},
            {"kind": "ingredient", "name": "jam"},  # new ingredient
            {
                "kind": "recipe",
                "name": "cake",
                "ingredients": ["egg", "flour", "sugar", "salt"],
            },
            {"kind": "recipe", "name": "bread", "ingredients": ["flour"]},
            {
                "kind": "recipe",
                "name": "toast",
                "ingredients": ["bread", "butter"],
            },
            {
                "kind": "recipe",
                "name": "sandwich",
                "ingredients": ["toast", "jam"],
            },
        ],
        targets=[ingredient_target, recipe_target],
    )
    print(p)
