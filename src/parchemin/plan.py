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
            for dependency_key in resource.target.depends_on(resource.current):
                current_graph.add_edge(dependency_key, resource.key)

        if resource.desired is not None:
            for dependency_key in resource.target.depends_on(resource.desired):
                desired_graph.add_edge(dependency_key, resource.key)

    for key in current_graph.topological_sort():
        resource = resources[key]
        print(f"{key}: {resource.decision}")
        for dependency_key in current_graph.edges.get(key, []):
            dependency = resources[dependency_key]
            print("  -", dependency_key, ":", dependency.decision)
            if resource.decision in [
                "create",
                "replace",
            ] and dependency.decision not in ["create", "replace", "delete"]:
                dependency.decision = "replace"
                print("   -> replace")
            elif resource.decision == "delete" and dependency.decision != "delete":
                raise ValueError(
                    f"Resource {key} is scheduled for deletion, "
                    "but dependency {dependency_key} is in a state that prevents it."
                )

    steps: list[Step] = []

    # Delete phase
    for key in current_graph.topological_sort():
        resource = resources[key]
        if resource.decision == "delete" or resource.decision == "replace":
            steps.insert(0, DeleteStep(resource.target, resource.current))

    # Create phase
    for key in desired_graph.topological_sort():
        resource = resources[key]
        if resource.decision == "create" or resource.decision == "replace":
            steps.append(CreateStep(resource.target, resource.desired))
        elif resource.decision == "update":
            steps.append(
                UpdateStep(resource.target, resource.current, resource.desired)
            )

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
