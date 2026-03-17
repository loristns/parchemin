import unittest
from collections.abc import Hashable, Iterable
from typing import Literal, TypedDict, TypeGuard

from parchemin.plan import plan
from parchemin.step import CreateStep, DeleteStep, UpdateStep
from parchemin.target import Target


class KindDict[K](TypedDict):
    kind: K
    name: str


class KindTarget[K, T: KindDict](Target[T]):
    def __init__(self, kind: K, store: list[T]):
        self.kind = kind
        self.store = store

    def matches(self, resource: object) -> TypeGuard[T]:
        return isinstance(resource, dict) and resource.get("kind") == self.kind

    def key(self, resource: T) -> Hashable:
        return resource["name"]

    def get_all(self) -> Iterable[T]:
        return self.store

    def create(self, resource: T) -> None:
        self.store.append(resource)

    def update(self, current: T, desired: T) -> None:
        index = self.store.index(current)
        self.store[index] = desired

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


class PlanActionGraphTests(unittest.TestCase):
    def test_update_that_removes_dependency_runs_before_delete(self) -> None:
        ingredients: list[Ingredient] = [
            {"kind": "ingredient", "name": "milk"},
            {"kind": "ingredient", "name": "flour"},
        ]
        recipes: list[Recipe] = [
            {
                "kind": "recipe",
                "name": "pancake",
                "ingredients": ["milk", "flour"],
            }
        ]

        planned = plan(
            desired=[
                {"kind": "ingredient", "name": "flour"},
                {"kind": "recipe", "name": "pancake", "ingredients": ["flour"]},
            ],
            targets=[IngredientTarget(ingredients), RecipeTarget(recipes)],
        )

        self.assertEqual(len(planned), 2)
        self.assertIsInstance(planned[0], UpdateStep)
        self.assertIsInstance(planned[1], DeleteStep)
        self.assertEqual(planned[0].desired["name"], "pancake")
        self.assertEqual(planned[1].resource["name"], "milk")

    def test_delete_is_rejected_when_dependant_keeps_dependency(self) -> None:
        ingredients: list[Ingredient] = [{"kind": "ingredient", "name": "milk"}]
        recipes: list[Recipe] = [
            {"kind": "recipe", "name": "pancake", "ingredients": ["milk"]}
        ]

        with self.assertRaises(ValueError):
            plan(
                desired=[
                    {"kind": "recipe", "name": "pancake", "ingredients": ["milk"]},
                ],
                targets=[IngredientTarget(ingredients), RecipeTarget(recipes)],
            )

    def test_create_dependency_runs_before_create_dependant(self) -> None:
        planned = plan(
            desired=[
                {"kind": "ingredient", "name": "jam"},
                {"kind": "recipe", "name": "toast", "ingredients": ["jam"]},
            ],
            targets=[IngredientTarget([]), RecipeTarget([])],
        )

        self.assertEqual(len(planned), 2)
        self.assertIsInstance(planned[0], CreateStep)
        self.assertIsInstance(planned[1], CreateStep)
        self.assertEqual(planned[0].resource["name"], "jam")
        self.assertEqual(planned[1].resource["name"], "toast")

    def test_create_dependency_runs_before_update_dependant(self) -> None:
        recipes: list[Recipe] = [
            {"kind": "recipe", "name": "toast", "ingredients": []},
        ]

        planned = plan(
            desired=[
                {"kind": "ingredient", "name": "jam"},
                {"kind": "recipe", "name": "toast", "ingredients": ["jam"]},
            ],
            targets=[IngredientTarget([]), RecipeTarget(recipes)],
        )

        self.assertEqual(len(planned), 2)
        self.assertIsInstance(planned[0], CreateStep)
        self.assertIsInstance(planned[1], UpdateStep)
        self.assertEqual(planned[0].resource["name"], "jam")
        self.assertEqual(planned[1].desired["name"], "toast")


if __name__ == "__main__":
    unittest.main()
