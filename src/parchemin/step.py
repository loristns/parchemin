from abc import ABC, abstractmethod
from dataclasses import dataclass

from parchemin.target import Target


class Step[R](ABC):
    target: Target[R]

    @abstractmethod
    def apply(self) -> None:
        pass


@dataclass(frozen=True, slots=True)
class CreateStep[R](Step[R]):
    target: Target[R]
    resource: R

    def apply(self) -> None:
        self.target.create(self.resource)

    def __repr__(self) -> str:
        return (
            f"{type(self.target).__name__}.create(\n    resource={self.resource!r},\n)"
        )


@dataclass(frozen=True, slots=True)
class UpdateStep[R](Step[R]):
    target: Target[R]
    current: R
    desired: R

    def apply(self) -> None:
        self.target.update(self.current, self.desired)

    def __repr__(self) -> str:
        return (
            f"{type(self.target).__name__}.update(\n"
            f"    current={self.current!r},\n"
            f"    desired={self.desired!r},\n"
            f")"
        )


@dataclass(frozen=True, slots=True)
class DeleteStep[R](Step[R]):
    target: Target[R]
    resource: R

    def apply(self) -> None:
        self.target.delete(self.resource)

    def __repr__(self) -> str:
        return (
            f"{type(self.target).__name__}.delete(\n    resource={self.resource!r},\n)"
        )
