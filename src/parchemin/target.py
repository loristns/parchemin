from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from typing import TypeGuard


class Target[R](ABC):
    @abstractmethod
    def matches(self, resource: object) -> TypeGuard[R]:
        raise NotImplementedError

    @abstractmethod
    def key(self, resource: R) -> Hashable:
        raise NotImplementedError

    def get_all(self) -> Iterable[R]:
        raise NotImplementedError

    def get(self, key: Hashable) -> R | None:
        for resource in self.get_all():
            if self.key(resource) == key:
                return resource

        return None

    def create(self, resource: R) -> None:
        raise NotImplementedError

    def can_update(self, current: R, desired: R) -> bool:  # noqa: ARG002
        # default implementation returns True if update is overriden by child class
        return type(self).update is not Target.update

    def update(self, current: R, desired: R) -> None:
        raise NotImplementedError

    def delete(self, resource: R) -> None:
        raise NotImplementedError

    def depends_on(self, resource: R) -> Iterable[Hashable]:  # noqa: ARG002
        return ()
