from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final, Generic, TypeVar

import anyio

T = TypeVar("T")  # rebind-ok: TypeVar declarations cannot have a variable annotation.
R = TypeVar("R")  # rebind-ok: TypeVar declarations cannot have a variable annotation.


@dataclass(frozen=True, slots=True)
class BatchResult(Generic[R]):
    value: R


class Batch(Generic[T, R]):
    def __init__(self) -> None:
        self.items: Final = list[T]()
        self.full: Final = anyio.Event()
        self.done: Final = anyio.Event()
        self.result: BatchResult[R] | Exception | None = None


class Batcher(Generic[T, R]):
    def __init__(self, execute: Callable[[tuple[T, ...]], Awaitable[R]], delay: float, size: int) -> None:
        self.execute = execute
        self.delay = delay
        self.size = size
        self.open: Batch[T, R] | None = None

    async def submit(self, item: T) -> R:
        leader: Final = self.open is None
        batch: Final = self.open or Batch[T, R]()
        self.open = batch
        batch.items.append(item)
        if len(batch.items) == self.size:
            self.seal(batch)
            batch.full.set()
        if leader:
            # The leading request also serves the other callers in this batch.
            with anyio.CancelScope(shield=True):
                with anyio.move_on_after(self.delay):
                    await batch.full.wait()
                self.seal(batch)
                try:
                    batch.result = BatchResult(await self.execute(tuple(batch.items)))
                except Exception as error:  # noqa: BLE001  # Forward the SQL failure to every caller in the batch.
                    batch.result = error
                finally:
                    batch.done.set()
        else:
            await batch.done.wait()
        result: Final = batch.result
        if isinstance(result, BatchResult):
            return result.value
        if isinstance(result, Exception):
            raise result
        raise RuntimeError("Batch ended without a result")

    def seal(self, batch: Batch[T, R]) -> None:
        if self.open is batch:
            self.open = None
