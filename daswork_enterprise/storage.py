from collections.abc import Sequence
from typing import TYPE_CHECKING, Final

from pydantic import JsonValue, TypeAdapter

from .contracts import JsonObject, PublicError

_ROWS: Final = TypeAdapter(tuple[dict[str, JsonValue], ...])


if TYPE_CHECKING:
    from litellm.proxy.db.prisma_client import PrismaWrapper


class Database:
    def __init__(self, writer: "PrismaWrapper") -> None:
        self.writer = writer

    async def rows(self, statement: str, parameters: Sequence[object] = ()) -> tuple[JsonObject, ...]:
        raw: Final[object] = await self.writer.query_raw(statement, *parameters)  # pyright: ignore[reportAny]  # Prisma forwards raw responses dynamically
        return _ROWS.validate_python(raw)

    async def one(self, statement: str, parameters: Sequence[object] = ()) -> JsonObject:
        records: Final = await self.rows(statement, parameters)
        if not records:
            raise PublicError(404, "record_not_found")
        return records[0]
