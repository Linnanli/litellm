from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from importlib.resources import files
from typing import Final

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import JsonValue, TypeAdapter

from .contracts import JsonObject, PublicError

_ROWS: Final = TypeAdapter(tuple[dict[str, JsonValue], ...])


class Database:
    def __init__(self, pool: AsyncConnectionPool[AsyncConnection[tuple[object, ...]]]) -> None:
        self.pool = pool

    async def migrate(self) -> None:
        schema: Final = files("daswork_enterprise").joinpath("schema.sql").read_bytes()
        async with self.pool.connection() as connection:
            await connection.execute(schema)

    async def rows(self, statement: str, parameters: Sequence[object] | Mapping[str, object] = ()) -> tuple[JsonObject, ...]:
        async with self.pool.connection() as connection, connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(statement.encode(), parameters or None)
            return _ROWS.validate_python(await cursor.fetchall())

    async def one(self, statement: str, parameters: Sequence[object] | Mapping[str, object] = ()) -> JsonObject:
        records: Final = await self.rows(statement, parameters)
        if not records:
            raise PublicError(404, "record_not_found")
        return records[0]


def instant(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
