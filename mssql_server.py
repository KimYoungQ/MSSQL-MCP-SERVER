from typing import Any
import json
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass
from mcp.server.fastmcp import FastMCP
from api_client import api_client


@dataclass
class QueryPreview:
    """쿼리 미리보기 정보를 저장하는 데이터 클래스"""
    database: str
    query: str
    query_hash: str
    timestamp: datetime
    query_type: str
    context: dict


class QueryPreviewStore:
    """쿼리 미리보기를 저장하고 관리하는 클래스"""

    EXPIRY_MINUTES = 5

    def __init__(self):
        self._previews: dict[str, QueryPreview] = {}

    def _generate_hash(self, database: str, query: str) -> str:
        """SHA-256 해시 생성 (16자)"""
        content = f"{database}:{query}:{datetime.now().isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def store(self, database: str, query: str, query_type: str, context: dict) -> QueryPreview:
        """쿼리 미리보기 저장"""
        query_hash = self._generate_hash(database, query)
        preview = QueryPreview(
            database=database,
            query=query,
            query_hash=query_hash,
            timestamp=datetime.now(),
            query_type=query_type,
            context=context
        )
        self._previews[query_hash] = preview
        self._cleanup_expired()
        return preview

    def get(self, query_hash: str) -> QueryPreview | None:
        """해시로 미리보기 조회"""
        self._cleanup_expired()
        return self._previews.get(query_hash)

    def validate_and_get(self, query_hash: str, database: str) -> tuple[QueryPreview | None, str | None]:
        """미리보기 검증 및 조회. (preview, error) 튜플 반환"""
        preview = self.get(query_hash)

        if not preview:
            return None, "Query preview not found or expired. Please generate a new preview."

        if preview.database != database:
            return None, f"Database mismatch. Preview was for '{preview.database}', not '{database}'."

        if self._is_expired(preview):
            del self._previews[query_hash]
            return None, "Query preview has expired. Please generate a new preview."

        return preview, None

    def remove(self, query_hash: str) -> None:
        """미리보기 삭제"""
        if query_hash in self._previews:
            del self._previews[query_hash]

    def _is_expired(self, preview: QueryPreview) -> bool:
        """만료 여부 확인"""
        expiry_time = preview.timestamp + timedelta(minutes=self.EXPIRY_MINUTES)
        return datetime.now() > expiry_time

    def _cleanup_expired(self) -> None:
        """만료된 미리보기 정리"""
        expired_hashes = [
            h for h, p in self._previews.items() if self._is_expired(p)
        ]
        for h in expired_hashes:
            del self._previews[h]


# 전역 QueryPreviewStore 인스턴스
query_preview_store = QueryPreviewStore()

# Initialize FastMCP server
mcp = FastMCP("mssql")


def format_error(data: dict[str, Any] | None) -> str:
    """Format error response from API"""
    if not data:
        return "Unknown error: No response from API Gateway"
    return data.get("error", "Unknown error occurred")


def format_rows(rows: list[dict], max_display: int = 10) -> str:
    """Format rows for display"""
    if not rows:
        return "No data found."

    total = len(rows)
    display_rows = rows[:max_display]

    result = [f"Total rows: {total}"]
    if total > max_display:
        result.append(f"(Showing first {max_display} rows)")
    result.append("")

    for i, row in enumerate(display_rows, 1):
        result.append(f"Row {i}:")
        for key, value in row.items():
            result.append(f"  {key}: {value}")

    return "\n".join(result)


@mcp.tool()
async def list_tables(database: str) -> str:
    """List all tables in a database.

    Args:
        database: Database name (e.g., school, testdb)
    """
    data = await api_client.get(f"/databases/{database}/tables")

    if not data or "error" in data:
        return f"Failed to list tables: {format_error(data)}"

    tables = data.get("tables", [])
    count = data.get("count", len(tables))

    if not tables:
        return f"No tables found in database '{database}'."

    result = [f"Database: {database}", f"Table count: {count}", "", "Tables:"]
    for table in tables:
        result.append(f"  - {table}")

    return "\n".join(result)


@mcp.tool()
async def get_table_schema(database: str, table: str) -> str:
    """Get schema information for a table (columns, types, nullable).

    Args:
        database: Database name
        table: Table name
    """
    data = await api_client.get(f"/databases/{database}/tables/{table}/schema")

    if not data or "error" in data:
        return f"Failed to get schema: {format_error(data)}"

    columns = data.get("columns", [])

    if not columns:
        return f"No columns found for table '{table}' in database '{database}'."

    result = [
        f"Database: {database}",
        f"Table: {table}",
        f"Column count: {len(columns)}",
        "",
        "Columns:"
    ]

    for col in columns:
        nullable = "NULL" if col.get("nullable") else "NOT NULL"
        type_info = col.get("type", "unknown")
        if col.get("maxLength"):
            type_info += f"({col['maxLength']})"
        default = f" DEFAULT {col['defaultValue']}" if col.get("defaultValue") else ""
        result.append(f"  - {col['name']}: {type_info} {nullable}{default}")

    return "\n".join(result)


@mcp.tool()
async def preview_table_query(database: str, table: str, limit: int = 100) -> str:
    """Preview a table query before execution. User must approve before running.

    Args:
        database: Database name
        table: Table name
        limit: Maximum number of rows to return (default 100, max 1000)
    """
    safe_limit = min(max(1, limit), 1000)
    query = f"SELECT TOP {safe_limit} * FROM [{table}]"

    preview = query_preview_store.store(
        database=database,
        query=query,
        query_type="SELECT",
        context={"table": table, "limit": safe_limit}
    )

    result = [
        "=== QUERY PREVIEW ===",
        f"Database: {database}",
        f"Table: {table}",
        f"Limit: {safe_limit} rows",
        "",
        "SQL to be executed:",
        query,
        "",
        "Operation: SELECT (Read-only)",
        "",
        f'To execute, use query_hash: "{preview.query_hash}"',
        "This preview expires in 5 minutes."
    ]

    return "\n".join(result)


@mcp.tool()
async def preview_select_query(database: str, query: str) -> str:
    """Preview a custom SELECT query before execution. User must approve before running.

    Args:
        database: Database name
        query: SELECT query to preview (only SELECT queries allowed)
    """
    # 쿼리가 SELECT로 시작하는지 기본 검증
    trimmed = query.strip().upper()
    if not trimmed.startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."

    preview = query_preview_store.store(
        database=database,
        query=query,
        query_type="SELECT",
        context={"custom_query": True}
    )

    result = [
        "=== QUERY PREVIEW ===",
        f"Database: {database}",
        "",
        "SQL to be executed:",
        query,
        "",
        "Operation: SELECT (Read-only)",
        "",
        f'To execute, use query_hash: "{preview.query_hash}"',
        "This preview expires in 5 minutes."
    ]

    return "\n".join(result)


@mcp.tool()
async def analyze_table(database: str, table: str) -> str:
    """Get statistics and analysis for a table (row count, column count, size).

    Args:
        database: Database name
        table: Table name
    """
    # Get stats
    stats_data = await api_client.get(f"/databases/{database}/tables/{table}/stats")

    if not stats_data or "error" in stats_data:
        return f"Failed to analyze table: {format_error(stats_data)}"

    # Get schema for additional info
    schema_data = await api_client.get(f"/databases/{database}/tables/{table}/schema")

    result = [
        f"=== Table Analysis ===",
        f"Database: {database}",
        f"Table: {table}",
        "",
        "Statistics:",
        f"  - Row count: {stats_data.get('rowCount', 'N/A'):,}",
        f"  - Column count: {stats_data.get('columnCount', 'N/A')}",
    ]

    size_kb = stats_data.get("sizeKB")
    if size_kb is not None:
        if size_kb >= 1024:
            result.append(f"  - Size: {size_kb / 1024:.2f} MB")
        else:
            result.append(f"  - Size: {size_kb} KB")

    if schema_data and "columns" in schema_data:
        columns = schema_data.get("columns", [])
        result.append("")
        result.append("Column types summary:")

        type_counts: dict[str, int] = {}
        for col in columns:
            col_type = col.get("type", "unknown")
            type_counts[col_type] = type_counts.get(col_type, 0) + 1

        for col_type, count in sorted(type_counts.items()):
            result.append(f"  - {col_type}: {count}")

    return "\n".join(result)


@mcp.tool()
async def execute_confirmed_query(database: str, query_hash: str) -> str:
    """Execute a previously previewed and approved query.

    Args:
        database: Database name
        query_hash: The query_hash from a previous preview (16 character hash)
    """
    # 미리보기 검증 및 조회
    preview, error = query_preview_store.validate_and_get(query_hash, database)

    if error:
        return f"Error: {error}"

    # 쿼리 실행
    payload = {"query": preview.query}
    data = await api_client.post(f"/databases/{database}/query", payload)

    # 사용된 미리보기 삭제
    query_preview_store.remove(query_hash)

    if not data or "error" in data:
        return f"Failed to execute query: {format_error(data)}"

    rows = data.get("rows", [])
    limited = data.get("limited", False)

    result = [
        "=== QUERY EXECUTED ===",
        f"Database: {database}",
        f"Query: {preview.query}",
        f"Rows returned: {len(rows)}",
    ]

    if limited:
        result.append("(Note: TOP 1000 limit was applied)")

    result.append("")

    if not rows:
        result.append("Query returned no results.")
    else:
        result.append(format_rows(rows, max_display=10))

    return "\n".join(result)


@mcp.tool()
async def list_stored_procedures(database: str) -> str:
    """List all stored procedures in a database.

    Args:
        database: Database name (e.g., school, testdb)
    """
    data = await api_client.get(f"/databases/{database}/stored-procedures")

    if not data or "error" in data:
        return f"Failed to list stored procedures: {format_error(data)}"

    procedures = data.get("procedures", [])
    count = data.get("count", len(procedures))

    if not procedures:
        return f"No stored procedures found in database '{database}'."

    result = [f"Database: {database}", f"Stored procedure count: {count}", "", "Stored Procedures:"]
    for proc in procedures:
        name = proc.get("name", "Unknown")
        created = proc.get("created", "N/A")
        last_altered = proc.get("lastAltered", "N/A")
        result.append(f"  - {name}")
        result.append(f"      Created: {created}")
        result.append(f"      Last Altered: {last_altered}")

    return "\n".join(result)


@mcp.tool()
async def get_stored_procedure_definition(database: str, procedure: str) -> str:
    """Get the definition (source code) of a stored procedure.

    Args:
        database: Database name
        procedure: Stored procedure name
    """
    data = await api_client.get(f"/databases/{database}/stored-procedures/{procedure}/definition")

    if not data or "error" in data:
        return f"Failed to get stored procedure definition: {format_error(data)}"

    definition = data.get("definition")

    if not definition:
        return f"No definition found for stored procedure '{procedure}' in database '{database}'. (The definition may be encrypted or inaccessible)"

    result = [
        f"=== Stored Procedure Definition ===",
        f"Database: {database}",
        f"Procedure: {procedure}",
        "",
        "Definition:",
        definition
    ]

    return "\n".join(result)


@mcp.tool()
async def get_stored_procedure_parameters(database: str, procedure: str) -> str:
    """Get parameter information for a stored procedure.

    Args:
        database: Database name
        procedure: Stored procedure name
    """
    data = await api_client.get(f"/databases/{database}/stored-procedures/{procedure}/parameters")

    if not data or "error" in data:
        return f"Failed to get stored procedure parameters: {format_error(data)}"

    parameters = data.get("parameters", [])

    result = [
        f"=== Stored Procedure Parameters ===",
        f"Database: {database}",
        f"Procedure: {procedure}",
        f"Parameter count: {len(parameters)}",
        ""
    ]

    if not parameters:
        result.append("This stored procedure has no parameters.")
    else:
        result.append("Parameters:")
        for param in parameters:
            name = param.get("name", "Unknown")
            data_type = param.get("type", "unknown")
            mode = param.get("mode", "IN")
            max_length = param.get("maxLength")

            type_info = data_type
            if max_length:
                type_info += f"({max_length})"

            result.append(f"  - {name}: {type_info} ({mode})")

    return "\n".join(result)


@mcp.tool()
async def get_stored_procedure_info(database: str, procedure: str) -> str:
    """Get complete information for a stored procedure (definition + parameters).

    Args:
        database: Database name
        procedure: Stored procedure name
    """
    data = await api_client.get(f"/databases/{database}/stored-procedures/{procedure}")

    if not data or "error" in data:
        return f"Failed to get stored procedure info: {format_error(data)}"

    proc_name = data.get("procedure", procedure)
    created = data.get("created", "N/A")
    last_altered = data.get("lastAltered", "N/A")
    definition = data.get("definition")
    parameters = data.get("parameters", [])

    result = [
        f"=== Stored Procedure Info ===",
        f"Database: {database}",
        f"Procedure: {proc_name}",
        f"Created: {created}",
        f"Last Altered: {last_altered}",
        ""
    ]

    # Parameters section
    result.append(f"Parameters ({len(parameters)}):")
    if not parameters:
        result.append("  (No parameters)")
    else:
        for param in parameters:
            name = param.get("name", "Unknown")
            data_type = param.get("type", "unknown")
            mode = param.get("mode", "IN")
            max_length = param.get("maxLength")

            type_info = data_type
            if max_length:
                type_info += f"({max_length})"

            result.append(f"  - {name}: {type_info} ({mode})")

    # Definition section
    result.append("")
    result.append("Definition:")
    if definition:
        result.append(definition)
    else:
        result.append("  (Definition not available - may be encrypted or inaccessible)")

    return "\n".join(result)


def main():
    # Run MCP server with stdio transport
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
