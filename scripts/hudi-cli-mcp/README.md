# Hudi CLI MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that exposes Apache Hudi CLI operations as tools for AI assistants. This enables LLMs like Claude, ChatGPT, and others to inspect, diagnose, and operate on Hudi tables through natural language.

## Overview

The server wraps the Hudi CLI (`hudi-cli`) in a structured MCP interface with:

- **40+ tools** covering table inspection, timeline analysis, compaction, clustering, cleaning, rollbacks, savepoints, and more
- **Read/write separation** — read-only commands execute immediately; write operations go through a tiered safety protocol
- **Token-based confirmation** — MEDIUM and HIGH risk operations require explicit confirmation before execution
- **Composite workflows** — multi-step operations (health checks, storage analysis, guided rollbacks) are bundled into single tools
- **Structured output** — raw Hudi CLI ASCII tables are parsed into structured JSON for reliable consumption by LLMs

## Prerequisites

- **Python 3.10+**
- **Apache Spark** — a local Spark installation (3.x recommended)
- **Hudi CLI bundles** — `hudi-cli-bundle` and `hudi-spark-bundle` JAR files
- **Java 8 or 11**

### Environment Variables

The following environment variables must be set before starting the server:

| Variable | Description | Example |
|----------|-------------|---------|
| `SPARK_HOME` | Path to your Spark installation | `/opt/spark-3.5.3-bin-hadoop3` |
| `CLI_BUNDLE_JAR` | Path to `hudi-cli-bundle` JAR | `/path/to/hudi-cli-bundle_2.12-0.14.1.jar` |
| `SPARK_BUNDLE_JAR` | Path to `hudi-spark-bundle` JAR | `/path/to/hudi-spark3.5-bundle_2.12-0.14.1.jar` |

## Installation

```bash
cd scripts/hudi-cli-mcp
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The only dependency is `fastmcp>=2.0.0`.

## Running the Server

```bash
# Set required environment variables
export SPARK_HOME=/path/to/spark
export CLI_BUNDLE_JAR=/path/to/hudi-cli-bundle_2.12-0.14.1.jar
export SPARK_BUNDLE_JAR=/path/to/hudi-spark3.5-bundle_2.12-0.14.1.jar

# Start the server (stdio transport, used by most MCP clients)
python server.py
```

## Configuring MCP Clients

### Claude Desktop

Add the following to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "hudi-cli": {
      "command": "/path/to/hudi/scripts/hudi-cli-mcp/venv/bin/python",
      "args": ["/path/to/hudi/scripts/hudi-cli-mcp/server.py"],
      "env": {
        "SPARK_HOME": "/path/to/spark",
        "CLI_BUNDLE_JAR": "/path/to/hudi-cli-bundle_2.12-0.14.1.jar",
        "SPARK_BUNDLE_JAR": "/path/to/hudi-spark3.5-bundle_2.12-0.14.1.jar"
      }
    }
  }
}
```

### Claude Code (CLI)

Add to your MCP settings (`~/.claude/mcp.json`):

```json
{
  "mcpServers": {
    "hudi-cli": {
      "command": "/path/to/hudi/scripts/hudi-cli-mcp/venv/bin/python",
      "args": ["/path/to/hudi/scripts/hudi-cli-mcp/server.py"],
      "env": {
        "SPARK_HOME": "/path/to/spark",
        "CLI_BUNDLE_JAR": "/path/to/hudi-cli-bundle_2.12-0.14.1.jar",
        "SPARK_BUNDLE_JAR": "/path/to/hudi-spark3.5-bundle_2.12-0.14.1.jar"
      }
    }
  }
}
```

### Other MCP Clients

Any MCP-compatible client can connect to this server. The server uses **stdio transport** by default. Point your client at `server.py` and provide the required environment variables.

## Available Tools

### Connection Management

| Tool | Description |
|------|-------------|
| `connect_to_table` | Connect to a Hudi table at a given path (local or cloud). Must be called first. |
| `disconnect` | Disconnect from the current table. |
| `show_connection` | Show current connection status and table path. |

### Read-Only Commands

| Tool | Description |
|------|-------------|
| `execute_hudi_command` | Execute any single read-only Hudi CLI command (e.g., `commits show --limit 10`). |
| `execute_hudi_commands` | Execute multiple read-only commands in a single batch. |

These tools accept any read-only Hudi CLI command. Examples:

```
commits show --limit 10 --desc true
timeline show incomplete
cleans show
compactions show all
partitions show
stats wa
commit showpartitions --commit <instant>
commit showfiles --commit <instant>
```

### Read-Only Workflows

Composite tools that run multiple commands and return consolidated results:

| Tool | Description |
|------|-------------|
| `table_overview` | Table description + partition listing. |
| `table_health_check` | Comprehensive health check: desc, recent commits, cleans, compactions, and stats. |
| `timeline_summary` | Active timeline + incomplete instants. |
| `commit_details` | Partition details, file-level details, and write stats for a specific commit. |
| `storage_analysis` | File sizes, filesystem view, and metadata stats. |

### Write Operations (with Safety Tiers)

Write operations are classified into three risk levels:

#### LOW Risk (execute immediately)

| Tool | Description |
|------|-------------|
| `create_savepoint` | Create a savepoint at a specific commit instant. |
| `toggle_lock_audit` | Enable or disable lock auditing. |
| `schedule_compaction` | Schedule a compaction operation. |
| `schedule_clustering` | Schedule a clustering operation. |

#### MEDIUM Risk (require confirmation)

| Tool | Description |
|------|-------------|
| `unschedule_compaction` | Remove a scheduled compaction. |
| `delete_savepoint` | Delete a savepoint. |
| `delete_markers` | Delete marker files for a commit. |
| `manage_metadata` | Create, delete, or initialize metadata table. |
| `trigger_archival` | Trigger timeline archival. |
| `repair_table` | Repair table by removing dangling data files. |
| `recover_table_configs` | Recover corrupted table properties. |
| `update_table_configs` | Update table configuration properties. |
| `delete_table_configs` | Delete table configuration properties. |
| `upgrade_or_downgrade_table` | Upgrade or downgrade the table version. |

#### HIGH Risk (require confirmation with dry-run preview)

| Tool | Description |
|------|-------------|
| `rollback_commit` | Roll back a specific commit. Destructive and irreversible. |
| `rollback_to_savepoint` | Roll back the table to a savepoint. All commits after it are undone. |
| `run_compaction` | Execute a compaction operation. |
| `run_clustering` | Execute a clustering operation. |
| `run_clean` | Execute cleaning to delete old file versions. |

### Confirmation Tools

| Tool | Description |
|------|-------------|
| `confirm_operation` | Confirm and execute a pending write operation using its token. |
| `cancel_operation` | Cancel a pending write operation. |
| `list_pending_operations` | List all pending operations awaiting confirmation. |

### Write Workflows (Guided Multi-Step)

| Tool | Description |
|------|-------------|
| `compaction_workflow` | Show pending compactions, then prepare compaction execution. |
| `clustering_workflow` | Analyze storage, then prepare clustering execution. |
| `safe_rollback_workflow` | Show commit impact (partitions, files, savepoints), then prepare rollback. |
| `table_repair_workflow` | Dry-run repair first, then prepare real repair execution. |

## Safety Model

The server implements a tiered safety protocol for write operations:

```
LOW risk    --> Execute immediately, return result
MEDIUM risk --> Generate confirmation token (5-minute TTL), return token + description
HIGH risk   --> Run dry-run/preview first, generate token, return preview + token + description
```

To execute a MEDIUM or HIGH risk operation:

1. Call the write tool (e.g., `rollback_commit`). You receive a token and a description of what will happen.
2. Review the description (and dry-run results for HIGH risk).
3. Call `confirm_operation` with the token to execute, or `cancel_operation` to abort.

Tokens expire after 5 minutes. Each token can only be used once.

## Usage Examples

### Inspect a table

```
User: Connect to /tmp/trips_table and show me recent commits

--> AI calls connect_to_table(path="/tmp/trips_table")
--> AI calls execute_hudi_command(command="commits show --limit 5 --desc true")
```

### Run a health check

```
User: How healthy is my table?

--> AI calls table_health_check()
--> Returns: table description, recent commits, clean history, compaction status, write stats
```

### Check for stuck operations

```
User: Are there any incomplete operations?

--> AI calls execute_hudi_command(command="timeline show incomplete")
```

### Roll back a bad commit

```
User: Roll back commit 20260422001330433

--> AI calls rollback_commit(commit_instant="20260422001330433")
--> Returns: dry-run preview + confirmation token
--> User reviews and says "confirm"
--> AI calls confirm_operation(token="<token>")
--> Rollback executes
```

### Trigger cleaning

```
User: Run clean on the table

--> AI calls run_clean()
--> Returns: previous clean history + confirmation token
--> User says "go ahead"
--> AI calls confirm_operation(token="<token>")
```

### Cloud tables

The server supports any path the Hudi CLI supports:

```
User: Connect to s3://my-bucket/warehouse/events_table

--> AI calls connect_to_table(path="s3://my-bucket/warehouse/events_table")
```

Ensure the appropriate Hadoop/cloud credentials are available in the environment (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or IAM role).

## Architecture

```
server.py                  # FastMCP server — registers all tools
hudi_cli/
  commands.py              # Command validation, risk classification, allowlists
  executor.py              # Subprocess execution of hudi-cli
  parser.py                # ASCII FlipTable output parsing to structured JSON
  safety.py                # Token-based confirmation protocol (SafetyManager)
  session.py               # Table connection state (SessionManager)
tools/
  connection.py            # connect_to_table, disconnect, show_connection
  generic.py               # execute_hudi_command, execute_hudi_commands
  workflows.py             # Read-only composite workflows
  confirmation.py          # confirm_operation, cancel_operation
  write_ops.py             # All individual write operation tools
  write_workflows.py       # Guided multi-step write workflows
tests/
  test_commands.py          # Command validation tests
  test_parser.py            # Output parser tests
  test_safety.py            # Safety manager tests
  test_session.py           # Session manager tests
  test_write_commands.py    # Write command classification tests
  test_write_ops.py         # Write operation tool tests
  sample_outputs/           # Sample Hudi CLI output fixtures
```

## Running Tests

```bash
cd scripts/hudi-cli-mcp
source venv/bin/activate
python -m pytest tests/ -v
```

Tests use mocked executors and do not require a running Spark or Hudi installation.

## Contributing

When adding new tools:

1. **Read-only commands** — add the command prefix to `READONLY_COMMAND_PREFIXES` in `commands.py`, then use `execute_hudi_command` or create a new workflow in `tools/workflows.py`.
2. **Write operations** — add the command prefix and risk level to `WRITE_COMMAND_PREFIXES` in `commands.py`, then add a tool function in `tools/write_ops.py` with the appropriate risk tier.
3. **Composite workflows** — combine multiple commands into a single tool in `tools/workflows.py` (read-only) or `tools/write_workflows.py` (write).
4. **Tests** — add corresponding tests in the `tests/` directory.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](../../LICENSE) file in the root of this repository.