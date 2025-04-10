# chuk-mcp-virtual-fs

MCP (Mission Control Platform) server for the Virtual FileSystem. This package provides a set of tools for managing files and directories in a virtual filesystem through the MCP framework.

## Features

- File and directory operations (create, read, write, delete, copy, move)
- Directory listing and file searching
- Filesystem snapshot management (create, restore, export, import)
- Storage statistics
- Support for different storage backends through Virtual FileSystem (local, S3, etc.)

## Installation

```bash
pip install chuk-mcp-virtual-fs
```

## Configuration

Create a `config.yaml` file with the following settings:

```yaml
host:
  name: "chuk-mcp-virtual-fs"
  log_level: "INFO"

server:
  type: "stdio"  # or "websocket" for network-based communication

mcp_servers:
  virtual_fs:
    enabled: true
    location: "."
    tools:
      enabled: true
      module: "chuk_mcp_virtual_fs.tools"
```

## Environment Variables

The following environment variables can be set to configure the virtual filesystem:

### For S3 Provider
- `VIRTUAL_FS_PROVIDER`: Set to "s3" to use S3 storage (default if not specified)
- `S3_BUCKET_NAME`: Name of the S3 bucket to use
- `S3_PREFIX`: Prefix for all files within the bucket
- `AWS_ENDPOINT_URL_S3`: Custom endpoint URL for S3-compatible storage
- `AWS_REGION`: AWS region to use

### For Local Provider
- `VIRTUAL_FS_PROVIDER`: Set to "local" to use local filesystem
- `LOCAL_FS_ROOT`: Root directory for the local filesystem

### For Memory Provider
- `VIRTUAL_FS_PROVIDER`: Set to "memory" to use in-memory filesystem
- `MEMORY_FS_ID`: Optional identifier for the memory filesystem (defaults to "default")

## Usage

Start the MCP server:

```bash
mcp-virtual-fs
```

Then use the MCP client to interact with the virtual filesystem tools.

## Available Tools

- `list_directory`: List contents of a directory
- `read_file`: Read file content
- `write_file`: Write content to a file
- `mkdir`: Create a directory
- `delete`: Delete a file or directory
- `copy`: Copy a file or directory
- `move`: Move a file or directory
- `find`: Find files matching a pattern
- `get_storage_stats`: Get storage statistics
- `create_snapshot`: Create a filesystem snapshot
- `restore_snapshot`: Restore from a filesystem snapshot
- `list_snapshots`: List available snapshots
- `export_snapshot`: Export a snapshot to a file
- `import_snapshot`: Import a snapshot from a file

## Development

### Prerequisites

- Python 3.11 or higher
- Required packages: boto3, pydantic, chuk-mcp-runtime, mcp, chuk-virtual-fs

### Setup Development Environment

1. Clone the repository
2. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
3. Run tests:
   ```bash
   pytest
   ```

## License

[MIT](LICENSE)