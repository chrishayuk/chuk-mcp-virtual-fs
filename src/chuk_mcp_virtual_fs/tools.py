import os
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import MCP tool decorator
from chuk_mcp_runtime.common.mcp_tool_decorator import mcp_tool

# Import VirtualFileSystem & SnapshotManager
from chuk_virtual_fs import VirtualFileSystem
try:
    from chuk_virtual_fs.snapshot_manager import SnapshotManager
except ImportError:
    SnapshotManager = None

# Import snapshot manager
from chuk_mcp_virtual_fs.simple_snapshot_manager import SimpleSnapshotManager

# Import models
from chuk_mcp_virtual_fs.models import (
    FileSystemNodeInfo,
    ListDirectoryInput,
    ListDirectoryResult,
    ReadFileInput,
    ReadFileResult,
    WriteFileInput,
    WriteFileResult,
    MkdirInput,
    MkdirResult,
    DeleteInput,
    DeleteResult,
    CopyInput,
    CopyResult,
    MoveInput,
    MoveResult,
    FindInput,
    FindResult,
    GetStorageStatsResult,
    CreateSnapshotInput,
    CreateSnapshotResult,
    RestoreSnapshotInput,
    RestoreSnapshotResult,
    ListSnapshotsResult,
    ExportSnapshotInput,
    ExportSnapshotResult,
    ImportSnapshotInput,
    ImportSnapshotResult
)

logger = logging.getLogger("chuk-mcp-virtual-fs")

# Cache the VirtualFileSystem
_fs_cache = {}

def get_virtual_fs(provider_type: str = None, **kwargs) -> VirtualFileSystem:
    """
    Create or retrieve a cached VirtualFileSystem instance.
    Defaults to S3. If S3 fails, it falls back to 'memory'.
    """
    global _fs_cache
    logger.info("Entering get_virtual_fs()")

    if not provider_type:
        provider_type = os.environ.get("VIRTUAL_FS_PROVIDER", "s3")
    logger.info(f"Requested provider_type: {provider_type}")

    def build_cache_key(prov: str, **params) -> str:
        if prov == "s3":
            bucket = params.get("bucket_name", "default-bucket")
            prefix = params.get("prefix", "default-prefix")
            endpoint = params.get("endpoint_url", "")
            key = f"s3:{bucket}:{prefix}:{endpoint}"
            logger.info(f"Built S3 cache key: {key}")
            return key
        elif prov == "memory":
            logger.info("Built memory cache key: memory")
            return "memory"
        else:
            logger.info(f"Built fallback provider key: {prov}")
            return prov

    # Handle default S3 env
    if provider_type == "s3":
        bucket_env = os.environ.get("S3_BUCKET_NAME", "my-virtual-fs")
        prefix_env = os.environ.get("S3_PREFIX", "default")
        endpoint_env = os.environ.get("AWS_ENDPOINT_URL_S3", "")
        region_env = os.environ.get("AWS_REGION", "us-east-1")

        if "bucket_name" not in kwargs:
            kwargs["bucket_name"] = bucket_env
        if "prefix" not in kwargs:
            kwargs["prefix"] = prefix_env
        if "endpoint_url" not in kwargs and endpoint_env:
            kwargs["endpoint_url"] = endpoint_env
        if "region_name" not in kwargs:
            kwargs["region_name"] = region_env

        logger.info(
            "S3 provider params: "
            f"bucket_name={kwargs['bucket_name']}, prefix={kwargs['prefix']}, "
            f"endpoint_url={kwargs.get('endpoint_url')}, region_name={kwargs['region_name']}"
        )

    cache_key = build_cache_key(provider_type, **kwargs)

    if cache_key in _fs_cache:
        logger.info(f"Returning cached VirtualFileSystem for key: {cache_key}")
        return _fs_cache[cache_key]

    logger.info(f"Creating VirtualFileSystem with provider '{provider_type}', kwargs={kwargs}")
    try:
        fs = VirtualFileSystem(provider_type, **kwargs)
        _fs_cache[cache_key] = fs
        logger.info(f"Successfully created filesystem '{provider_type}' with cache key '{cache_key}'")
        return fs
    except Exception as e:
        logger.error(f"Error creating VirtualFileSystem for provider '{provider_type}': {e}")

        # If S3 fails, fallback to memory
        if provider_type == "s3":
            logger.info("Attempting fallback to memory provider.")
            fallback_provider = "memory"
            fallback_cache_key = build_cache_key(fallback_provider)

            if fallback_cache_key in _fs_cache:
                logger.info(f"Falling back to existing memory filesystem: {fallback_cache_key}")
                return _fs_cache[fallback_cache_key]
            try:
                fs = VirtualFileSystem(fallback_provider)
                _fs_cache[fallback_cache_key] = fs
                logger.info("Fell back to new memory provider successfully.")
                return fs
            except Exception as mem_e:
                logger.error(f"Error falling back to memory provider: {mem_e}")
                raise ValueError(f"Error creating VirtualFileSystem: S3 error ({e}) "
                                 f"and fallback (memory) error ({mem_e})")
        else:
            raise ValueError(f"Error creating VirtualFileSystem: {e}")


def get_snapshot_manager(fs: VirtualFileSystem) -> Any:
    """Create and return a SnapshotManager for the given filesystem."""
    if hasattr(fs, "_snapshot_manager"):
        return fs._snapshot_manager
    provider = os.environ.get("VIRTUAL_FS_PROVIDER", "s3")
    if provider.lower() == "memory":
        sm = SimpleSnapshotManager(fs)
    else:
        try:
            sm = SnapshotManager(fs)
        except Exception as e:
            logger.error(f"Error creating SnapshotManager: {e}")
            sm = SimpleSnapshotManager(fs)
    fs._snapshot_manager = sm
    return sm


def _node_to_info(path: str, node: Any) -> FileSystemNodeInfo:
    """Convert a node from VirtualFileSystem to FileSystemNodeInfo model."""
    name = os.path.basename(path) or path
    return FileSystemNodeInfo(
        path=path,
        name=name,
        is_dir=node.is_dir if hasattr(node, "is_dir") else False,
        size=node.size if hasattr(node, "size") else None,
        modified=node.modified if hasattr(node, "modified") else None,
        created=node.created if hasattr(node, "created") else None,
        metadata=node.metadata if hasattr(node, "metadata") else {}
    )

def normalize_path(path: str) -> str:
    """
    Normalizes path '.' -> '/', ensures a leading slash, etc.
    
    This is the only path normalization needed at the MCP level - the
    underlying VirtualFileSystem and providers will handle their own
    specific path construction.
    """
    if not path or path in (".", "./"):
        return "/"

    if path.startswith("./"):
        path = path[2:]
    if not path.startswith("/"):
        path = "/" + path
    return path


@mcp_tool(name="list_directory", description="List contents of a directory")
def list_directory(path: str, recursive: bool = False) -> dict:
    fs = get_virtual_fs()
    try:
        # Only normalize paths at the MCP level - no need to handle S3 prefixes, etc.
        norm_path = normalize_path(path)
        logger.info(f"list_directory: normalized user path '{path}' -> '{norm_path}'")
        input_data = ListDirectoryInput(path=norm_path, recursive=recursive)

        if input_data.recursive:
            paths = fs.find(input_data.path, recursive=True)
            nodes = []
            for p in paths:
                node_info = fs.get_node_info(p)
                if node_info:
                    nodes.append(_node_to_info(p, node_info))
        else:
            items = fs.ls(input_data.path)
            nodes = []
            for item in items:
                item_path = os.path.join(input_data.path, item).replace("\\", "/")
                node_info = fs.get_node_info(item_path)
                if node_info:
                    nodes.append(_node_to_info(item_path, node_info))

        return ListDirectoryResult(nodes=nodes).model_dump()

    except Exception as e:
        logger.error(f"Error listing directory '{path}': {e}")
        raise ValueError(f"Error listing directory: {e}")


@mcp_tool(name="read_file", description="Read file content")
def read_file(path: str, encoding: str = "utf-8") -> dict:
    fs = get_virtual_fs()
    try:
        norm_path = normalize_path(path)
        logger.info(f"read_file: normalized user path '{path}' -> '{norm_path}'")
        
        input_data = ReadFileInput(path=norm_path, encoding=encoding)
        
        # Try to check if file exists first
        try:
            if hasattr(fs, 'exists') and not fs.exists(input_data.path):
                raise ValueError(f"File not found: {input_data.path}")
        except Exception as check_e:
            logger.warning(f"Error checking file existence: {check_e}")
        
        try:
            content = fs.read_file(input_data.path, encoding=input_data.encoding)
        except TypeError:
            content = fs.read_file(input_data.path)
        
        node_info = fs.get_node_info(input_data.path)
        if not node_info:
            raise ValueError(f"File not found: {input_data.path}")
        
        file_info = _node_to_info(input_data.path, node_info)
        return ReadFileResult(content=content, file_info=file_info).model_dump()
    
    except Exception as e:
        logger.error(f"Error reading file '{path}': {e}")
        raise ValueError(f"Error reading file: {e}")


@mcp_tool(name="write_file", description="Write content to a file")
def write_file(path: str, content: str, encoding: str = "utf-8") -> dict:
    """
    Writes a file and does NOT fail if fresh metadata isn't immediately available.
    Instead, we log a warning and return fallback info.
    """
    fs = get_virtual_fs()
    try:
        norm_path = normalize_path(path)
        logger.info(f"write_file: normalized user path '{path}' -> '{norm_path}'")
        
        input_data = WriteFileInput(path=norm_path, content=content, encoding=encoding)

        # Create parent directory if needed
        parent_dir = os.path.dirname(input_data.path)
        if parent_dir:
            try:
                if hasattr(fs, 'exists') and not fs.exists(parent_dir):
                    fs.mkdir(parent_dir, recursive=True)
                else:
                    try:
                        fs.mkdir(parent_dir, recursive=True)
                    except Exception:
                        pass
            except Exception:
                try:
                    fs.mkdir(parent_dir, recursive=True)
                except Exception:
                    pass

        # Write file content
        try:
            fs.write_file(input_data.path, input_data.content, encoding=input_data.encoding)
            logger.info(f"Successfully wrote content to '{input_data.path}'")
            
            # For debugging, store a reference to the file path somewhere accessible
            try:
                ref_file_path = os.path.join(os.path.dirname(norm_path), "__file_references.txt")
                try:
                    existing_content = fs.read_file(ref_file_path, encoding="utf-8")
                except:
                    existing_content = ""
                    
                fs.write_file(
                    ref_file_path, 
                    f"{existing_content}\nWritten: {path} -> {norm_path} at {datetime.now().isoformat()}", 
                    encoding="utf-8"
                )
            except Exception as ref_e:
                logger.warning(f"Could not write reference file: {ref_e}")
                
        except TypeError:
            fs.write_file(input_data.path, input_data.content)
            logger.info(f"Successfully wrote content to '{input_data.path}' (fallback method)")

        # Attempt to retrieve metadata, but handle if it's missing
        try:
            node_info = fs.get_node_info(input_data.path)
        except Exception as meta_e:
            logger.warning(f"Error getting node info: {meta_e}")
            node_info = None

        if not node_info:
            logger.warning(f"Could not retrieve metadata for newly created file '{input_data.path}'.")
            # Fallback to minimal info
            file_info = FileSystemNodeInfo(
                path=input_data.path,
                name=os.path.basename(input_data.path),
                is_dir=False,
                size=len(input_data.content),
                modified=None,
                created=None,
                metadata={}
            )
        else:
            file_info = _node_to_info(input_data.path, node_info)

        result = WriteFileResult(
            message=f"File '{input_data.path}' written successfully.",
            file_info=file_info
        )
        return result.model_dump()

    except Exception as e:
        logger.error(f"Error writing file '{path}': {e}")
        raise ValueError(f"Error writing file: {e}")


@mcp_tool(name="mkdir", description="Create a directory")
def mkdir(path: str, recursive: bool = False) -> dict:
    fs = get_virtual_fs()
    try:
        norm_path = normalize_path(path)
        logger.info(f"mkdir: normalized user path '{path}' -> '{norm_path}'")
        input_data = MkdirInput(path=norm_path, recursive=recursive)

        try:
            fs.mkdir(input_data.path, recursive=input_data.recursive)
        except TypeError:
            fs.mkdir(input_data.path)
            if input_data.recursive and "/" in input_data.path:
                parent_dir = os.path.dirname(input_data.path)
                if parent_dir:
                    parts = parent_dir.split("/")
                    current_path = ""
                    for part in parts:
                        if part:
                            current_path += "/" + part
                            try:
                                if hasattr(fs, 'exists') and fs.exists(current_path):
                                    continue
                                fs.mkdir(current_path)
                            except Exception:
                                pass

        node_info = fs.get_node_info(input_data.path)
        if not node_info:
            raise ValueError(f"Failed to get info for directory: {input_data.path}")

        dir_info = _node_to_info(input_data.path, node_info)
        return MkdirResult(
            message=f"Directory '{input_data.path}' created successfully.",
            dir_info=dir_info
        ).model_dump()

    except Exception as e:
        logger.error(f"Error creating directory '{path}': {e}")
        raise ValueError(f"Error creating directory: {e}")


@mcp_tool(name="delete", description="Delete a file or directory")
def delete(path: str, recursive: bool = False) -> dict:
    fs = get_virtual_fs()
    try:
        norm_path = normalize_path(path)
        logger.info(f"delete: normalized user path '{path}' -> '{norm_path}'")
        input_data = DeleteInput(path=norm_path, recursive=recursive)

        path_exists = True
        if hasattr(fs, 'exists'):
            path_exists = fs.exists(input_data.path)
        else:
            try:
                node_info = fs.get_node_info(input_data.path)
                path_exists = node_info is not None
            except Exception:
                path_exists = False

        if not path_exists:
            return DeleteResult(message=f"Path '{input_data.path}' does not exist.").model_dump()

        is_dir = False
        try:
            node_info = fs.get_node_info(input_data.path)
            is_dir = (node_info and getattr(node_info, 'is_dir', False))
        except Exception:
            pass

        if is_dir:
            try:
                fs.rmdir(input_data.path, recursive=input_data.recursive)
            except TypeError:
                fs.rmdir(input_data.path)
                if input_data.recursive:
                    try:
                        items = fs.find(input_data.path, recursive=True)
                        for item in sorted(items, reverse=True):
                            if item != input_data.path:
                                try:
                                    node = fs.get_node_info(item)
                                    if node and getattr(node, 'is_dir', False):
                                        fs.rmdir(item)
                                    else:
                                        fs.rm(item)
                                except Exception:
                                    pass
                    except Exception:
                        pass
            try:
                fs.rmdir(input_data.path)
            except Exception:
                pass
            msg = f"Directory '{input_data.path}' deleted successfully."
        else:
            fs.rm(input_data.path)
            msg = f"File '{input_data.path}' deleted successfully."

        return DeleteResult(message=msg).model_dump()
    except Exception as e:
        logger.error(f"Error deleting '{path}': {e}")
        raise ValueError(f"Error deleting: {e}")


@mcp_tool(name="copy", description="Copy a file or directory")
def copy(source: str, destination: str, recursive: bool = False) -> dict:
    fs = get_virtual_fs()
    try:
        src_norm = normalize_path(source)
        dst_norm = normalize_path(destination)
        logger.info(f"copy: normalized source path '{source}' -> '{src_norm}', destination path '{destination}' -> '{dst_norm}'")
        input_data = CopyInput(source=src_norm, destination=dst_norm, recursive=recursive)

        source_exists = True
        if hasattr(fs, 'exists'):
            source_exists = fs.exists(input_data.source)
        else:
            try:
                node_info = fs.get_node_info(input_data.source)
                source_exists = (node_info is not None)
            except Exception:
                source_exists = False

        if not source_exists:
            raise ValueError(f"Source path '{input_data.source}' does not exist.")

        try:
            fs.cp(input_data.source, input_data.destination, recursive=input_data.recursive)
        except TypeError:
            fs.cp(input_data.source, input_data.destination)

        return CopyResult(
            message=f"'{input_data.source}' copied to '{input_data.destination}' successfully."
        ).model_dump()

    except Exception as e:
        logger.error(f"Error copying '{source}' to '{destination}': {e}")
        raise ValueError(f"Error copying: {e}")


@mcp_tool(name="move", description="Move a file or directory")
def move(source: str, destination: str) -> dict:
    fs = get_virtual_fs()
    try:
        src_norm = normalize_path(source)
        dst_norm = normalize_path(destination)
        logger.info(f"move: normalized source path '{source}' -> '{src_norm}', destination path '{destination}' -> '{dst_norm}'")
        input_data = MoveInput(source=src_norm, destination=dst_norm)

        source_exists = True
        if hasattr(fs, 'exists'):
            source_exists = fs.exists(input_data.source)
        else:
            try:
                node_info = fs.get_node_info(input_data.source)
                source_exists = (node_info is not None)
            except Exception:
                source_exists = False

        if not source_exists:
            raise ValueError(f"Source path '{input_data.source}' does not exist.")

        fs.mv(input_data.source, input_data.destination)
        return MoveResult(
            message=f"'{input_data.source}' moved to '{input_data.destination}' successfully."
        ).model_dump()

    except Exception as e:
        logger.error(f"Error moving '{source}' to '{destination}': {e}")
        raise ValueError(f"Error moving: {e}")


@mcp_tool(name="find", description="Find files matching a pattern")
def find(path: str, pattern: Optional[str] = None, recursive: bool = True) -> dict:
    fs = get_virtual_fs()
    try:
        norm_path = normalize_path(path)
        logger.info(f"find: normalized user path '{path}' -> '{norm_path}'")
        input_data = FindInput(path=norm_path, pattern=pattern, recursive=recursive)
        paths = fs.find(input_data.path, recursive=input_data.recursive)
        if input_data.pattern:
            import fnmatch
            paths = [p for p in paths if fnmatch.fnmatch(p, input_data.pattern)]
        return FindResult(paths=paths).model_dump()
    except Exception as e:
        logger.error(f"Error finding files in '{path}': {e}")
        raise ValueError(f"Error finding files: {e}")


@mcp_tool(name="get_storage_stats", description="Get storage statistics")
def get_storage_stats() -> dict:
    fs = get_virtual_fs()
    try:
        stats = fs.get_storage_stats()
        total_files = stats.get("total_files", 0)
        total_dirs = stats.get("total_directories", 0)
        total_size = stats.get("total_size", 0)
        return GetStorageStatsResult(
            total_files=total_files,
            total_directories=total_dirs,
            total_size=total_size,
            stats=stats
        ).model_dump()
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")
        raise ValueError(f"Error getting storage stats: {e}")


@mcp_tool(name="create_snapshot", description="Create a filesystem snapshot")
def create_snapshot(name: str, description: Optional[str] = None) -> dict:
    fs = get_virtual_fs()
    try:
        input_data = CreateSnapshotInput(name=name, description=description)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot = snapshot_mgr.create_snapshot(
            input_data.name,
            input_data.description or f"Snapshot created at {datetime.now().isoformat()}"
        )
        if isinstance(snapshot, dict):
            snap_name = snapshot.get("name", input_data.name)
            snap_created = snapshot.get("created", datetime.now())
        else:
            snap_name = snapshot
            snap_created = datetime.now()
        return CreateSnapshotResult(
            name=snap_name,
            created=snap_created,
            message=f"Snapshot '{input_data.name}' created successfully."
        ).model_dump()
    except Exception as e:
        logger.error(f"Error creating snapshot '{name}': {e}")
        raise ValueError(f"Error creating snapshot: {e}")


@mcp_tool(name="restore_snapshot", description="Restore from a filesystem snapshot")
def restore_snapshot(name: str) -> dict:
    fs = get_virtual_fs()
    try:
        input_data = RestoreSnapshotInput(name=name)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot_mgr.restore_snapshot(input_data.name)
        return RestoreSnapshotResult(
            message=f"Filesystem restored from snapshot '{input_data.name}' successfully."
        ).model_dump()
    except Exception as e:
        logger.error(f"Error restoring snapshot '{name}': {e}")
        raise ValueError(f"Error restoring snapshot: {e}")


@mcp_tool(name="list_snapshots", description="List available snapshots")
def list_snapshots() -> dict:
    fs = get_virtual_fs()
    try:
        snapshot_mgr = get_snapshot_manager(fs)
        snaps = snapshot_mgr.list_snapshots()
        return ListSnapshotsResult(snapshots=snaps).model_dump()
    except Exception as e:
        logger.error(f"Error listing snapshots: {e}")
        raise ValueError(f"Error listing snapshots: {e}")


@mcp_tool(name="export_snapshot", description="Export a snapshot to a file")
def export_snapshot(name: str, path: str) -> dict:
    fs = get_virtual_fs()
    try:
        input_data = ExportSnapshotInput(name=name, path=path)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot_mgr.export_snapshot(input_data.name, input_data.path)
        return ExportSnapshotResult(
            message=f"Snapshot '{input_data.name}' exported to '{input_data.path}' successfully."
        ).model_dump()
    except Exception as e:
        logger.error(f"Error exporting snapshot '{name}': {e}")
        raise ValueError(f"Error exporting snapshot: {e}")


@mcp_tool(name="import_snapshot", description="Import a snapshot from a file")
def import_snapshot(path: str, name: Optional[str] = None) -> dict:
    fs = get_virtual_fs()
    try:
        input_data = ImportSnapshotInput(path=path, name=name)
        snapshot_mgr = get_snapshot_manager(fs)
        imported_name = snapshot_mgr.import_snapshot(input_data.path, input_data.name)
        return ImportSnapshotResult(
            message=f"Snapshot imported from '{input_data.path}' successfully.",
            name=imported_name
        ).model_dump()
    except Exception as e:
        logger.error(f"Error importing snapshot from '{path}': {e}")
        raise ValueError(f"Error importing snapshot: {e}")