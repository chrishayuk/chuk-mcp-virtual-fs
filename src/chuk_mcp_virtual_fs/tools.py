import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time

# Import MCP tool decorator from your runtime
from chuk_mcp_runtime.common.mcp_tool_decorator import mcp_tool

# Import the VirtualFileSystem and SnapshotManager
from chuk_virtual_fs import VirtualFileSystem
try:
    from chuk_virtual_fs.snapshot_manager import SnapshotManager
except ImportError:
    # Create a placeholder if the import fails
    SnapshotManager = None

# Import the defined models
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

# Cache the VirtualFileSystem instance
_fs_cache = {}

def get_virtual_fs(provider_type: str = None, **kwargs) -> VirtualFileSystem:
    """
    Create or retrieve a cached VirtualFileSystem instance.
    
    Args:
        provider_type: Type of storage provider ('local', 's3', 'memory', etc.)
        **kwargs: Additional arguments for the VirtualFileSystem
        
    Returns:
        VirtualFileSystem instance
    """
    global _fs_cache
    
    if not provider_type:
        provider_type = os.environ.get("VIRTUAL_FS_PROVIDER", "s3")
    
    cache_key = f"{provider_type}:"
    if provider_type == "s3":
        bucket = kwargs.get("bucket_name") or os.environ.get("S3_BUCKET_NAME", "default-bucket")
        prefix = kwargs.get("prefix") or os.environ.get("S3_PREFIX", "default-prefix")
        endpoint = kwargs.get("endpoint_url") or os.environ.get("AWS_ENDPOINT_URL_S3", "")
        cache_key += f"{bucket}:{prefix}:{endpoint}"
    elif provider_type == "local":
        root_dir = kwargs.get("root_dir") or os.environ.get("LOCAL_FS_ROOT", "/tmp/virtual_fs")
        cache_key += f"{root_dir}"
    elif provider_type == "memory":
        memory_id = kwargs.get("memory_id") or os.environ.get("MEMORY_FS_ID", "default")
        cache_key += f"{memory_id}"
    
    if cache_key in _fs_cache:
        return _fs_cache[cache_key]
    
    try:
        if provider_type == "s3":
            if "bucket_name" not in kwargs:
                kwargs["bucket_name"] = os.environ.get("S3_BUCKET_NAME", "my-virtual-fs")
            if "prefix" not in kwargs:
                kwargs["prefix"] = os.environ.get("S3_PREFIX", "default")
            if "endpoint_url" not in kwargs and "AWS_ENDPOINT_URL_S3" in os.environ:
                kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL_S3")
            if "region_name" not in kwargs:
                kwargs["region_name"] = os.environ.get("AWS_REGION", "us-east-1")
        elif provider_type == "local":
            if "root_dir" not in kwargs:
                kwargs["root_dir"] = os.environ.get("LOCAL_FS_ROOT", "/tmp/virtual_fs")
        fs = VirtualFileSystem(provider_type, **kwargs)
        _fs_cache[cache_key] = fs
        return fs
    except Exception as e:
        logger.error(f"Error creating VirtualFileSystem: {e}")
        raise ValueError(f"Error creating VirtualFileSystem: {e}")

def get_snapshot_manager(fs: VirtualFileSystem) -> SnapshotManager:
    """Create and return a SnapshotManager for the given filesystem, caching it per fs instance."""
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

class SimpleSnapshotManager:
    """A simplified snapshot manager that works with any filesystem."""
    
    def __init__(self, fs):
        self.fs = fs
        self._snapshots = {}
        self._snapshot_data = {}
        
    def create_snapshot(self, name, description=None):
        """Create a snapshot with the given name and description."""
        paths = []
        try:
            if hasattr(self.fs, 'find'):
                paths = self.fs.find("/", recursive=True)
            else:
                try:
                    paths = [f"/{item}" for item in self.fs.ls("/")]
                except Exception:
                    pass
        except Exception:
            pass
        
        snapshot_data = {}
        for path in paths:
            try:
                is_dir = False
                if hasattr(self.fs, 'get_node_info'):
                    node_info = self.fs.get_node_info(path)
                    if node_info and hasattr(node_info, 'is_dir'):
                        is_dir = node_info.is_dir
                if not is_dir:
                    try:
                        content = self.fs.read_file(path, encoding="utf-8")
                    except (TypeError, ValueError):
                        content = self.fs.read_file(path)
                    snapshot_data[path] = content
            except Exception:
                pass
        
        timestamp = datetime.now()
        self._snapshots[name] = {
            "name": name,
            "description": description or f"Snapshot created at {timestamp.isoformat()}",
            "created": timestamp
        }
        self._snapshot_data[name] = snapshot_data
        return self._snapshots[name]
    
    def restore_snapshot(self, name):
        """Restore from a snapshot with the given name."""
        if name not in self._snapshots:
            raise ValueError(f"Snapshot '{name}' not found")
        current_paths = set()
        try:
            if hasattr(self.fs, 'find'):
                current_paths = set(self.fs.find("/", recursive=True))
            else:
                try:
                    current_paths = set([f"/{item}" for item in self.fs.ls("/")])
                except Exception:
                    pass
        except Exception:
            pass
            
        snapshot_paths = set(self._snapshot_data[name].keys())
        for path in current_paths - snapshot_paths:
            try:
                is_dir = False
                if hasattr(self.fs, 'get_node_info'):
                    node_info = self.fs.get_node_info(path)
                    if node_info and hasattr(node_info, 'is_dir'):
                        is_dir = node_info.is_dir
                if not is_dir:
                    if hasattr(self.fs, 'rm'):
                        self.fs.rm(path)
            except Exception:
                pass
        
        for path in snapshot_paths:
            parent_dir = os.path.dirname(path)
            if parent_dir:
                try:
                    if hasattr(self.fs, 'mkdir'):
                        try:
                            self.fs.mkdir(parent_dir, recursive=True)
                        except TypeError:
                            self.fs.mkdir(parent_dir)
                except Exception:
                    pass
        
        for path, content in self._snapshot_data[name].items():
            try:
                try:
                    self.fs.write_file(path, content, encoding="utf-8")
                except TypeError:
                    self.fs.write_file(path, content)
            except Exception:
                pass
        
        return f"Restored snapshot '{name}'"
    
    def list_snapshots(self):
        """List all available snapshots."""
        return list(self._snapshots.values())
    
    def export_snapshot(self, name, path):
        """Export a snapshot to a file."""
        if name not in self._snapshots:
            raise ValueError(f"Snapshot '{name}' not found")
        export_data = {
            **self._snapshots[name],
            "files": self._snapshot_data[name]
        }
        if isinstance(export_data["created"], datetime):
            export_data["created"] = export_data["created"].isoformat()
        with open(path, "w") as f:
            import json
            json.dump(export_data, f, indent=2)
        return True
    
    def import_snapshot(self, path, new_name=None):
        """Import a snapshot from a file."""
        with open(path, "r") as f:
            import json
            data = json.load(f)
        name = new_name or data.get("name", f"imported_{int(time.time())}")
        description = data.get("description", "Imported snapshot")
        created = data.get("created", datetime.now().isoformat())
        if isinstance(created, str):
            try:
                created = datetime.fromisoformat(created)
            except ValueError:
                created = datetime.now()
        self._snapshots[name] = {
            "name": name,
            "description": description,
            "created": created
        }
        self._snapshot_data[name] = data.get("files", {})
        return name

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

@mcp_tool(name="list_directory", description="List contents of a directory")
def list_directory(path: str, recursive: bool = False) -> dict:
    """List contents of a directory in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = ListDirectoryInput(path=path, recursive=recursive)
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
        result = ListDirectoryResult(nodes=nodes)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error listing directory {path}: {e}")
        raise ValueError(f"Error listing directory: {e}")

@mcp_tool(name="read_file", description="Read file content from the filesystem")
def read_file(path: str, encoding: str = "utf-8") -> dict:
    """Read content of a file from the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = ReadFileInput(path=path, encoding=encoding)
        try:
            content = fs.read_file(input_data.path, encoding=input_data.encoding)
        except TypeError:
            content = fs.read_file(input_data.path)
        node_info = fs.get_node_info(input_data.path)
        if not node_info:
            raise ValueError(f"File not found: {input_data.path}")
        file_info = _node_to_info(input_data.path, node_info)
        result = ReadFileResult(content=content, file_info=file_info)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error reading file {path}: {e}")
        raise ValueError(f"Error reading file: {e}")

@mcp_tool(name="write_file", description="Write content to a file")
def write_file(path: str, content: str, encoding: str = "utf-8") -> dict:
    """Write content to a file in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = WriteFileInput(path=path, content=content, encoding=encoding)
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
        try:
            fs.write_file(input_data.path, input_data.content, encoding=input_data.encoding)
        except TypeError:
            fs.write_file(input_data.path, input_data.content)
        node_info = fs.get_node_info(input_data.path)
        if not node_info:
            raise ValueError(f"Failed to get information for file: {input_data.path}")
        file_info = _node_to_info(input_data.path, node_info)
        result = WriteFileResult(
            message=f"File '{input_data.path}' written successfully.",
            file_info=file_info
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error writing file {path}: {e}")
        raise ValueError(f"Error writing file: {e}")

@mcp_tool(name="mkdir", description="Create a directory")
def mkdir(path: str, recursive: bool = False) -> dict:
    """Create a directory in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = MkdirInput(path=path, recursive=recursive)
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
            raise ValueError(f"Failed to get information for directory: {input_data.path}")
        dir_info = _node_to_info(input_data.path, node_info)
        result = MkdirResult(
            message=f"Directory '{input_data.path}' created successfully.",
            dir_info=dir_info
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error creating directory {path}: {e}")
        raise ValueError(f"Error creating directory: {e}")

@mcp_tool(name="delete", description="Delete a file or directory")
def delete(path: str, recursive: bool = False) -> dict:
    """Delete a file or directory from the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = DeleteInput(path=path, recursive=recursive)
        path_exists = True
        if hasattr(fs, 'exists'):
            path_exists = fs.exists(input_data.path)
        else:
            try:
                if hasattr(fs, 'get_node_info'):
                    node_info = fs.get_node_info(input_data.path)
                    path_exists = node_info is not None
                else:
                    try:
                        parent_dir = os.path.dirname(input_data.path) or "/"
                        file_name = os.path.basename(input_data.path)
                        if hasattr(fs, 'ls'):
                            items = fs.ls(parent_dir)
                            path_exists = file_name in items
                        else:
                            fs.read_file(input_data.path)
                    except Exception:
                        path_exists = False
            except Exception:
                path_exists = False
                
        if not path_exists:
            return DeleteResult(message=f"Path '{input_data.path}' does not exist.").model_dump()
        
        is_dir = False
        try:
            if hasattr(fs, 'get_node_info'):
                node_info = fs.get_node_info(input_data.path)
                is_dir = node_info and (hasattr(node_info, 'is_dir') and node_info.is_dir)
            else:
                try:
                    fs.ls(input_data.path)
                    is_dir = True
                except Exception:
                    is_dir = False
        except Exception:
            is_dir = False
            
        if is_dir:
            try:
                fs.rmdir(input_data.path, recursive=input_data.recursive)
            except TypeError:
                fs.rmdir(input_data.path)
                if input_data.recursive:
                    try:
                        if hasattr(fs, 'find'):
                            items = fs.find(input_data.path, recursive=True)
                            for item in sorted(items, reverse=True):
                                if item != input_data.path:
                                    try:
                                        if hasattr(fs, 'get_node_info'):
                                            node = fs.get_node_info(item)
                                            if node and hasattr(node, 'is_dir') and node.is_dir:
                                                fs.rmdir(item)
                                            else:
                                                fs.rm(item)
                                        else:
                                            try:
                                                fs.rm(item)
                                            except Exception:
                                                try:
                                                    fs.rmdir(item)
                                                except Exception:
                                                        pass
                                    except Exception:
                                        pass
                    except Exception:
                        pass
            # Ensure the directory itself is removed
            try:
                fs.rmdir(input_data.path)
            except Exception:
                pass
            msg = f"Directory '{input_data.path}' deleted successfully."
        else:
            fs.rm(input_data.path)
            msg = f"File '{input_data.path}' deleted successfully."
        
        result = DeleteResult(message=msg)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error deleting {path}: {e}")
        raise ValueError(f"Error deleting: {e}")

@mcp_tool(name="copy", description="Copy a file or directory")
def copy(source: str, destination: str, recursive: bool = False) -> dict:
    """Copy a file or directory in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = CopyInput(source=source, destination=destination, recursive=recursive)
        source_exists = True
        if hasattr(fs, 'exists'):
            source_exists = fs.exists(input_data.source)
        else:
            try:
                if hasattr(fs, 'get_node_info'):
                    node_info = fs.get_node_info(input_data.source)
                    source_exists = node_info is not None
                else:
                    try:
                        fs.read_file(input_data.source)
                    except Exception:
                        source_exists = False
            except Exception:
                source_exists = False
        if not source_exists:
            raise ValueError(f"Source path '{input_data.source}' does not exist.")
        try:
            fs.cp(input_data.source, input_data.destination, recursive=input_data.recursive)
        except TypeError:
            fs.cp(input_data.source, input_data.destination)
        result = CopyResult(
            message=f"'{input_data.source}' copied to '{input_data.destination}' successfully."
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error copying {source} to {destination}: {e}")
        raise ValueError(f"Error copying: {e}")

@mcp_tool(name="move", description="Move a file or directory")
def move(source: str, destination: str) -> dict:
    """Move a file or directory in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = MoveInput(source=source, destination=destination)
        source_exists = True
        if hasattr(fs, 'exists'):
            source_exists = fs.exists(input_data.source)
        else:
            try:
                if hasattr(fs, 'get_node_info'):
                    node_info = fs.get_node_info(input_data.source)
                    source_exists = node_info is not None
                else:
                    try:
                        fs.read_file(input_data.source)
                    except Exception:
                        source_exists = False
            except Exception:
                source_exists = False
        if not source_exists:
            raise ValueError(f"Source path '{input_data.source}' does not exist.")
        fs.mv(input_data.source, input_data.destination)
        result = MoveResult(
            message=f"'{input_data.source}' moved to '{input_data.destination}' successfully."
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error moving {source} to {destination}: {e}")
        raise ValueError(f"Error moving: {e}")

@mcp_tool(name="find", description="Find files matching pattern")
def find(path: str, pattern: Optional[str] = None, recursive: bool = True) -> dict:
    """Find files matching a pattern in the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        input_data = FindInput(path=path, pattern=pattern, recursive=recursive)
        # Call find without passing pattern to the underlying fs.find() method
        paths = fs.find(input_data.path, recursive=input_data.recursive)
        if input_data.pattern:
            import fnmatch
            paths = [p for p in paths if fnmatch.fnmatch(p, input_data.pattern)]
        result = FindResult(paths=paths)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error finding files in {path}: {e}")
        raise ValueError(f"Error finding files: {e}")

@mcp_tool(name="get_storage_stats", description="Get storage statistics")
def get_storage_stats() -> dict:
    """Get storage statistics from the virtual filesystem."""
    fs = get_virtual_fs()
    try:
        stats = fs.get_storage_stats()
        total_files = stats.get("total_files", 0)
        total_directories = stats.get("total_directories", 0)
        total_size = stats.get("total_size", 0)
        result = GetStorageStatsResult(
            total_files=total_files,
            total_directories=total_directories,
            total_size=total_size,
            stats=stats
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")
        raise ValueError(f"Error getting storage stats: {e}")

@mcp_tool(name="create_snapshot", description="Create a filesystem snapshot")
def create_snapshot(name: str, description: Optional[str] = None) -> dict:
    """Create a snapshot of the filesystem state."""
    fs = get_virtual_fs()
    try:
        input_data = CreateSnapshotInput(name=name, description=description)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot = snapshot_mgr.create_snapshot(
            input_data.name, 
            input_data.description or f"Snapshot created at {datetime.now().isoformat()}"
        )
        if isinstance(snapshot, dict):
            snapshot_name = snapshot.get("name", input_data.name)
            snapshot_created = snapshot.get("created", datetime.now())
        else:
            snapshot_name = snapshot
            snapshot_created = datetime.now()
        result = CreateSnapshotResult(
            name=snapshot_name,
            created=snapshot_created,
            message=f"Snapshot '{input_data.name}' created successfully."
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error creating snapshot {name}: {e}")
        raise ValueError(f"Error creating snapshot: {e}")

@mcp_tool(name="restore_snapshot", description="Restore from a filesystem snapshot")
def restore_snapshot(name: str) -> dict:
    """Restore the filesystem state from a snapshot."""
    fs = get_virtual_fs()
    try:
        input_data = RestoreSnapshotInput(name=name)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot_mgr.restore_snapshot(input_data.name)
        result = RestoreSnapshotResult(
            message=f"Filesystem restored from snapshot '{input_data.name}' successfully."
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error restoring snapshot {name}: {e}")
        raise ValueError(f"Error restoring snapshot: {e}")

@mcp_tool(name="list_snapshots", description="List available snapshots")
def list_snapshots() -> dict:
    """List all available snapshots."""
    fs = get_virtual_fs()
    try:
        snapshot_mgr = get_snapshot_manager(fs)
        snapshots = snapshot_mgr.list_snapshots()
        result = ListSnapshotsResult(snapshots=snapshots)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error listing snapshots: {e}")
        raise ValueError(f"Error listing snapshots: {e}")

@mcp_tool(name="export_snapshot", description="Export a snapshot to a file")
def export_snapshot(name: str, path: str) -> dict:
    """Export a snapshot to a file."""
    fs = get_virtual_fs()
    try:
        input_data = ExportSnapshotInput(name=name, path=path)
        snapshot_mgr = get_snapshot_manager(fs)
        snapshot_mgr.export_snapshot(input_data.name, input_data.path)
        result = ExportSnapshotResult(
            message=f"Snapshot '{input_data.name}' exported to '{input_data.path}' successfully."
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error exporting snapshot {name}: {e}")
        raise ValueError(f"Error exporting snapshot: {e}")

@mcp_tool(name="import_snapshot", description="Import a snapshot from a file")
def import_snapshot(path: str, name: Optional[str] = None) -> dict:
    """Import a snapshot from a file."""
    fs = get_virtual_fs()
    try:
        input_data = ImportSnapshotInput(path=path, name=name)
        snapshot_mgr = get_snapshot_manager(fs)
        imported_name = snapshot_mgr.import_snapshot(input_data.path, input_data.name)
        result = ImportSnapshotResult(
            message=f"Snapshot imported from '{input_data.path}' successfully.",
            name=imported_name
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error importing snapshot from {path}: {e}")
        raise ValueError(f"Error importing snapshot: {e}")
