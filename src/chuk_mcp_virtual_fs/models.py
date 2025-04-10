# chuk_mcp_virtual_fs/models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class FileSystemNodeInfo(BaseModel):
    """Information about a file system node (file or directory)."""
    path: str = Field(..., description="Path of the node")
    name: str = Field(..., description="Name of the node")
    is_dir: bool = Field(..., description="True if the node is a directory")
    size: Optional[int] = Field(None, description="Size of the file in bytes (None for directories)")
    modified: Optional[datetime] = Field(None, description="Last modified timestamp")
    created: Optional[datetime] = Field(None, description="Creation timestamp")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

class ListDirectoryInput(BaseModel):
    """Input model for listing directory contents."""
    path: str = Field(..., description="Directory path to list")
    recursive: bool = Field(False, description="Whether to list recursively")

class ListDirectoryResult(BaseModel):
    """Result model for listing directory contents."""
    nodes: List[FileSystemNodeInfo] = Field(..., description="List of nodes in the directory")

class ReadFileInput(BaseModel):
    """Input model for reading a file."""
    path: str = Field(..., description="Path to the file to read")
    encoding: Optional[str] = Field("utf-8", description="Encoding to use for reading")

class ReadFileResult(BaseModel):
    """Result model for reading a file."""
    content: str = Field(..., description="Content of the file")
    file_info: FileSystemNodeInfo = Field(..., description="Information about the file")

class WriteFileInput(BaseModel):
    """Input model for writing a file."""
    path: str = Field(..., description="Path to the file to write")
    content: str = Field(..., description="Content to write to the file")
    encoding: Optional[str] = Field("utf-8", description="Encoding to use for writing")

class WriteFileResult(BaseModel):
    """Result model for writing a file."""
    message: str = Field(..., description="Status message")
    file_info: FileSystemNodeInfo = Field(..., description="Information about the file")

class MkdirInput(BaseModel):
    """Input model for creating a directory."""
    path: str = Field(..., description="Path to the directory to create")
    recursive: bool = Field(False, description="Whether to create parent directories")

class MkdirResult(BaseModel):
    """Result model for creating a directory."""
    message: str = Field(..., description="Status message")
    dir_info: FileSystemNodeInfo = Field(..., description="Information about the directory")

class DeleteInput(BaseModel):
    """Input model for deleting a file or directory."""
    path: str = Field(..., description="Path to the file or directory to delete")
    recursive: bool = Field(False, description="Whether to delete recursively (for directories)")

class DeleteResult(BaseModel):
    """Result model for deleting a file or directory."""
    message: str = Field(..., description="Status message")

class CopyInput(BaseModel):
    """Input model for copying a file or directory."""
    source: str = Field(..., description="Source path")
    destination: str = Field(..., description="Destination path")
    recursive: bool = Field(False, description="Whether to copy recursively (for directories)")

class CopyResult(BaseModel):
    """Result model for copying a file or directory."""
    message: str = Field(..., description="Status message")

class MoveInput(BaseModel):
    """Input model for moving a file or directory."""
    source: str = Field(..., description="Source path")
    destination: str = Field(..., description="Destination path")

class MoveResult(BaseModel):
    """Result model for moving a file or directory."""
    message: str = Field(..., description="Status message")

class FindInput(BaseModel):
    """Input model for finding files."""
    path: str = Field(..., description="Base path to search from")
    pattern: Optional[str] = Field(None, description="Pattern to match (glob syntax)")
    recursive: bool = Field(True, description="Whether to search recursively")

class FindResult(BaseModel):
    """Result model for finding files."""
    paths: List[str] = Field(..., description="List of paths that match the criteria")

class GetStorageStatsResult(BaseModel):
    """Result model for getting storage statistics."""
    total_files: int = Field(..., description="Total number of files")
    total_directories: int = Field(..., description="Total number of directories")
    total_size: int = Field(..., description="Total size in bytes")
    stats: Dict[str, Any] = Field(..., description="Additional provider-specific statistics")

class CreateSnapshotInput(BaseModel):
    """Input model for creating a snapshot."""
    name: str = Field(..., description="Name of the snapshot")
    description: Optional[str] = Field(None, description="Description of the snapshot")

class CreateSnapshotResult(BaseModel):
    """Result model for creating a snapshot."""
    name: str = Field(..., description="Name of the snapshot")
    created: datetime = Field(..., description="Creation timestamp")
    message: str = Field(..., description="Status message")

class RestoreSnapshotInput(BaseModel):
    """Input model for restoring a snapshot."""
    name: str = Field(..., description="Name of the snapshot to restore")

class RestoreSnapshotResult(BaseModel):
    """Result model for restoring a snapshot."""
    message: str = Field(..., description="Status message")

class ListSnapshotsResult(BaseModel):
    """Result model for listing snapshots."""
    snapshots: List[Dict[str, Any]] = Field(..., description="List of snapshots")

class ExportSnapshotInput(BaseModel):
    """Input model for exporting a snapshot."""
    name: str = Field(..., description="Name of the snapshot to export")
    path: str = Field(..., description="Path to export the snapshot to")

class ExportSnapshotResult(BaseModel):
    """Result model for exporting a snapshot."""
    message: str = Field(..., description="Status message")

class ImportSnapshotInput(BaseModel):
    """Input model for importing a snapshot."""
    path: str = Field(..., description="Path to import the snapshot from")
    name: Optional[str] = Field(None, description="New name for the imported snapshot")

class ImportSnapshotResult(BaseModel):
    """Result model for importing a snapshot."""
    message: str = Field(..., description="Status message")
    name: str = Field(..., description="Name of the imported snapshot")