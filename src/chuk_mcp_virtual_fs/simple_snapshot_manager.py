import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

logger = logging.getLogger("chuk-mcp-virtual-fs")

class SimpleSnapshotManager:
    """
    A simplified snapshot manager for VirtualFileSystem.
    
    This class provides basic snapshot functionality for VirtualFileSystem instances,
    allowing users to create, restore, list, export, and import snapshots of the filesystem state.
    """
    def __init__(self, fs):
        """
        Initialize a new SimpleSnapshotManager.
        
        Args:
            fs: The VirtualFileSystem instance to manage snapshots for
        """
        self.fs = fs
        self._snapshots = {}
        self._snapshot_data = {}
        self.snapshot_dir = "/.snapshots"
        
        # Try to create snapshots directory if it doesn't exist
        try:
            if hasattr(self.fs, 'exists') and not self.fs.exists(self.snapshot_dir):
                if hasattr(self.fs, 'mkdir'):
                    try:
                        self.fs.mkdir(self.snapshot_dir)
                        logger.info(f"Created snapshots directory at {self.snapshot_dir}")
                    except Exception as e:
                        logger.warning(f"Could not create snapshots directory: {e}")
        except Exception as e:
            logger.warning(f"Error checking/creating snapshot directory: {e}")
            
        # Try to load existing snapshots
        self._load_snapshots()
            
    def _load_snapshots(self):
        """Load existing snapshots from the filesystem if available."""
        try:
            if hasattr(self.fs, 'exists') and self.fs.exists(self.snapshot_dir):
                try:
                    items = self.fs.ls(self.snapshot_dir)
                    for item in items:
                        if item.endswith('.json'):
                            snapshot_name = item[:-5]  # Remove .json extension
                            try:
                                snapshot_path = f"{self.snapshot_dir}/{item}"
                                content = self.fs.read_file(snapshot_path, encoding="utf-8")
                                data = json.loads(content)
                                
                                # Add to in-memory snapshots
                                self._snapshots[snapshot_name] = {
                                    "name": snapshot_name,
                                    "description": data.get("description", ""),
                                    "created": data.get("created")
                                }
                                
                                # Convert created timestamp string to datetime if needed
                                if isinstance(self._snapshots[snapshot_name]["created"], str):
                                    try:
                                        self._snapshots[snapshot_name]["created"] = datetime.fromisoformat(
                                            self._snapshots[snapshot_name]["created"]
                                        )
                                    except ValueError:
                                        self._snapshots[snapshot_name]["created"] = datetime.now()
                                
                                # Store snapshot data
                                self._snapshot_data[snapshot_name] = data.get("files", {})
                                logger.info(f"Loaded snapshot '{snapshot_name}' from {snapshot_path}")
                            except Exception as e:
                                logger.warning(f"Error loading snapshot {item}: {e}")
                except Exception as e:
                    logger.warning(f"Error listing snapshots directory: {e}")
        except Exception as e:
            logger.warning(f"Error loading snapshots: {e}")

    def create_snapshot(self, name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a snapshot of the current filesystem state.
        
        Args:
            name: Name of the snapshot
            description: Optional description of the snapshot
            
        Returns:
            Dictionary with snapshot metadata
        """
        logger.info(f"Creating snapshot '{name}'")
        paths = []
        try:
            if hasattr(self.fs, 'find'):
                paths = self.fs.find("/", recursive=True)
                # Exclude snapshot directory itself from the snapshot
                paths = [p for p in paths if not p.startswith(self.snapshot_dir)]
            else:
                try:
                    paths = [f"/{item}" for item in self.fs.ls("/") if item != self.snapshot_dir.lstrip('/')]
                except Exception as e:
                    logger.warning(f"Error listing root directory: {e}")
        except Exception as e:
            logger.warning(f"Error finding files for snapshot: {e}")

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
                        try:
                            # For binary files, use base64 encoding
                            import base64
                            binary_content = self.fs.read_file(path)
                            content = base64.b64encode(binary_content).decode('ascii')
                        except Exception:
                            logger.warning(f"Could not read file {path} for snapshot")
                            continue
                    snapshot_data[path] = content
            except Exception as e:
                logger.warning(f"Error processing file {path} for snapshot: {e}")

        timestamp = datetime.now()
        self._snapshots[name] = {
            "name": name,
            "description": description or f"Snapshot created at {timestamp.isoformat()}",
            "created": timestamp
        }
        self._snapshot_data[name] = snapshot_data
        
        # Persist the snapshot to filesystem
        try:
            self._save_snapshot(name)
        except Exception as e:
            logger.error(f"Error saving snapshot to filesystem: {e}")
        
        return self._snapshots[name]
    
    def _save_snapshot(self, name: str) -> bool:
        """Save a snapshot to the filesystem."""
        if name not in self._snapshots:
            logger.error(f"Cannot save non-existent snapshot '{name}'")
            return False
            
        snapshot_path = f"{self.snapshot_dir}/{name}.json"
        
        # Ensure the snapshots directory exists
        try:
            if hasattr(self.fs, 'exists') and not self.fs.exists(self.snapshot_dir):
                if hasattr(self.fs, 'mkdir'):
                    self.fs.mkdir(self.snapshot_dir)
        except Exception as e:
            logger.warning(f"Error ensuring snapshots directory exists: {e}")
        
        # Create the export data
        export_data = {
            **self._snapshots[name],
            "files": self._snapshot_data[name]
        }
        
        # Convert datetime to string for JSON serialization
        if isinstance(export_data["created"], datetime):
            export_data["created"] = export_data["created"].isoformat()
            
        # Write to filesystem
        try:
            self.fs.write_file(snapshot_path, json.dumps(export_data, indent=2), encoding="utf-8")
            logger.info(f"Saved snapshot '{name}' to {snapshot_path}")
            return True
        except Exception as e:
            logger.error(f"Error writing snapshot to {snapshot_path}: {e}")
            return False

    def restore_snapshot(self, name: str) -> str:
        """
        Restore the filesystem state from a snapshot.
        
        Args:
            name: Name of the snapshot to restore
            
        Returns:
            A message indicating the result of the operation
            
        Raises:
            ValueError: If the snapshot doesn't exist
        """
        if name not in self._snapshots:
            raise ValueError(f"Snapshot '{name}' not found")
            
        logger.info(f"Restoring snapshot '{name}'")
        
        # Get current paths
        current_paths = set()
        try:
            if hasattr(self.fs, 'find'):
                current_paths = set(self.fs.find("/", recursive=True))
                # Exclude snapshot directory
                current_paths = {p for p in current_paths if not p.startswith(self.snapshot_dir)}
            else:
                try:
                    current_paths = set([f"/{item}" for item in self.fs.ls("/") if item != self.snapshot_dir.lstrip('/')])
                except Exception as e:
                    logger.warning(f"Error listing root directory: {e}")
        except Exception as e:
            logger.warning(f"Error finding current files: {e}")

        snapshot_paths = set(self._snapshot_data[name].keys())
        
        # Delete files that aren't in the snapshot
        for path in current_paths - snapshot_paths:
            try:
                is_dir = False
                if hasattr(self.fs, 'get_node_info'):
                    node_info = self.fs.get_node_info(path)
                    if node_info and hasattr(node_info, 'is_dir'):
                        is_dir = node_info.is_dir
                if not is_dir:
                    if hasattr(self.fs, 'rm'):
                        logger.info(f"Removing file {path} (not in snapshot)")
                        self.fs.rm(path)
            except Exception as e:
                logger.warning(f"Error removing file {path}: {e}")

        # Create parent directories for all files in the snapshot
        for path in snapshot_paths:
            parent_dir = os.path.dirname(path)
            if parent_dir:
                try:
                    if hasattr(self.fs, 'mkdir'):
                        try:
                            self.fs.mkdir(parent_dir, recursive=True)
                            logger.info(f"Created directory {parent_dir} for snapshot restore")
                        except TypeError:
                            self.fs.mkdir(parent_dir)
                except Exception as e:
                    logger.warning(f"Error creating directory {parent_dir}: {e}")

        # Restore files from the snapshot
        for path, content in self._snapshot_data[name].items():
            try:
                try:
                    self.fs.write_file(path, content, encoding="utf-8")
                    logger.info(f"Restored file {path} from snapshot")
                except TypeError:
                    try:
                        # Try to detect if content is base64-encoded binary data
                        if isinstance(content, str) and content.startswith("b'") and content.endswith("'"):
                            import base64
                            binary_content = base64.b64decode(content[2:-1])
                            self.fs.write_file(path, binary_content)
                        else:
                            self.fs.write_file(path, content)
                    except Exception as write_e:
                        logger.warning(f"Error writing file {path}: {write_e}")
            except Exception as e:
                logger.warning(f"Error restoring file {path}: {e}")

        return f"Restored snapshot '{name}'"

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """
        List all available snapshots.
        
        Returns:
            List of snapshot metadata dictionaries
        """
        return list(self._snapshots.values())

    def export_snapshot(self, name: str, path: str) -> bool:
        """
        Export a snapshot to a JSON file.
        
        Args:
            name: Name of the snapshot to export
            path: Path to save the exported snapshot
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            ValueError: If the snapshot doesn't exist
        """
        if name not in self._snapshots:
            raise ValueError(f"Snapshot '{name}' not found")
            
        export_data = {
            **self._snapshots[name],
            "files": self._snapshot_data[name]
        }
        
        # Convert datetime to string for JSON serialization
        if isinstance(export_data["created"], datetime):
            export_data["created"] = export_data["created"].isoformat()
            
        try:
            with open(path, "w") as f:
                json.dump(export_data, f, indent=2)
            logger.info(f"Exported snapshot '{name}' to {path}")
            return True
        except Exception as e:
            logger.error(f"Error exporting snapshot to {path}: {e}")
            return False

    def import_snapshot(self, path: str, new_name: Optional[str] = None) -> str:
        """
        Import a snapshot from a JSON file.
        
        Args:
            path: Path to the snapshot file
            new_name: Optional new name for the imported snapshot
            
        Returns:
            Name of the imported snapshot
            
        Raises:
            ValueError: If the file cannot be read or parsed
        """
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except Exception as e:
            raise ValueError(f"Error reading snapshot file {path}: {e}")
            
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
        
        # Save the imported snapshot to filesystem
        try:
            self._save_snapshot(name)
        except Exception as e:
            logger.warning(f"Error saving imported snapshot to filesystem: {e}")
            
        logger.info(f"Imported snapshot as '{name}'")
        return name