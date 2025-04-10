import os
import json
import pytest
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import the functions to test
from chuk_mcp_virtual_fs.tools import (
    get_virtual_fs,
    create_snapshot,
    restore_snapshot,
    list_snapshots,
    export_snapshot,
    import_snapshot,
    write_file,
    mkdir,
    read_file
)

# Create a fixture for a temporary local filesystem
@pytest.fixture
def temp_fs_dir():
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        # Clean up after the test
        shutil.rmtree(temp_dir, ignore_errors=True)

# Create a mock VirtualFileSystem and SnapshotManager for testing
@pytest.fixture
def mock_fs_and_snapshot_mgr():
    mock_fs = MagicMock()
    
    # Setup mock snapshot manager
    mock_snapshot_mgr = MagicMock()
    
    # Configure mock snapshot manager methods
    mock_snapshot_mgr.create_snapshot.return_value = {
        "name": "test-snapshot",
        "created": datetime.now(),
        "description": "Test snapshot description"
    }
    
    mock_snapshot_mgr.list_snapshots.return_value = [
        {
            "name": "snapshot1",
            "created": datetime.now(),
            "description": "First snapshot"
        },
        {
            "name": "snapshot2",
            "created": datetime.now(),
            "description": "Second snapshot"
        }
    ]
    
    return mock_fs, mock_snapshot_mgr

# Test the create_snapshot function
@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_create_snapshot_dict_return(mock_get_snapshot_mgr, mock_get_fs, mock_fs_and_snapshot_mgr):
    mock_fs, mock_snapshot_mgr = mock_fs_and_snapshot_mgr
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    
    # Test creating a snapshot with dict return
    result = create_snapshot("test-snapshot", "Test snapshot description")
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.create_snapshot.assert_called_once_with(
        "test-snapshot", 
        "Test snapshot description"
    )
    
    # Verify the result contains the expected data
    assert "name" in result
    assert result["name"] == "test-snapshot"
    assert "created" in result
    assert "message" in result
    assert "created successfully" in result["message"]

@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_create_snapshot_string_return(mock_get_snapshot_mgr, mock_get_fs):
    mock_fs = MagicMock()
    mock_snapshot_mgr = MagicMock()
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    
    # Configure mock to return a string instead of a dict
    mock_snapshot_mgr.create_snapshot.return_value = "test-snapshot"
    
    # Test creating a snapshot with string return
    result = create_snapshot("test-snapshot", "Test snapshot description")
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.create_snapshot.assert_called_once_with(
        "test-snapshot", 
        "Test snapshot description"
    )
    
    # Verify the result contains the expected data
    assert "name" in result
    assert result["name"] == "test-snapshot"
    assert "created" in result  # Should have a default created timestamp
    assert "message" in result
    assert "created successfully" in result["message"]

# Test the restore_snapshot function
@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_restore_snapshot(mock_get_snapshot_mgr, mock_get_fs, mock_fs_and_snapshot_mgr):
    mock_fs, mock_snapshot_mgr = mock_fs_and_snapshot_mgr
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    
    # Test restoring a snapshot
    result = restore_snapshot("test-snapshot")
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.restore_snapshot.assert_called_once_with("test-snapshot")
    
    # Verify the result contains the expected message
    assert "message" in result
    assert "restored" in result["message"]

# Test the list_snapshots function
@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_list_snapshots(mock_get_snapshot_mgr, mock_get_fs, mock_fs_and_snapshot_mgr):
    mock_fs, mock_snapshot_mgr = mock_fs_and_snapshot_mgr
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    
    # Test listing snapshots
    result = list_snapshots()
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.list_snapshots.assert_called_once()
    
    # Verify the result contains the expected data
    assert "snapshots" in result
    assert len(result["snapshots"]) == 2  # Should have 2 snapshots from the mock

# Test the export_snapshot function
@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_export_snapshot(mock_get_snapshot_mgr, mock_get_fs, mock_fs_and_snapshot_mgr):
    mock_fs, mock_snapshot_mgr = mock_fs_and_snapshot_mgr
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    
    # Test exporting a snapshot
    result = export_snapshot("test-snapshot", "/path/to/export.json")
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.export_snapshot.assert_called_once_with(
        "test-snapshot", 
        "/path/to/export.json"
    )
    
    # Verify the result contains the expected message
    assert "message" in result
    assert "exported" in result["message"]

# Test the import_snapshot function
@patch("chuk_mcp_virtual_fs.tools.get_virtual_fs")
@patch("chuk_mcp_virtual_fs.tools.get_snapshot_manager")
def test_import_snapshot(mock_get_snapshot_mgr, mock_get_fs, mock_fs_and_snapshot_mgr):
    mock_fs, mock_snapshot_mgr = mock_fs_and_snapshot_mgr
    mock_get_fs.return_value = mock_fs
    mock_get_snapshot_mgr.return_value = mock_snapshot_mgr
    mock_snapshot_mgr.import_snapshot.return_value = "imported-snapshot"
    
    # Test importing a snapshot
    result = import_snapshot("/path/to/import.json", "new-snapshot-name")
    
    # Verify the snapshot manager was called correctly
    mock_snapshot_mgr.import_snapshot.assert_called_once_with(
        "/path/to/import.json", 
        "new-snapshot-name"
    )
    
    # Verify the result contains the expected data
    assert "message" in result
    assert "imported" in result["message"]
    assert "name" in result
    assert result["name"] == "imported-snapshot"

# Test snapshot functionality with a real local filesystem
def test_real_snapshot_operations(temp_fs_dir):
    """Test snapshot functionality with the virtual filesystem."""
    # Use memory provider for faster tests
    os.environ["VIRTUAL_FS_PROVIDER"] = "memory"
    
    try:
        # Clear any existing filesystem cache
        from chuk_mcp_virtual_fs.tools import _fs_cache
        _fs_cache.clear()
        
        # Create initial file structure using the tools directly
        write_file("/file1.txt", "Initial content")
        mkdir("/test_dir")
        write_file("/test_dir/nested.txt", "Nested content")
        
        # Create a snapshot
        result = create_snapshot("initial-state", "Initial file structure")
        
        # Verify the result
        assert "name" in result
        assert result["name"] == "initial-state"
        assert "message" in result
        
        # Modify file structure
        write_file("/file2.txt", "New file content")
        write_file("/file1.txt", "Modified content")
        
        # Create another snapshot
        result = create_snapshot("modified-state", "Modified file structure")
        
        # List snapshots - with memory provider, we'll use SimpleSnapshotManager
        result = list_snapshots()
        
        # Verify we have snapshots - with our SimpleSnapshotManager, we should have some
        assert "snapshots" in result
        
        # Get the snapshot names and verify both are in the list
        snapshots = result["snapshots"]
        assert len(snapshots) > 0, f"Expected at least one snapshot but found {len(snapshots)}"
        
        snapshot_names = [s["name"] for s in snapshots]
        assert "initial-state" in snapshot_names, f"Expected 'initial-state' in {snapshot_names}"
        assert "modified-state" in snapshot_names, f"Expected 'modified-state' in {snapshot_names}"
        
        # Create export file path
        export_path = os.path.join(temp_fs_dir, "exported_snapshot.json")
        
        # Export a snapshot
        result = export_snapshot("modified-state", export_path)
        
        # Verify export file exists
        assert os.path.exists(export_path)
        
        # Verify export file contains valid JSON with the right snapshot name
        with open(export_path, "r") as f:
            import json
            exported_data = json.load(f)
            assert "name" in exported_data
            assert exported_data["name"] == "modified-state"
        
        # Restore to initial state
        result = restore_snapshot("initial-state")
        
        # Verify restoration worked
        assert "message" in result
        
        # Check file contents after restore
        read_result = read_file("/file1.txt")
        assert read_result["content"] == "Initial content"
        
        # New file should be gone
        try:
            read_file("/file2.txt")
            assert False, "file2.txt should not exist after restore"
        except ValueError:
            # Expected - file should not exist
            pass
            
        # Import the exported snapshot with a new name
        result = import_snapshot(export_path, "imported-snapshot") 
        
        # Verify import result
        assert "name" in result
        assert result["name"] == "imported-snapshot"
        
        # List snapshots again to verify the imported snapshot is there
        result = list_snapshots()
        snapshot_names = [s["name"] for s in result["snapshots"]]
        assert "imported-snapshot" in snapshot_names
        
        # We should have 3 snapshots now: initial-state, modified-state, and imported-snapshot
        assert len(result["snapshots"]) >= 3, f"Expected at least 3 snapshots, got {len(result['snapshots'])}"
        
    except Exception as e:
        # If there's an error, print it for debugging
        import traceback
        traceback.print_exc()
        # Re-raise so test fails
        raise
    finally:
        # Clean up environment variables
        if "VIRTUAL_FS_PROVIDER" in os.environ:
            del os.environ["VIRTUAL_FS_PROVIDER"]
        if "LOCAL_FS_ROOT" in os.environ:
            del os.environ["LOCAL_FS_ROOT"]
            
        # Clear filesystem cache again
        _fs_cache.clear()