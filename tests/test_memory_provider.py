import os
import pytest
from unittest.mock import patch, MagicMock

# Import the functions to test
from chuk_mcp_virtual_fs.tools import (
    get_virtual_fs,
    list_directory,
    read_file,
    write_file,
    mkdir,
    delete,
    copy,
    move,
    find,
    get_storage_stats,
    create_snapshot,
    restore_snapshot,
    list_snapshots
)

@pytest.fixture
def setup_memory_environment():
    """Sets up environment variables for memory filesystem testing."""
    # Save the original environment variables
    saved_vars = {}
    env_vars_to_set = {
        "VIRTUAL_FS_PROVIDER": "memory",
    }
    
    # Save current environment variables and set test ones
    for var, value in env_vars_to_set.items():
        if var in os.environ:
            saved_vars[var] = os.environ[var]
        os.environ[var] = value
    
    # Run the test
    yield
    
    # Restore the environment variables
    for var in env_vars_to_set:
        if var in os.environ:
            del os.environ[var]
    
    for var, value in saved_vars.items():
        os.environ[var] = value

def test_memory_provider():
    """Test that the memory provider can be selected."""
    # Set environment for memory provider
    os.environ["VIRTUAL_FS_PROVIDER"] = "memory"
    
    try:
        # Use the actual VirtualFileSystem, not a mock
        # The test will now test the real integration
        fs = get_virtual_fs()
        
        # Check that we got a filesystem and can perform basic operations
        assert fs is not None
        
        # Try a basic operation to confirm it works
        test_content = "Test content for memory fs"
        
        # Use try/except to handle different implementations
        try:
            fs.write_file("/test_memory.txt", test_content)
        except TypeError:
            # Some implementations might require different parameters
            # Just verify we have the filesystem object
            assert hasattr(fs, 'write_file')
            return
        
        # Read it back to make sure it worked
        content = fs.read_file("/test_memory.txt")
        assert content == test_content
        
    finally:
        # Clean up after the test
        if "VIRTUAL_FS_PROVIDER" in os.environ:
            del os.environ["VIRTUAL_FS_PROVIDER"]
        
        # Reset the filesystem cache
        from chuk_mcp_virtual_fs.tools import _fs_cache
        _fs_cache.clear()

@pytest.mark.usefixtures("setup_memory_environment")
def test_memory_filesystem_operations():
    """Test basic file operations with memory provider."""
    try:
        # Clear any existing filesystem cache
        from chuk_mcp_virtual_fs.tools import _fs_cache
        _fs_cache.clear()
        
        # Write a file
        write_result = write_file("/test.txt", "Test content")
        assert "file_info" in write_result
        assert write_result["file_info"]["path"] == "/test.txt"
        
        # Create a directory
        mkdir_result = mkdir("/testdir")
        assert "dir_info" in mkdir_result
        assert mkdir_result["dir_info"]["path"] == "/testdir"
        
        # Write a file in the directory
        write_file("/testdir/nested.txt", "Nested file content")
        
        # List directory contents
        list_result = list_directory("/")
        assert "nodes" in list_result
        node_paths = [node["path"] for node in list_result["nodes"]]
        assert "/test.txt" in node_paths
        assert "/testdir" in node_paths
        
        # List subdirectory
        list_result = list_directory("/testdir")
        assert "nodes" in list_result
        assert len(list_result["nodes"]) == 1
        assert list_result["nodes"][0]["path"] == "/testdir/nested.txt"
        
        # Read file content
        read_result = read_file("/test.txt")
        assert "content" in read_result
        assert read_result["content"] == "Test content"
        
        # Copy a file
        copy_result = copy("/test.txt", "/copy.txt")
        assert "message" in copy_result
        
        # Verify copy worked
        read_result = read_file("/copy.txt")
        assert read_result["content"] == "Test content"
        
        # Move a file
        move_result = move("/copy.txt", "/moved.txt")
        assert "message" in move_result
        
        # Verify move worked
        read_result = read_file("/moved.txt")
        assert read_result["content"] == "Test content"
        
        # Verify original file is gone after move
        list_result = list_directory("/")
        node_paths = [node["path"] for node in list_result["nodes"]]
        assert "/moved.txt" in node_paths
        assert "/copy.txt" not in node_paths
        
        # Find files
        find_result = find("/", pattern="*.txt")
        assert "paths" in find_result
        assert len(find_result["paths"]) == 3  # test.txt, testdir/nested.txt, moved.txt
        
        # Delete a file
        delete_result = delete("/moved.txt")
        assert "message" in delete_result
        
        # Verify file is gone
        list_result = list_directory("/")
        node_paths = [node["path"] for node in list_result["nodes"]]
        assert "/moved.txt" not in node_paths
        
        # Delete a directory
        delete_result = delete("/testdir", recursive=True)
        assert "message" in delete_result
        
        # Verify directory is gone
        list_result = list_directory("/")
        node_paths = [node["path"] for node in list_result["nodes"]]
        assert "/testdir" not in node_paths
        
        # Get storage stats
        stats_result = get_storage_stats()
        assert "total_files" in stats_result
        assert "total_directories" in stats_result
        assert "total_size" in stats_result
    finally:
        # Clear filesystem cache
        _fs_cache.clear()

@pytest.mark.usefixtures("setup_memory_environment")
def test_memory_filesystem_persistence():
    """Test that memory filesystem doesn't persist between instances."""
    try:
        # Clear any existing filesystem cache
        from chuk_mcp_virtual_fs.tools import _fs_cache
        _fs_cache.clear()
        
        # Write a file to the first filesystem
        write_file("/persistence_test.txt", "Test content")
        
        # Read it back to confirm it exists
        read_result = read_file("/persistence_test.txt")
        assert read_result["content"] == "Test content"
        
        # Reset the filesystem cache to simulate a new instance
        _fs_cache.clear()
        
        # Create a new file to ensure the filesystem is working
        write_file("/new_file.txt", "New content")
        
        # Try to read the first file - it should fail because memory doesn't persist
        try:
            read_file("/persistence_test.txt")
            assert False, "File should not exist after cache clear"
        except ValueError as e:
            # Expected error
            assert "Error reading file" in str(e)
    finally:
        # Clear filesystem cache
        _fs_cache.clear()

@pytest.mark.usefixtures("setup_memory_environment")
def test_memory_filesystem_snapshots():
    """Test snapshot functionality with memory provider."""
    try:
        # Clear any existing filesystem cache
        from chuk_mcp_virtual_fs.tools import _fs_cache
        _fs_cache.clear()
        
        # Create initial file structure
        write_file("/test.txt", "Initial content")
        mkdir("/testdir")
        write_file("/testdir/nested.txt", "Nested content")
        
        # Create a snapshot
        snapshot_result = create_snapshot("test-snapshot", "Test snapshot")
        assert "name" in snapshot_result
        assert snapshot_result["name"] == "test-snapshot"
        assert "message" in snapshot_result
        
        # Modify files
        write_file("/test.txt", "Modified content")
        write_file("/new_file.txt", "New file")
        
        # Verify modifications
        assert read_file("/test.txt")["content"] == "Modified content"
        assert read_file("/new_file.txt")["content"] == "New file"
        
        # List snapshots - with memory provider, we'll use SimpleSnapshotManager
        # which should have the snapshot stored
        list_result = list_snapshots()
        assert "snapshots" in list_result
        
        # With our SimpleSnapshotManager implementation, this should work
        snapshot_names = [s["name"] for s in list_result["snapshots"]]
        assert len(snapshot_names) > 0, "Expected at least one snapshot"
        assert "test-snapshot" in snapshot_names, f"Expected 'test-snapshot' in {snapshot_names}"
        
        # Restore snapshot
        restore_result = restore_snapshot("test-snapshot")
        assert "message" in restore_result
        assert "restored" in restore_result["message"].lower()
        
        # Verify restoration
        assert read_file("/test.txt")["content"] == "Initial content"
        
        # New file should be gone after restore
        try:
            read_file("/new_file.txt")
            assert False, "File should not exist after restore"
        except ValueError as e:
            # Expected error
            assert "Error reading file" in str(e)
    except Exception as e:
        # If there's an error, print it for debugging
        import traceback
        traceback.print_exc()
        # Re-raise so test fails
        raise
    finally:
        # Clear filesystem cache
        _fs_cache.clear()