# LMDB Storage Layer Enhancement

## Overview

This enhancement adds LMDB (Lightning Memory-Mapped Database) as an alternative storage backend for BlazingMQ's cluster state ledger. The Raft consensus mechanism can now manage both traditional file-based storage and LMDB-based storage through a unified interface.

## Motivation

LMDB provides several advantages for cluster state storage:
- **ACID Compliance**: Full transactional guarantees
- **High Performance**: Memory-mapped B+ tree with excellent read/write performance  
- **Crash Safety**: Data integrity guaranteed even on unexpected shutdowns
- **Simplicity**: Single-level storage with no external dependencies
- **Compact**: Efficient storage with minimal overhead

## Architecture

### Component Structure

```
mqbsi::Ledger (Interface)
    ↓
mqbsl::Ledger (Implementation)
    ↓
mqbsi::LogFactory (Interface)
    ↓
    ├── mqbsl::MemoryMappedOnDiskLogFactory (Existing)
    │       ↓
    │   mqbsl::MemoryMappedOnDiskLog
    │
    └── mqbsl::LmdbLogFactory (NEW)
            ↓
        mqbsl::LmdbLog (NEW)
```

### Key Interfaces

- **mqbsi::Log**: Abstract interface for log storage operations
- **mqbsl::LmdbLog**: LMDB implementation of the Log interface
- **mqbsl::LmdbLogFactory**: Factory for creating LMDB log instances

### Storage Layout

LMDB stores records using a simple key-value model:
- **Key**: Record offset (Int64)
- **Value**: Serialized blob data

Each record is identified by its offset, allowing efficient sequential and random access.

## Configuration

### Schema Changes

Added to `mqbcfg.xsd`:

```xml
<simpleType name='ClusterStateLogType'>
  <restriction base='string'>
    <enumeration value='E_FILE_BACKED'/>  <!-- Default -->
    <enumeration value='E_LMDB'/>          <!-- LMDB storage -->
  </restriction>
</simpleType>

<complexType name='PartitionConfig'>
  <sequence>
    ...
    <element name='cslStorageType' type='tns:ClusterStateLogType' default='E_FILE_BACKED'/>
  </sequence>
</complexType>
```

### Configuration Example

```json
{
  "partitionConfig": {
    "numPartitions": 4,
    "location": "/var/blazingmq/storage",
    "maxCSLFileSize": 67108864,
    "cslStorageType": "E_LMDB"
  }
}
```

## Implementation Details

### LMDB Log Features

1. **Configurable Map Size**: Uses `LogConfig::maxSize()` or defaults to 10GB
2. **Transaction Safety**: All writes are transactional
3. **Read-Only Mode**: Supports opening in read-only mode for replicas
4. **No Aliasing**: Returns copies of data for safety (LMDB data is memory-mapped)
5. **Automatic Sync**: Provides `flush()` operation for explicit synchronization

### Resource Management

- Environment and database handles properly managed
- Clean shutdown with no resource leaks
- Error handling with proper cleanup on all failure paths

### Compatibility

- **Backward Compatible**: File-based storage remains the default
- **Drop-in Replacement**: Same interface as existing log implementations
- **Independent**: Can be enabled/disabled without affecting other storage

## Testing

Unit tests are provided in `mqbsl_lmdblog.t.cpp`:

1. **Breathing Test**: Basic open/write/read/close operations
2. **Reopen Test**: Data persistence across sessions
3. **Error Handling**: Validation of error conditions

## Performance Characteristics

### LMDB Advantages
- Fast sequential writes (append-only workload)
- Excellent random read performance
- Efficient memory usage with memory-mapped I/O
- No write-ahead log overhead

### Trade-offs
- Map size must be pre-configured
- No in-place aliasing (data must be copied)
- Database file may be larger than actual data

## Migration Path

### Enabling LMDB Storage

1. Update cluster configuration with `cslStorageType: E_LMDB`
2. Restart cluster nodes (one at a time for rolling upgrade)
3. New cluster state will be stored in LMDB

### Reverting to File-Based Storage

1. Update configuration back to `cslStorageType: E_FILE_BACKED`
2. Restart cluster nodes
3. Cluster state will rebuild from replicas

**Note**: Direct migration of existing data between storage types is not supported. The cluster will rebuild state from scratch or from replicas.

## Security Considerations

### Vulnerability Assessment
- LMDB dependency checked against GitHub Advisory Database: ✅ No known vulnerabilities
- Proper input validation on all read/write operations
- Safe error handling with no information leakage
- Resource cleanup prevents denial of service

### Best Practices
- Use appropriate file permissions on LMDB database files
- Ensure adequate disk space for map size configuration
- Monitor database size to prevent exhaustion
- Regular backups of cluster state recommended

## Dependencies

### New Dependencies
- **lmdb**: Added to `vcpkg.json` for package management

### Build System Changes
- CMake updated to link LMDB library
- Package member file updated to include LMDB log

## Future Enhancements

Potential future improvements:
1. Online migration between storage types
2. Compression support for stored records
3. Statistics and monitoring integration
4. Automatic map size adjustment
5. Multi-database support for partitioning

## References

- [LMDB Documentation](http://www.lmdb.tech/doc/)
- [BlazingMQ Architecture](https://bloomberg.github.io/blazingmq)
- [Raft Consensus Algorithm](https://raft.github.io/)

## Authors

This enhancement was developed as part of the BlazingMQ project to provide flexible storage options for cluster state management.
