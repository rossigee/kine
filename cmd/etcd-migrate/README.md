# etcd-migrate - etcd to Kine Migration Tool

A command-line tool to migrate data from etcd snapshots to Kine databases.

## Features

- **Snapshot Reading**: Parses etcd snapshot (.db) files using bbolt
- **Batch Processing**: Processes keys in configurable batches for efficiency
- **Progress Tracking**: Real-time progress reporting and statistics
- **Error Handling**: Robust error handling with detailed logging
- **Data Verification**: Optional post-migration verification
- **Dry Run Mode**: Preview what would be migrated without making changes
- **System Key Filtering**: Option to skip ephemeral Kubernetes system keys
- **TLS Support**: Secure connections to Kine endpoints

## Usage

### Basic Migration
```bash
./etcd-migrate -snapshot /path/to/etcd/snapshot.db -kine-endpoint http://localhost:2379
```

### With Verification
```bash
./etcd-migrate -snapshot /path/to/etcd/snapshot.db -kine-endpoint http://localhost:2379 -verify
```

### Dry Run (Preview Mode)
```bash
./etcd-migrate -snapshot /path/to/etcd/snapshot.db -kine-endpoint http://localhost:2379 -dry-run
```

### Skip System Keys
```bash
./etcd-migrate -snapshot /path/to/etcd/snapshot.db -kine-endpoint http://localhost:2379 -skip-system
```

### With TLS
```bash
./etcd-migrate \
  -snapshot /path/to/etcd/snapshot.db \
  -kine-endpoint https://kine.example.com:2379 \
  -tls-cert /path/to/client.crt \
  -tls-key /path/to/client.key \
  -tls-ca /path/to/ca.crt
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-snapshot` | Path to etcd snapshot file (.db) | Required |
| `-kine-endpoint` | Kine endpoint URL | Required |
| `-batch-size` | Number of keys to process in each batch | 100 |
| `-dry-run` | Show what would be migrated without doing it | false |
| `-skip-system` | Skip Kubernetes system keys (events, etc.) | false |
| `-progress` | Report progress every N keys | 1000 |
| `-verify` | Run verification after migration | false |
| `-tls-cert` | TLS certificate file | "" |
| `-tls-key` | TLS private key file | "" |
| `-tls-ca` | TLS CA certificate file | "" |

## Building

```bash
cd cmd/etcd-migrate
go build -o etcd-migrate .
```

## Migration Process

1. **Snapshot Analysis**: Opens and analyzes the etcd snapshot file
2. **Key Counting**: Counts total keys and identifies skippable keys
3. **Batch Processing**: Processes keys in configurable batches
4. **Data Transfer**: Creates or updates keys in Kine using transactions
5. **Progress Reporting**: Reports progress and statistics
6. **Verification** (optional): Compares source and destination data

## System Keys Filtering

When `-skip-system` is enabled, the following key prefixes are skipped:
- `/registry/events/` - Kubernetes events (ephemeral)
- `/registry/minions/` - Node information (often regenerated)
- `/registry/ranges/serviceips` - Service IP allocations
- `/registry/ranges/servicenodeports` - NodePort allocations

## Error Handling

- **Key Conflicts**: Attempts to update existing keys
- **Connection Issues**: Provides detailed error messages
- **Data Corruption**: Validates data integrity where possible
- **Partial Failures**: Continues processing remaining keys

## Performance

- **Batch Processing**: Configurable batch sizes (default: 100 keys)
- **Progress Reporting**: Regular status updates (default: every 1000 keys)
- **Memory Efficient**: Processes data in streams rather than loading all into memory
- **Concurrent Safe**: Uses Kine's transaction-based operations

## Verification

The verification process:
1. Reads each key from the original etcd snapshot
2. Retrieves the corresponding key from Kine
3. Compares values for exact matches
4. Reports missing keys and value differences
5. Provides detailed statistics

## Limitations

1. **Revision Mapping**: etcd's complex revision system is simplified for Kine
2. **Lease Handling**: Advanced lease features may not be fully preserved
3. **Concurrent Access**: Should be run when Kine is not under heavy load
4. **Large Values**: Very large values (>1MB) may need special handling

## Examples

### Migrate Production etcd Backup
```bash
# First, run a dry run to see what would be migrated
./etcd-migrate -snapshot backup-2023-12-01.db -kine-endpoint http://kine:2379 -dry-run

# Run the actual migration with verification
./etcd-migrate -snapshot backup-2023-12-01.db -kine-endpoint http://kine:2379 -verify -skip-system
```

### Large Dataset Migration
```bash
# Increase batch size and reduce progress reporting for large datasets
./etcd-migrate \
  -snapshot large-backup.db \
  -kine-endpoint http://kine:2379 \
  -batch-size 500 \
  -progress 5000
```

## Troubleshooting

### Common Issues

1. **"key bucket not found"**: Ensure the snapshot file is a valid etcd backup
2. **"connection refused"**: Verify Kine endpoint is accessible
3. **"key exists" errors**: Normal for updates, but may indicate duplicate processing
4. **TLS errors**: Check certificate paths and validity

### Performance Tuning

- Increase `-batch-size` for faster processing (default: 100)
- Decrease `-progress` reporting frequency for large datasets
- Run during low-traffic periods for better performance
- Ensure adequate disk space on the Kine backend

## Safety

- Always test with `-dry-run` first
- Take Kine backups before migration
- Use `-verify` to ensure data integrity
- Monitor system resources during migration
- Consider using `-skip-system` for Kubernetes environments