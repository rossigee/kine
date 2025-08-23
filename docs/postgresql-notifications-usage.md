# PostgreSQL Event-Driven Notifications - Usage Guide

Complete guide for using Kine's PostgreSQL LISTEN/NOTIFY implementation for improved performance.

## Quick Start

### Enable Event-Driven Mode (Default)
```bash
kine --endpoint="postgres://user:pass@host/db"
```

### Disable Event-Driven Mode (Polling Only)
```bash
kine --endpoint="postgres://user:pass@host/db" --disable-notifications
```

## Configuration Options

### Command-Line Flags

| Flag | Default | Description | Environment Variable |
|------|---------|-------------|---------------------|
| `--disable-notifications` | `false` | Disable event-driven notifications, use polling only | `KINE_DISABLE_NOTIFICATIONS` |
| `--notification-buffer-size` | `1024` | Size of notification channel buffer | `KINE_NOTIFICATION_BUFFER_SIZE` |

### Environment Variables

Set environment variables for containerized deployments:

```bash
# Disable notifications
export KINE_DISABLE_NOTIFICATIONS=true

# Adjust buffer size for high-traffic environments
export KINE_NOTIFICATION_BUFFER_SIZE=2048

# Start Kine
kine --endpoint="postgres://user:pass@host/db"
```

### Configuration Examples

#### Development Environment
```bash
# Small buffer, detailed logging
kine --endpoint="postgres://postgres:postgres@localhost/kubernetes" \
     --notification-buffer-size=512 \
     --debug
```

#### Production Environment
```bash
# Large buffer, optimized for performance
kine --endpoint="postgres://kine:secret@db.example.com/k8s?sslmode=require" \
     --notification-buffer-size=4096 \
     --compact-interval=10m
```

#### High Availability Setup
```bash
# Multiple Kine instances with notifications
kine --endpoint="postgres://kine:secret@db1.example.com,db2.example.com/k8s?sslmode=require" \
     --listen-address="0.0.0.0:2379" \
     --notification-buffer-size=2048
```

#### Fallback Mode (Polling Only)
```bash
# Force polling mode for troubleshooting
kine --endpoint="postgres://kine:secret@db.example.com/k8s" \
     --disable-notifications \
     --poll-batch-size=1000
```

## Performance Tuning

### Buffer Size Guidelines

| Environment | Recommended Size | Rationale |
|-------------|------------------|-----------|
| Development | 512-1024 | Low traffic, memory efficient |
| Production | 1024-4096 | Balanced performance and memory |
| High Traffic | 2048-8192 | Handle burst notifications |
| Constrained Memory | 256-512 | Minimize memory usage |

### Buffer Size Formula
```
Buffer Size = (Peak Writes/Second) * (Notification Latency in Seconds) * (Safety Factor)
```

Example:
- 1000 writes/sec
- 0.01s notification latency
- 2x safety factor
- Buffer Size = 1000 * 0.01 * 2 = 20 (minimum 256)

### Performance Monitoring

#### Key Metrics

Monitor these Prometheus metrics:

```
# Notification success rate
rate(kine_notifications_total{result="success"}[5m])

# Notification latency
histogram_quantile(0.95, kine_notification_latency_seconds)

# Queue utilization
kine_notification_queue_size / notification_buffer_size

# Dropped notifications (should be 0)
rate(kine_notifications_total{result="dropped"}[5m])
```

#### Grafana Dashboard Query Examples

```promql
# Notification Rate
rate(kine_notifications_total[5m])

# Average Latency
rate(kine_notification_latency_seconds_sum[5m]) / rate(kine_notification_latency_seconds_count[5m])

# Queue Utilization %
(kine_notification_queue_size / 1024) * 100

# Error Rate
rate(kine_notifications_total{result!="success"}[5m])
```

## Troubleshooting

### Common Issues

#### 1. Notifications Not Working
**Symptoms**: Slow watch responses, polling logs every second

**Check**:
```bash
# Verify trigger exists
psql -d your_db -c "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'kine_insert_trigger';"

# Check Kine logs
grep "notification listener" /var/log/kine.log
```

**Solutions**:
- Ensure database user has necessary permissions
- Check network connectivity to PostgreSQL
- Verify trigger was created during migration

#### 2. High Memory Usage
**Symptoms**: Kine memory usage growing, OOM errors

**Check**:
```bash
# Monitor queue size
curl -s localhost:8080/metrics | grep kine_notification_queue_size
```

**Solutions**:
- Reduce `--notification-buffer-size`
- Investigate notification processing bottlenecks
- Scale horizontally with multiple Kine instances

#### 3. Dropped Notifications
**Symptoms**: Metrics showing `result="dropped"`

**Check**:
```bash
# Check drop rate
curl -s localhost:8080/metrics | grep 'kine_notifications_total.*dropped'
```

**Solutions**:
- Increase `--notification-buffer-size`
- Optimize application read patterns
- Scale PostgreSQL read replicas

#### 4. Connection Issues
**Symptoms**: Frequent "notification listener disconnected" logs

**Check**:
- PostgreSQL connection limits
- Network stability between Kine and PostgreSQL
- PostgreSQL configuration (timeouts, idle connection handling)

**Solutions**:
```postgresql
-- Increase PostgreSQL connection limits
ALTER SYSTEM SET max_connections = 200;

-- Adjust connection timeouts
ALTER SYSTEM SET tcp_keepalives_idle = 300;
ALTER SYSTEM SET tcp_keepalives_interval = 30;
ALTER SYSTEM SET tcp_keepalives_count = 3;
```

### Diagnostic Commands

```bash
# Test notification functionality
./scripts/test-notifications

# Run integration tests
./scripts/test-postgres-integration

# Performance comparison
./scripts/benchmark-comparison 10

# Check PostgreSQL permissions
psql -d your_db -c "SELECT has_function_privilege(current_user, 'kine_notify()', 'execute');"
```

### Log Analysis

#### Successful Startup
```
INFO PostgreSQL event-driven notifications enabled
INFO PostgreSQL notification listener started
```

#### Notification Activity
```
TRACE Received notification for revision: 12345
TRACE Event-driven notification received for revision: 12345
```

#### Error Patterns
```
ERROR Failed to create notification connection: connection refused
WARN Notification listener disconnected, attempting to reconnect...
ERROR Invalid notification payload: invalid_data
```

## Best Practices

### Production Deployment

1. **Always Enable Notifications**: Don't disable unless troubleshooting
2. **Monitor Metrics**: Set up alerts for dropped notifications
3. **Size Buffers Appropriately**: Start with 2048, adjust based on metrics  
4. **Use Connection Pooling**: Configure PostgreSQL connection pooling
5. **Plan for Failover**: Test behavior when PostgreSQL becomes unavailable

### Development Best Practices

1. **Test Both Modes**: Verify functionality with and without notifications
2. **Use Debug Logging**: Enable `--debug` during development
3. **Monitor Resource Usage**: Watch memory and CPU usage patterns
4. **Test Edge Cases**: Simulate high load and connection failures

### Security Considerations

1. **Database Permissions**: Grant minimal required permissions
2. **Network Security**: Use TLS for PostgreSQL connections
3. **Resource Limits**: Set appropriate buffer limits to prevent DoS
4. **Monitoring**: Monitor for anomalous notification patterns

## Migration Guide

### From Polling to Event-Driven

1. **Backup**: Always backup your database
2. **Test**: Verify in staging environment first
3. **Deploy**: Update Kine with new version
4. **Monitor**: Watch metrics for successful transition
5. **Rollback Plan**: Keep option to add `--disable-notifications`

### Migration Commands

```bash
# 1. Backup database
pg_dump your_db > backup_$(date +%Y%m%d).sql

# 2. Update Kine deployment (Kubernetes example)
kubectl set image deployment/kine kine=kine:new-version

# 3. Verify notifications enabled
kubectl logs deployment/kine | grep "notification listener started"

# 4. Monitor metrics
kubectl port-forward svc/kine 8080:8080
curl localhost:8080/metrics | grep kine_notifications
```

## Frequently Asked Questions

### Q: Does this work with PostgreSQL read replicas?
A: Notifications only work on the primary database. Read replicas can be used for read queries but notifications must come from the primary.

### Q: What happens if the notification channel fills up?
A: New notifications are dropped (logged and metered), but polling continues as a fallback mechanism.

### Q: Can I use this with existing Kine deployments?
A: Yes, the feature is backward compatible. Existing deployments will automatically get notifications after upgrade.

### Q: How does this affect PostgreSQL performance?
A: Minimal impact. The trigger adds microseconds per INSERT, and LISTEN/NOTIFY is very efficient.

### Q: What if I need to disable notifications temporarily?
A: Set `KINE_DISABLE_NOTIFICATIONS=true` or add `--disable-notifications` flag.

## Performance Comparison

| Configuration | Watch Latency | CPU Usage | Memory Usage | Recommended For |
|---------------|---------------|-----------|--------------|-----------------|
| Event-Driven (default) | <10ms | Low | 1024-4096 buffer | Production |
| Event-Driven (large buffer) | <5ms | Low | 4096-8192 buffer | High traffic |
| Polling Mode | 0-1000ms | Medium | Lower | Troubleshooting |

## Support

For issues or questions:
1. Check logs with `--debug` enabled
2. Run diagnostic scripts in `/scripts/`
3. Review metrics at `/metrics` endpoint
4. See troubleshooting section above