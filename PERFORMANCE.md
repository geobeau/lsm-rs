# Performance logs

This page is used to measure performance improvements and to document tweaks that are made


## Configuration

Lsm-rs is running on a single core.
Memtier benchmark is used with following configuration:
`memtier_benchmark -h 127.0.0.1 -p 11211 -P memcache_binary -t 2 -c 6 --test-time=10`
`memtier_benchmark -h 127.0.0.1 -p 6379 -P resp3 -t 2 -c 7 --test-time=10`

## Baseline

Note: lsm-rs might not be isolated during testing which can cause high P99 

```
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
----------------------------------------------------------------------------------------------------------------------------
Sets        30779.38          ---          ---         0.04475         0.04700         0.05500         0.09500      2371.14 
Gets       307786.73       313.44    307473.29         0.04464         0.04700         0.05500         0.09500     11399.24 
Waits           0.00          ---          ---             ---             ---             ---             ---          --- 
Totals     338566.12       313.44    307473.29         0.04465         0.04700         0.05500         0.09500     13770.38 
```


```
    432,797      syscalls:sys_enter_io_uring_enter                                   
    432,797      syscalls:sys_exit_io_uring_enter
```

