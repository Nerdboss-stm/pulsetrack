# S3 Partition Benchmark — Real-S3 Results

- Started: 2026-05-10T18:32:12.069491+00:00
- Ended:   2026-05-10T18:43:59.874205+00:00
- Records: 1,000,000 primary + 500,000 herd
- Devices: 1,000

| Strategy | Wall-Clock (s) | Files | Partition Dirs | Bytes | S3 PUT Requests | 5xx Errors | Notes |
|----------|---------------:|------:|---------------:|------:|----------------:|-----------:|-------|
| `date_first` | 106.43 | 2,000 | 1,000 | 6,563,762 | — | — |  |
| `reversed_id` | 97.40 | 2,000 | 1,000 | 6,563,762 | — | — |  |
| `hash_bucket` | 24.66 | 512 | 256 | 3,375,709 | — | — |  |
| `date_first` | 85.68 | 4,000 | 1,000 | 9,452,832 | — | — | thundering-herd: all records on 1 day |
| `reversed_id` | 87.24 | 4,000 | 1,000 | 9,452,832 | — | — | thundering-herd: all records on 1 day |
| `hash_bucket` | 23.12 | 1,024 | 256 | 3,364,816 | — | — | thundering-herd: all records on 1 day |
