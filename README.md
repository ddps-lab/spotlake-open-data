# SpotLake AWS Spot Instance Dataset (2025 Sample)

SpotLake is a spot instance dataset archive service operated by the
[Distributed Data Processing Systems Lab (DDPS)](https://ddps.cloud) at Hanyang University.
This dataset is a static, one-year sample of the AWS portion of SpotLake: every
10-minute snapshot collected from 2025-01-01 00:00 UTC to 2025-12-31 23:50 UTC.

Each snapshot records, for every (instance type, Availability Zone) pair offered on the
AWS spot market, the Spot Placement Score, the Spot Instance Advisor interruption
frequency band, the current spot price, the on-demand price, and the resulting savings.

| Item | Value |
| -- | -- |
| Coverage | 2025-01-01 00:00 UTC to 2025-12-31 23:50 UTC |
| Snapshot interval | 10 minutes |
| Objects | 52,372 gzip-compressed CSV files (188 of 52,560 slots missing, see Known issues) |
| Size | 19.0 GB compressed (17.73 GiB) |
| Rows | 1,766,104,605 (about 31,000 per snapshot in January to 38,000 in December) |
| Instance types / regions / AZs | about 1,079 / 17 / 55 (2025-12-31 snapshot) |
| Updates | None. This is a static sample. |
| License | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |

## Accessing the data

The bucket is public. No AWS account is needed.

```bash
aws s3 ls --no-sign-request s3://spotlake-open-data/data/aws/2025/01/01/
aws s3 cp --no-sign-request s3://spotlake-open-data/data/aws/2025/01/01/00-00-00.csv.gz .
```

```python
import pandas as pd  # reading s3:// paths also needs the s3fs package
df = pd.read_csv(
    "s3://spotlake-open-data/data/aws/2025/01/01/00-00-00.csv.gz",
    storage_options={"anon": True},
)
```

A guided tour is available in the
[Get To Know A Dataset notebook](get-to-know-a-dataset.ipynb).

## Bucket layout

```
s3://spotlake-open-data/
  README.md
  LICENSE.txt
  data/
    aws/
      2025/
        MM/
          DD/
            HH-MM-SS.csv.gz      one snapshot, timestamp in UTC
```

The `data/aws/` prefix leaves room for other cloud vendors without moving existing objects.

## Schema

Every file has the same 11 columns. Missing values are empty cells.

| Column | Type | Description |
| -- | -- | -- |
| `Time` | string | Snapshot time, `YYYY-MM-DD HH:MM:SS`, UTC. Equal to the object key time. |
| `InstanceType` | string | EC2 instance type, e.g. `m7i.large`. |
| `Region` | string | AWS region code, e.g. `us-east-1`. |
| `AZ` | string | Availability Zone **ID**, e.g. `use1-az1`. AZ IDs, unlike AZ names, identify the same physical zone across accounts. |
| `SPS` | integer | Spot Placement Score of the pool, 1 (low) to 10 (high). See [Placement columns](#placement-columns). Empty when the pool has no score. |
| `T3` | integer | Largest target capacity, among 1, 5, 10, ..., 50, that recently scored 3 or higher. `0` if none. Empty when `SPS` is empty. |
| `T2` | integer | Same as `T3` for a score of 2 or higher. `0` if none. Empty when `SPS` is empty. |
| `IF` | float | Spot Instance Advisor interruption frequency band of the (region, instance type), as a grade: `3.0` = <5%, `2.5` = 5-10%, `2.0` = 10-15%, `1.5` = 15-20%, `1.0` = >20%. Identical across AZs of a region. Empty if not listed. |
| `OndemandPrice` | float | On-demand Linux price, USD per hour. Empty if not sold on demand. |
| `SpotPrice` | float | Current spot price, USD per hour, Linux/UNIX. Empty if unavailable. |
| `Savings` | integer | `100 - SpotPrice / OndemandPrice * 100`, truncated to an integer percent. Empty if either price is missing. |

In `T2` and `T3`, `0` is a value ("no target capacity qualified"), not a missing value.

## How the data was collected

All values come from public AWS APIs or public AWS data.

| Column | Source |
| -- | -- |
| `SpotPrice` | EC2 [`DescribeSpotPriceHistory`](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotPriceHistory.html), all regions, Linux/UNIX only, latest price at collection time. AZ names are converted to AZ IDs with [`DescribeAvailabilityZones`](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeAvailabilityZones.html). Every 10 minutes. |
| `OndemandPrice` | AWS Price List API [`GetProducts`](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_pricing_GetProducts.html), service code `AmazonEC2`. |
| `IF` | [Spot Instance Advisor](https://aws.amazon.com/ec2/spot/instance-advisor/) data, read through the open source CLI [`spotinfo`](https://github.com/alexei-led/spotinfo) (`spotinfo --output csv --region all`). Every 10 minutes. |
| `SPS`, `T2`, `T3` | EC2 [`GetSpotPlacementScores`](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_GetSpotPlacementScores.html) with `SingleAvailabilityZone=true`, one instance type per query, at target capacities 1, 5, 10, ..., 50. |

Spot Placement Score queries are limited per account. To cover every instance type, region,
and target capacity, SpotLake spreads queries over several AWS accounts. Every query is an
ordinary call to the public API, and only the returned score is stored.

### Placement columns

The query schedule changed once during 2025:

| Period | Target capacity 1 | Target capacities 5 to 50 |
| -- | -- | -- |
| until 2025-02-12 | every 10 minutes | one per 10 minutes, rotating (100-minute cycle) |
| from 2025-02-13 | one capacity per 10 minutes, rotating through 1, 5, ..., 50 (110-minute cycle) | (same rotation) |

Placement columns are state carried between queries:

- `SPS` is the latest target capacity 1 score. When a larger target capacity returns a higher
  score, `SPS` is raised to it. Target capacity 1 itself is queried every 10 minutes until
  2025-02-12 and once per 110-minute rotation from 2025-02-13, but a raise can happen at any
  10-minute step.
- `T3` (`T2`) records the largest target capacity whose score was 3 (2) or higher. When a query
  at any target capacity does not qualify, the value is capped just below that capacity (for
  example, a failing query at 5 caps it at 1). Both reset to 0 when the target capacity 1 score
  drops below the threshold.

## Processing applied for this release

The SpotLake service went through several collector versions during 2025, so its stored
files are not uniform. This release rebuilds them so the whole year follows one definition:

- **Placement columns recomputed.** `SPS`, `T2`, and `T3` were recomputed for every row by
  replaying all raw placement score queries from 2024-12-01 (warm-up) through 2025 with the
  current SpotLake logic. Before 2025-02-13 each slot replays its target capacity 1 query and
  then its rotating target capacity query. For the 99 slots whose snapshot is missing but whose
  query was saved, the query still updates the carried state. The replay was validated against service output
  produced by the current logic (2026-09-20 to 2026-09-26, 48.5 million rows): every `SPS`,
  `T2`, and `T3` value matched.
- **Uniform columns.** Original files before 2025-02-15 had no `T2` and `T3` columns.
- **Uniform missing values.** The original files used `-1` for missing `IF`, prices, and
  `Savings`, and empty cells for missing scores. All missing values are now empty cells.
- **Duplicate rows removed.** From 2025-10-20 07:40 UTC some original us-east-1 rows were
  repeated with identical values. One copy is kept (3,936,398 rows removed).
- **Two prices for one pool kept.** In a few snapshots the spot price API returned two prices
  for the same pool. Both rows are kept, with the same `SPS`, `T2`, and `T3`. In 2025 this happens once (two pools
  at 2025-01-27 22:00 UTC).
- All other values are unchanged.

Compared with the original files, the recomputation changed `SPS` in 4,462,185 rows (0.25%),
and `T3` and `T2` in 26,290,418 (1.68%) and 33,783,226 (2.16%) of the rows whose original file
had these columns. Almost all `SPS` changes fall on 2025-02-13 and 2025-02-14, the first two
days of the per-target collector, which ran an earlier version of the carry logic. `T2` and
`T3` changes come mainly from the `== 2` and `== 3` conditions used during 2025.
The script is [`scripts/reprocess_2025.py`](scripts/reprocess_2025.py).

## Known issues

- **Missing snapshots.** 188 of 52,560 10-minute slots are missing. Most gaps are one or two
  slots. Longer outages: 2025-02-13 04:40-05:30, 2025-06-25 18:40-23:50 (three gaps),
  2025-09-08 06:30-06:50, 2025-11-29 01:30-10:40, 2025-12-31 09:50-10:50 (UTC).
- **Placement score scale change on 2025-09-25.** Until 2025-09-24 every score was 3 or lower.
  From 2025-09-25, P-family GPU instances (p4d, p5, p5e, p5en, p6-b200, and others) report
  scores from 4 to 9 for single instance type queries, while all other families stay at 3 or lower.
  This change comes from AWS and is reflected as is. AWS documents that queries with fewer than
  three instance types receive low scores, and SpotLake queries always use one.
- **Refresh rate of `SPS`.** Target capacity 1 is queried every 10 minutes until 2025-02-12 and
  every 110 minutes after that, as described above.

## License

The dataset is released under the
[Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

## Citation

If you use this dataset, please cite:

> S. Lee, J. Hwang, and K. Lee, "SpotLake: Diverse Spot Instance Dataset Archive Service,"
> in Proc. IEEE International Symposium on Workload Characterization (IISWC), 2022.
> https://doi.org/10.1109/IISWC55918.2022.00029

## Contact

DDPS Lab, Hanyang University: spotlake@hanyang.ac.kr
Project: https://github.com/ddps-lab/spotlake
