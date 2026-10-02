"""Generate get-to-know-a-dataset.ipynb from the AWS Open Data template structure.

Edit the cells here and rerun, so the notebook stays reviewable as plain Python.
"""
from pathlib import Path

import nbformat as nbf

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell

cells = [
    md("""# Get to Know a Dataset: SpotLake AWS Spot Instance Dataset (2025 Sample)

This notebook serves as a guided tour of the [SpotLake AWS Spot Instance Dataset (2025 Sample)](https://registry.opendata.aws/spotlake) dataset. More usage examples, tutorials, and documentation for this dataset and others can be found at the [Registry of Open Data on AWS](https://registry.opendata.aws/).

SpotLake is a spot instance dataset archive service run by the DDPS Lab at Hanyang University. This sample holds every 10-minute AWS snapshot of 2025: for each instance type and Availability Zone, the Spot Placement Score, the interruption frequency band, the spot price, the on-demand price, and the savings."""),
    md("""### Q: How have you organized your dataset? Help us understand the key prefix structure of your S3 bucket.

At the top level of the bucket there are a `README.md`, a `LICENSE.txt`, and a `data/` prefix. Under `data/`, objects are grouped by cloud vendor and then by UTC date, one object per 10-minute snapshot:

```
data/aws/2025/MM/DD/HH-MM-SS.csv.gz
```

So a single day is 144 objects and the whole year is 52,372 objects (a few slots are missing because of collection outages)."""),
    code("""# This notebook requires the following additional libraries
# (please install using the preferred method for your environment, e.g. pip, conda):
#
# boto3 >= 1.34
# pandas >= 2.0
# matplotlib >= 3.9

import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import matplotlib.pyplot as plt
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config"""),
    md("Next, we define the location of the dataset and create an S3 client. The bucket is public, so requests do not need to be signed."),
    code("""bucket = os.environ.get("SPOTLAKE_BUCKET", "spotlake-open-data")
prefix = os.environ.get("SPOTLAKE_PREFIX", "data/aws/2025/")

if os.environ.get("SPOTLAKE_PROFILE"):  # signed access, only for testing against a private copy
    s3 = boto3.Session(profile_name=os.environ["SPOTLAKE_PROFILE"]).client("s3")
else:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Months available under the year prefix
for p in s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")["CommonPrefixes"]:
    print(p["Prefix"])"""),
    md("Each month holds one prefix per day, and each day holds the 10-minute snapshots of that day."),
    code("""day_prefix = prefix + "06/15/"
day_keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=day_prefix)["Contents"]]
print(len(day_keys), "snapshots on 2025-06-15")
day_keys[:5]"""),
    md("""### Q: What data formats are present in your dataset? What kinds of data are stored using these formats? Can you give us some insight into why you chose these formats?

Every snapshot is a gzip-compressed CSV file with a header row. CSV is the format SpotLake has always distributed, and it opens with any tool, from `pandas` and `polars` to spreadsheet software. Gzip cuts each snapshot to roughly 300 to 500 KB.

Every file has the same 11 columns:

| Column | Meaning |
| -- | -- |
| `Time` | snapshot time, UTC |
| `InstanceType`, `Region`, `AZ` | the spot pool; `AZ` is an Availability Zone ID such as `use1-az1` |
| `SPS` | Spot Placement Score for one instance, 1 (low) to 10 (high) |
| `T3`, `T2` | largest target capacity (1, 5, ..., 50) that recently scored 3 or higher, or 2 or higher |
| `IF` | interruption frequency grade: 3.0 = <5%, 2.5 = 5-10%, 2.0 = 10-15%, 1.5 = 15-20%, 1.0 = >20% |
| `OndemandPrice`, `SpotPrice` | USD per hour, Linux |
| `Savings` | integer percent saved by spot versus on-demand |

Missing values are empty cells, which `pandas` reads as `NaN`. In `T2` and `T3`, `0` is not missing: it means no target capacity qualified.

Prices and `IF` are refreshed every 10 minutes. Placement scores are carried forward between queries: target capacity 1 is queried every 10 minutes until 2025-02-12 and every 110 minutes after that, and a higher score at a larger target capacity can raise `SPS` at any 10-minute step. The dataset README explains the collection schedule and how `T2` and `T3` were recomputed.

For analysis across many snapshots, Amazon Athena can query the CSV files in place, and AWS Glue can convert them to Parquet in your own account."""),
    md("""### Q: Can you show us an example of downloading and loading data from your dataset?

Let us load the snapshot taken at 2025-06-15 12:00 UTC."""),
    code("""def load(key):
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
        return pd.read_csv(body, compression="gzip")

df = load(prefix + "06/15/12-00-00.csv.gz")
print(df.shape)
df.head()"""),
    md("One row is one spot pool, that is, one instance type in one Availability Zone. Here is the size of a snapshot along each dimension, and how many values are missing in each column."),
    code("""print("instance types:", df.InstanceType.nunique())
print("regions:", df.Region.nunique())
print("availability zones:", df.AZ.nunique())
df.isna().sum()"""),
    md("""### Q: A picture is worth a thousand words. Show us a visual (or several!) from your dataset that either illustrates something informative about your dataset, or that you think might excite someone to dig in further.

How much do spot instances save, and does it depend on the region? We drop rows without a savings value and plot the distribution of savings for the regions with the most pools."""),
    code("""valid = df.dropna(subset=["Savings"])
top_regions = valid.Region.value_counts().index[:6]

fig, ax = plt.subplots(figsize=(10, 5))
ax.boxplot([valid[valid.Region == r].Savings for r in top_regions], tick_labels=top_regions)
ax.set_ylabel("Savings over on-demand (%)")
ax.set_title("Spot savings per pool, 2025-06-15 12:00 UTC")
ax.grid(axis="y", alpha=0.3)
plt.show()"""),
    md("Prices and placement scores move during the day. We load the 24 hourly snapshots of the same day and pick the instance type and region whose placement scores changed most often, then plot every Availability Zone of it."),
    code("""hourly = [prefix + f"06/15/{h:02d}-00-00.csv.gz" for h in range(24)]
with ThreadPoolExecutor(8) as pool:
    day = pd.concat(pool.map(load, hourly), ignore_index=True)
day["Time"] = pd.to_datetime(day.Time)

changes = (day.sort_values("Time")
           .groupby(["InstanceType", "Region", "AZ"]).SPS
           .apply(lambda x: (x.diff().fillna(0) != 0).sum())
           .groupby(level=["InstanceType", "Region"]).sum())
itype, region = changes.idxmax()
pool_df = day[(day.InstanceType == itype) & (day.Region == region)]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
for az, g in pool_df.groupby("AZ"):
    ax1.step(g.Time, g.SpotPrice, where="post", label=az)
    ax2.step(g.Time, g.SPS, where="post", label=az)
ax1.set_ylabel("Spot price (USD/h)")
ax2.set_ylabel("Placement score")
ax1.set_title(f"{itype} in {region}, 2025-06-15 (hourly)")
ax1.legend(ncol=3, fontsize=8)
plt.show()"""),
    md("""### Q: What is one question that you have answered using these data? Can you show us how you came to that answer?

**Is the cheapest spot pool also the easiest one to get?** A natural strategy is to pick the pool with the largest savings. SpotLake was built partly to test that assumption. In one snapshot we group pools by their savings and look at how their placement scores are distributed. If cheap pools were also the most available ones, the share of score 3 would rise with savings."""),
    code("""scored = valid.dropna(subset=["SPS"]).copy()
scored = scored[scored.SPS <= 3]  # keep the 1 to 3 scale shared by all instance families
scored["savings_band"] = pd.cut(scored.Savings, bins=[0, 50, 60, 70, 80, 90, 100], include_lowest=True)
share = pd.crosstab(scored.savings_band, scored.SPS.astype(int), normalize="index")

share.plot(kind="bar", stacked=True, figsize=(10, 5), color=["#d62728", "#ff7f0e", "#2ca02c"])
plt.ylabel("Share of pools")
plt.xlabel("Savings band (%)")
plt.title("Placement score by savings band, 2025-06-15 12:00 UTC")
plt.legend(title="SPS")
plt.show()
share.round(3)"""),
    md("""In this snapshot the answer is no. The share of pools scoring 3 is highest in the 50 to 60% savings band (about 86%) and falls steadily as savings grow, down to about 63% in the 80 to 90% band. The deepest discounts tend to sit in pools with less spare capacity.

Savings and placement scores are not simply aligned. The interruption frequency grade (`IF`) from the Spot Instance Advisor adds a third view, and the three signals often disagree for the same pool. SpotLake's papers discuss this in detail, and comparing the signals across time is a good way to start using the dataset.

To scale this up, repeat the analysis over many snapshots. Loading all 52,372 objects takes about 18 GiB of transfer, so a common approach is to pick one snapshot per hour or per day, or to query the files with Amazon Athena."""),
    md("""### Q: What is one unanswered question that you think could be answered using these data? Do you have any recommendations or advice for someone wanting to answer this question?

**Can we predict a drop in placement score before it happens?** Spot prices, interruption bands, and placement scores are recorded every 10 minutes for the same pools. A model that warns of an upcoming score drop, or a sharp price move, from the recent history of a pool and of its neighbours (same family in other AZs, other sizes in the same AZ) would directly help schedulers move work before capacity disappears.

Some advice:

- Build time series per (`InstanceType`, `AZ`). Pools come and go, so expect gaps.
- Treat empty cells as missing, not as low values.
- From 2025-02-13, target capacity 1 is queried once per 110-minute rotation, so most `SPS` changes follow that cadence. Changes in between come from higher scores at larger target capacities.
- From 2025-09-25, P-family GPU instances report scores above 3 while other families stay at 3 or lower. Model the two groups separately or rescale them.
- `T2` and `T3` are carried between queries like `SPS`. The dataset README describes how they are maintained."""),
]

nb = nbf.v4.new_notebook()
nb["cells"] = cells
nb["metadata"] = {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}}
out = Path(__file__).resolve().parent / "get-to-know-a-dataset.ipynb"
nbf.write(nb, out)
print(f"wrote {out}")
