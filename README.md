# Rearc Data Quest

This repository is my submission for the Rearc data quest. It covers all four
parts: sourcing and republishing a BLS dataset to S3, fetching population data
from the DataUSA API, analyzing both datasets, and automating the whole flow as
an infrastructure-as-code data pipeline.

Parts 1 through 3 are implemented as Databricks notebooks. Part 4 is a Terraform
data pipeline that orchestrates those notebooks as Databricks Jobs on a schedule
and in response to S3 events.

## Submission at a glance

| Part | What it does | Where to find it |
|------|--------------|------------------|
| 1 | Sync the BLS `pr` time-series dataset to S3, kept in step with the source | `part_1.ipynb` |
| 2 | Fetch DataUSA population data and save it as JSON in S3 | `part_2.ipynb` |
| 3 | Analytics: population mean/stddev, best year per series, and a joined series/population report | `part_3.ipynb` (run output in `part_3_with_outputs.ipynb`, dashboard in `part_3_analysis_dashboard.pdf`) |
| 4 | Terraform data pipeline (scheduled Lambda, S3-to-SQS notification, consumer Lambda) | `part_4/` |

Shared PySpark helper functions used across the notebooks live in
`common.ipynb`.

## Data link (Part 1)

The republished BLS dataset is publicly readable in S3. Browse and
download urls are in the [Access to the data](#access-to-the-data) section
below.

## Architecture decisions

### The Lambdas orchestrate Databricks; they do not run the data logic inline

The quest's Part 4 describes a Lambda that executes Parts 1 and 2 directly and a
consumer Lambda that outputs the Part 3 reports. This submission satisfies that
intent with a deliberate variation: the data logic for Parts 1-3 lives in
Databricks notebooks, wrapped as Databricks Jobs, and the Lambdas trigger those
jobs rather than executing the logic themselves.

Concretely:

- The **scheduled Lambda** triggers the `parts_1_and_2` Databricks Job, which
  runs the Part 1 (BLS sync) notebook and then the Part 2 (DataUSA API fetch)
  notebook as dependent tasks. That job writes the JSON population file to S3.
- The JSON file landing in S3 emits an event notification to an SQS queue.
- The **consumer Lambda**, invoked by that queue, triggers the `part_3`
  Databricks Job, which runs the Part 3 analytics notebook and refreshes the
  analysis dashboard.

So the pipeline shape the quest asks for, daily execution of Parts 1-2, an
S3-notification-driven SQS queue, and a consumer that produces the Part 3
reports, is fully present. What differs is the execution layer: Databricks does
the work, and the Lambdas orchestrate.

The Databricks Jobs are defined declaratively in
`part_4/databricks_jobs/parts_1_and_2.yaml` and `part_4/databricks_jobs/part_3.yaml`,
and they source the notebooks from Git, so the job definitions are part of the
submission rather than click-configured state.

### Why this choice

- **The role targets Databricks skills.** Implementing the data work as
  Databricks notebooks and jobs demonstrates the exact capability being
  evaluated, rather than hiding it inside Lambda handlers.
- **The Part 3 analytics are native Spark work.** The reports rely on Spark
  dataframe operations and joins across the BLS time-series and the population
  data (see the shared helpers in `common.ipynb`). Reimplementing them in a
  Lambda would mean either packaging Spark into Lambda (awkward) or rewriting in
  pandas (discarding the Spark work the quest is assessing). Triggering
  Databricks is the more natural execution model for this analysis.
- **The quest sets a floor, not a ceiling.** "Just logging the results of the
  queries would be enough" establishes a minimum output bar. Using Databricks as
  the compute layer clears that bar with richer, more production-representative
  tooling.

### The tradeoff, stated honestly

A Lambda-native implementation would be fully self-contained: deploy the
Terraform and everything runs with no external system. This design instead has an
external dependency, a Databricks workspace with the jobs defined and a token
provisioned. That is a real cost for a reviewer who wants to clone and run in
isolation.

In the production context this design targets, that dependency is reasonable: a
Databricks-centric data platform already hosts the workspace and jobs, so
orchestrating them from lightweight Lambdas is a sensible division of labor
rather than an added burden. The `part_4/README.md` documents exactly what must
exist on the Databricks side for the pipeline to run.

## Access to the data

The dataset in S3 is the deliverable for Part 1, so the bucket is publicly
readable by the reviewers.

**Key files (direct download):**

- BLS `pr` current time-series (Part 1, consumed by Part 3):
  `https://rearc-quest-107628756615-us-east-2-an.s3.us-east-2.amazonaws.com/bls-data/pr/pr.data.0.Current`
- DataUSA population JSON (Part 2 output, consumed by Part 3):
  `https://rearc-quest-107628756615-us-east-2-an.s3.us-east-2.amazonaws.com/datausa/annual_us_pop_2013_thru_2024.json`

The complete BLS `pr` dataset (all series, mappings, and documentation files)
is republished under the `bls-data/pr/` prefix.

**Browse every object:** the following URL returns a listing of the full bucket
contents as an S3 XML document (the native S3 listing format, not a styled web
page):

`https://rearc-quest-107628756615-us-east-2-an.s3.us-east-2.amazonaws.com/?list-type=2`

Read access is granted through a scoped **bucket policy** allowing
`s3:GetObject` and `s3:ListBucket`, rather than through object ACLs. This is the
current AWS-recommended mechanism for shared read access and keeps object
ownership enforced and ACLs disabled. The data is open BLS data being
republished, so public read of these specific objects is appropriate to the
task.

## Repository layout

```
.
|-- README.md                          (this file)
|-- common.ipynb                       Shared PySpark helper functions
|-- part_1.ipynb                       Part 1: BLS -> S3 sync
|-- part_2.ipynb                       Part 2: DataUSA API -> S3 JSON
|-- part_3.ipynb                       Part 3: analytics and reports
|-- part_3_with_outputs.ipynb          Part 3 executed, with results inline
|-- part_3_analysis_dashboard.pdf      Part 3 dashboard export
`-- part_4/
    |-- README.md                      Detailed infrastructure documentation
    |-- main.tf                        Terraform: Lambdas, IAM, SQS, S3 notification
    |-- terraform.tfvars               Non-secret config (host, job IDs)
    |-- src/
    |   `-- index.py                   Shared Lambda handler
    `-- databricks_jobs/
        |-- parts_1_and_2.yaml         Job: run Part 1 then Part 2
        `-- part_3.yaml                Job: run Part 3 then refresh dashboard
```

## Notes for reviewers

- **BLS 403 handling (Part 1):** the BLS site rejects programmatic requests that
  do not identify themselves. The sync sets a descriptive `User-Agent` header
  with a contact address to comply with the BLS data-access policy and fetch the
  files successfully.
- **Idempotent sync (Part 1):** the sync compares the source listing against
  what is already in S3 and only uploads new or changed files, and removes files
  deleted at the source, so it stays in step without re-uploading unchanged
  objects and without relying on hard-coded filenames.
- **Part 3 outputs:** `part_3.ipynb` is the clean notebook;
  `part_3_with_outputs.ipynb` shows the executed results, and
  `part_3_analysis_dashboard.pdf` is the dashboard the Part 3 job refreshes.
- **Part 4 details:** the `part_4/README.md` documents the Terraform resources,
  the least-privilege IAM design, how the Databricks token is kept out of source
  and state, and exactly what must exist on the Databricks side.
