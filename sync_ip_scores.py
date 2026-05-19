"""
sync_ip_scores.py
=================
Reads Drug_Name, IP_Dimension_1_Score, Rationale, updated_at
from `cognito-prod-394707.cognito_prod_datamart.Master_LOE`

and upserts into `cognito-prod-394707.cognito_prod_datamart.dim_scores`:
    product   = Drug_Name
    score     = IP_Dimension_1_Score
    rationale = Rationale
    timestamp = updated_at
    pillar    = 'IP'
    dimension = 'Primary Market Entry Horizon'

Usage:
    python sync_ip_scores.py

Prerequisites (.env):
    GCS_CREDENTIALS=/path/to/service-account.json
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(override=True)

# ── Config ────────────────────────────────────────────────────────────────────
BQ_PROJECT  = os.getenv("BQ_PROJECT_ID",  "cognito-prod-394707")
BQ_DATASET  = os.getenv("BQ_DATASET_ID",  "cognito_prod_datamart")
CREDENTIALS = os.getenv("GCS_CREDENTIALS", "")

SOURCE_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.Master_LOE"
TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.dim_scores"

PILLAR    = "IP"
DIMENSION = "Primary Market Entry Horizon"

# ── Validation ────────────────────────────────────────────────────────────────
def _validate():
    if not CREDENTIALS:
        sys.exit(
            "ERROR: GCS_CREDENTIALS is not set.\n"
            "Add it to your .env file:\n"
            "  GCS_CREDENTIALS=/path/to/service-account.json"
        )
    if not os.path.exists(CREDENTIALS):
        sys.exit(f"ERROR: Service-account file not found: {CREDENTIALS}")


# ── BQ Client ─────────────────────────────────────────────────────────────────
def _get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=creds)


# ── Read Source ───────────────────────────────────────────────────────────────
def read_master_loe(client) -> list[dict]:
    query = f"""
        SELECT
            Drug_Name,
            IP_Dimension_1_Score,
            Rationale,
            updated_at
        FROM `{SOURCE_TABLE}`
        WHERE Drug_Name IS NOT NULL
    """
    print(f"  Reading from: {SOURCE_TABLE}")
    rows = list(client.query(query).result())
    print(f"  Rows fetched : {len(rows)}")
    return rows


# ── Upsert to dim_scores ──────────────────────────────────────────────────────
def upsert_dim_scores(client, rows: list[dict]):
    from google.cloud import bigquery

    if not rows:
        print("  [SKIP] No rows to upsert.")
        return

    # Build MERGE SQL — upsert on (product, pillar, dimension)
    merge_sql = f"""
        MERGE `{TARGET_TABLE}` AS T
        USING (
            SELECT
                CAST(drug_name   AS STRING)  AS product,
                CAST(score       AS FLOAT64) AS score,
                CAST(rationale   AS STRING)  AS rationale,
                CAST(ts          AS TIMESTAMP) AS timestamp,
                '{PILLAR}'                   AS pillar,
                '{DIMENSION}'               AS dimension
            FROM UNNEST(@records) AS r(drug_name, score, rationale, ts)
        ) AS S
        ON  T.product   = S.product
        AND T.pillar    = S.pillar
        AND T.dimension = S.dimension
        WHEN MATCHED THEN
            UPDATE SET
                T.score     = S.score,
                T.rationale = S.rationale,
                T.timestamp = S.timestamp
        WHEN NOT MATCHED THEN
            INSERT (product, score, rationale, timestamp, pillar, dimension)
            VALUES (S.product, S.score, S.rationale, S.timestamp, S.pillar, S.dimension)
    """

    # Prepare records as STRUCT array for parameterised query
    records = []
    for row in rows:
        records.append({
            "drug_name": str(row["Drug_Name"]),
            "score":     float(row["IP_Dimension_1_Score"]) if row["IP_Dimension_1_Score"] is not None else None,
            "rationale": str(row["Rationale"]) if row["Rationale"] is not None else None,
            "ts":        row["updated_at"].isoformat() if row["updated_at"] is not None else datetime.utcnow().isoformat(),
        })

    struct_type = bigquery.StructQueryParameterType(
        bigquery.ScalarQueryParameterType("STRING",    name="drug_name"),
        bigquery.ScalarQueryParameterType("FLOAT64",   name="score"),
        bigquery.ScalarQueryParameterType("STRING",    name="rationale"),
        bigquery.ScalarQueryParameterType("TIMESTAMP", name="ts"),
    )
    param = bigquery.ArrayQueryParameter("records", struct_type, records)
    job_config = bigquery.QueryJobConfig(query_parameters=[param])

    print(f"  Upserting {len(records)} row(s) into: {TARGET_TABLE}")
    client.query(merge_sql, job_config=job_config).result()
    print(f"  Upsert complete.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  IP SCORE SYNC  —  Master_LOE → dim_scores")
    print("=" * 60)
    print(f"  Source : {SOURCE_TABLE}")
    print(f"  Target : {TARGET_TABLE}")
    print(f"  Pillar : {PILLAR}")
    print(f"  Dim    : {DIMENSION}")
    print("=" * 60)

    _validate()
    client = _get_bq_client()

    rows = read_master_loe(client)

    if not rows:
        print("  [WARN] No data found in Master_LOE. Exiting.")
        return

    # Print preview
    print(f"\n  Preview (first 5 rows):")
    for r in rows[:5]:
        print(f"    {r['Drug_Name']:30s}  score={r['IP_Dimension_1_Score']}  updated={r['updated_at']}")

    upsert_dim_scores(client, rows)

    print("\n" + "=" * 60)
    print(f"  Done. {len(rows)} drug(s) synced.")
    print("=" * 60)


if __name__ == "__main__":
    main()
