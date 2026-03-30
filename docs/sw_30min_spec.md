# sw_30min: 30-Minute Space Weather Aggregation Table

## Overview

Aggregated space weather data at 30-minute cadence, combining OMNI solar wind parameters and HPo geomagnetic indices. Designed for deep learning feature extraction.

## Source Data

| Source | Table | Resolution | Variables |
|--------|-------|------------|-----------|
| OMNI | `omni_high_resolution` | 1-min | V, Np, T, Bx, By(GSM), Bz(GSM), Bt |
| HPo | `hpo_hp30` | 30-min | ap30, hp30 |

## Column Mapping

### OMNI Variables

Each OMNI variable is aggregated over a 30-minute window `[t, t+30min)` using 1-minute resolution data.

| Variable | Description | Source Column | Output Columns |
|----------|-------------|---------------|----------------|
| V | Solar wind speed (km/s) | `flow_speed_km_s` | `v_avg`, `v_min`, `v_max` |
| Np | Proton density (n/cc) | `proton_density_n_cc` | `np_avg`, `np_min`, `np_max` |
| T | Proton temperature (K) | `temperature_k` | `t_avg`, `t_min`, `t_max` |
| Bx | IMF Bx GSE/GSM (nT) | `bx_gse_nt` | `bx_avg`, `bx_min`, `bx_max` |
| By | IMF By GSM (nT) | `by_gsm_nt` | `by_avg`, `by_min`, `by_max` |
| Bz | IMF Bz GSM (nT) | `bz_gsm_nt` | `bz_avg`, `bz_min`, `bz_max` |
| Bt | IMF magnitude (nT) | `b_magnitude_nt` | `bt_avg`, `bt_min`, `bt_max` |

### HPo Variables

Directly matched from `hpo_hp30` table (already at 30-minute cadence).

| Variable | Description | Source Column |
|----------|-------------|---------------|
| ap30 | 30-min ap index | `ap30` |
| hp30 | 30-min Hp index | `hp30` |

## Aggregation Rules

- **Window**: `[t, t+30min)` — left-inclusive, right-exclusive
- **Method**: For each OMNI variable, compute `mean`, `min`, `max` from 1-minute data within the window
- **Example**: For `datetime = 2021-01-01 00:00:00`, aggregate OMNI 1-min data from `00:00:00` to `00:29:00` (up to 30 data points)

## Missing Data Handling

- If all 1-min values within a 30-min window are NaN for a variable → aggregated value is NaN
- If some 1-min values are NaN → compute aggregation using only valid values
- All 30-min timestamps are stored regardless of NaN presence (no row exclusion)
- NaN filtering is deferred to downstream deep learning dataset construction

## Table Schema

```
sw_30min (
    datetime    TIMESTAMP PRIMARY KEY,
    v_avg       REAL,
    v_min       REAL,
    v_max       REAL,
    np_avg      REAL,
    np_min      REAL,
    np_max      REAL,
    t_avg       REAL,
    t_min       REAL,
    t_max       REAL,
    bx_avg      REAL,
    bx_min      REAL,
    bx_max      REAL,
    by_avg      REAL,
    by_min      REAL,
    by_max      REAL,
    bz_avg      REAL,
    bz_min      REAL,
    bz_max      REAL,
    bt_avg      REAL,
    bt_min      REAL,
    bt_max      REAL,
    ap30        SMALLINT,
    hp30        REAL
)
```

**Total**: 24 columns (1 datetime + 21 OMNI aggregations + 2 HPo indices)

## Event Extraction

Extract data for a time window around a reference time T.

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| T | Reference time (datetime) | Required |
| B | Days before T | 5 |
| A | Days after T | 3 |

### Time Range

- **Start**: `T - B days`
- **End**: `T + A days - 30min`
- **Cadence**: 30 minutes (fixed)

### Example

```
T = 2021-01-10 00:00:00, B = 5, A = 3
Range: 2021-01-05 00:00:00 ~ 2021-01-12 23:30:00
Rows: (5 + 3) * 48 = 384
```

## CLI Usage

```bash
# Build aggregation table (year-by-year)
python scripts/build_sw_30min.py --build --start-year 2010 --end-year 2025

# Extract event data to CSV
python scripts/build_sw_30min.py --extract -t "2021-01-10 00:00:00" -b 5 -a 3 -o output.csv
```
