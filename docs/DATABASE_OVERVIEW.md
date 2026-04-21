# Database Overview / 데이터베이스 개요

High-level guide to the two PostgreSQL databases managed by `setup-sw-db`: what
each table stores, where the data originates, the empirical coverage after the
current backfill, and representative queries. For pure schema reference see
[TABLE_DESIGN.md](TABLE_DESIGN.md); for the 30-min aggregation rule see
[sw_30min_spec.md](sw_30min_spec.md).

`setup-sw-db` 가 관리하는 두 개의 PostgreSQL 데이터베이스에 대한 고수준 가이드.
각 테이블이 저장하는 내용, 원시 데이터 출처, 현재 백필 기준 실측 커버리지,
대표 쿼리를 다룬다. 순수 스키마 참조는
[TABLE_DESIGN.md](TABLE_DESIGN.md), 30 분 집계 규칙은
[sw_30min_spec.md](sw_30min_spec.md) 를 참조.

---

## 1. Architecture / 구조

Two databases are used, separating tabular time-series from image-file metadata.

표형 시계열 자료와 이미지 파일 메타데이터를 분리해 두 개의 DB 로 운영한다.

| Database | Purpose | Tables |
|----------|---------|--------|
| `space_weather` | Tabular time-series (solar wind, geomagnetic indices, GOES in-situ) | `omni_low_resolution`, `omni_high_resolution`, `omni_high_resolution_5min`, `hpo_hp30`, `hpo_hp60`, `goes_xrs`, `goes_mag`, `goes_proton`, `sw_30min` |
| `solar_images` | File-reference metadata for FITS imagery | `sdo`, `lasco`, `secchi` |

Connection credentials are supplied via the `DB_HOST` / `DB_USER` /
`DB_PASSWORD` environment variables. All timestamps are stored as naive UTC
`TIMESTAMP` columns by convention.

DB 접속 정보는 `DB_HOST` / `DB_USER` / `DB_PASSWORD` 환경변수로 제공된다.
모든 시간 컬럼은 관례적으로 UTC 기준의 timezone-naive `TIMESTAMP` 이다.

---

## 2. space_weather Database / space_weather 데이터베이스

### 2.1 OMNI (NASA SPDF)

Canonical solar-wind and geomagnetic indices merged at L1 by NASA SPDF. Three
tables at different cadences share the same raw source (`omni2_*.dat`,
`omni_min*.asc`, `omni_5min*.asc`).

NASA SPDF 에서 L1 기준으로 병합·보정한 태양풍/지자기 지수 표준 데이터셋.
해상도별로 3 개 테이블을 별도 저장한다.

| Table | Cadence | Columns | Notable fields |
|-------|---------|---------|-----------------|
| `omni_low_resolution` | 1 hour | 56 | `kp_index`, `dst_index_nt`, `ap_index_nt`, `f10_7_index_sfu`, proton fluxes |
| `omni_high_resolution` | 1 min | 47 | `flow_speed_km_s`, `bz_gsm_nt`, `sym_h_nt`, `ae_index_nt` |
| `omni_high_resolution_5min` | 5 min | 50 | Same as 1-min + `proton_flux_gt10/30/60mev` (embedded GOES proton) |

Per-column definitions in [TABLE_DESIGN.md](TABLE_DESIGN.md). All three use
`datetime` as a single-column primary key and are updated per-year (download
+ delete-by-year + bulk insert).

컬럼별 정의는 [TABLE_DESIGN.md](TABLE_DESIGN.md) 참조. 세 테이블 모두 `datetime`
단일 PK 이며, 연 단위로 다운로드 → delete-by-year → bulk insert 로 갱신된다.

### 2.2 HPo Geomagnetic Indices (GFZ Potsdam)

High-cadence successor to the 3-hour Kp index. Published by GFZ Potsdam since
1985, available at 30-min and 60-min resolutions.

3 시간 Kp 지수의 고해상도 후속 지수. 독일 GFZ Potsdam 에서 1985 년부터 공개하며
30 분·60 분 해상도로 제공된다.

| Table | Cadence | Key columns |
|-------|---------|-------------|
| `hpo_hp30` | 30 min | `hp30` (REAL), `ap30` (SMALLINT) |
| `hpo_hp60` | 60 min | `hp60` (REAL), `ap60` (SMALLINT) |

Source `-1` / `-1.000` sentinels are stored as SQL `NULL`. Two update modes
are available: `complete` (year-range bulk) and `nowcast` (last 30 days,
upsert).

소스의 결측 표기(`-1`, `-1.000`) 는 SQL `NULL` 로 저장한다. 두 가지 갱신 모드:
`complete` (연 단위 벌크) 와 `nowcast` (최근 30 일 upsert).

### 2.3 GOES In-Situ Observations (NOAA NCEI)

Geostationary-orbit in-situ measurements from the GOES satellite fleet.
Introduced in Phase 1 (2026-04). Three instruments, stored as per-instrument
unified tables keyed on `(satellite, datetime)` — multiple satellites coexist
at the same timestamp without conflict.

NOAA NCEI 의 GOES 정지궤도 직접관측 자료. Phase 1 (2026-04) 에 도입.
3 개 계측기를 계측기별 통합 테이블로 저장하며, PK 가 `(satellite, datetime)` 복합키라
같은 시각에 여러 위성 관측이 공존할 수 있다.

| Table | Instrument | Cadence | Generation coverage |
|-------|-----------|---------|---------------------|
| `goes_xrs` | X-Ray Sensor | 1 min | Legacy (13/14/15, 2-channel) + GOES-R (16+, 4 redundant channels) |
| `goes_mag` | Magnetometer | 1 min | GOES-R only (16/17/18/19); legacy stored as hi-res, not yet ingested |
| `goes_proton` | SGPS integral proton flux | 1 min | GOES-R only; NCEI L2 avg1m product starts 2020-11 |

Full source URLs and per-column schemas are in [TABLE_DESIGN.md](TABLE_DESIGN.md).

전체 소스 URL 과 컬럼별 스키마는 [TABLE_DESIGN.md](TABLE_DESIGN.md) 참조.

`goes_xrs` carries both the 2-channel primary columns (`xrs_a_flux_w_m2`,
`xrs_b_flux_w_m2`) and the 4 redundant GOES-R channels (`a1/a2/b1/b2`).
Queries wanting "the long-band X-ray flux" should always read
`xrs_b_flux_w_m2`; it is populated for every satellite in every era.

`goes_xrs` 는 공통 2 채널 컬럼과 GOES-R 의 redundant 4 채널을 함께 저장한다.
"장파장 X선 flux" 조회 시에는 항상 `xrs_b_flux_w_m2` 를 쓰면 된다 — 레거시·GOES-R
모든 시대에 채워져 있다.

For `goes_proton`, the schema threshold columns (>1/5/10/30/50/60/100 MeV)
are derived from the SGPS 13-channel differential flux by partial-channel
integration:

`goes_proton` 의 임계값 컬럼(>1/5/10/30/50/60/100 MeV) 은 SGPS 의 13 채널
차분 flux 를 임계값 위 에너지 구간만 부분 적분해 유도한다:

```
integral(>T) = Sum_c diff_flux[c] * max(0, upper[c] - max(T, lower[c]))
```

### 2.4 sw_30min Aggregation

30-min window aggregation of `omni_high_resolution` (avg/min/max for V, Np, T,
Bx, By, Bz, Bt) joined with `hpo_hp30`'s `ap30` and `hp30`. Consumed downstream
by `regression-sw` via per-event CSV exports.

`omni_high_resolution` 의 30 분 구간 집계(V, Np, T, Bx, By, Bz, Bt 각
avg/min/max) 와 `hpo_hp30` 의 `ap30`, `hp30` 를 조인한 테이블. 하류
`regression-sw` 가 이벤트별 CSV 로 읽어 사용한다.

GOES columns are intentionally **not** added to `sw_30min` in Phase 1 —
downstream code can JOIN `goes_xrs` / `goes_mag` / `goes_proton` on
`datetime` as needed.

Phase 1 에서는 `sw_30min` 에 GOES 컬럼을 **일부러** 추가하지 않았다. 하류에서
필요하면 `datetime` 키로 `goes_*` 를 직접 JOIN 해 사용하면 된다.

---

## 3. solar_images Database / solar_images 데이터베이스

Metadata rows referencing FITS files on a shared archive (`/opt/nas/archive/`).
Pixel data is never loaded into PostgreSQL; the database stores time, channel,
and `file_path` metadata so that consumers can match observations by timestamp.

공유 아카이브(`/opt/nas/archive/`) 상의 FITS 파일을 참조하는 메타데이터 테이블.
픽셀 데이터는 DB 에 적재하지 않고, `datetime`/`channel`/`file_path` 메타만
저장하여 타임스탬프로 매칭 가능하게 한다.

| Table | Source | PK columns |
|-------|--------|-----------|
| `sdo` | JSOC (AIA/HMI) | `telescope`, `channel`, `datetime` |
| `lasco` | NRL / VSO (SOHO coronagraph) | `camera`, `datetime` |
| `secchi` | NASA STEREO | `datatype`, `spacecraft`, `instrument`, `channel`, `datetime` |

Each table has `file_path` as an additional UNIQUE constraint and a `datetime`
index.

각 테이블에는 `file_path` UNIQUE 제약과 `datetime` 인덱스가 걸려 있다.

---

## 4. Empirical Coverage (as of 2026-04) / 실측 커버리지 (2026-04 기준)

Result of the Phase 1 GOES backfill and prior OMNI/HPo ingestion. Row counts
from `SELECT satellite, COUNT(*) FROM goes_xrs GROUP BY satellite` and
equivalents.

Phase 1 GOES 백필과 기존 OMNI/HPo 적재 이후의 상태.

### 4.1 GOES time-series coverage / GOES 시계열 커버리지

| Table | Rows | Start | End | Satellites |
|-------|------|-------|-----|-----------|
| `goes_xrs` | 17.8 M | 2010-01 | 2026-04 | 13, 14, 15, 16, 17, 18, 19 |
| `goes_mag` | 9.0 M | 2017-12 | 2026-04 | 16, 17, 18, 19 |
| `goes_proton` | 6.3 M | 2020-11 | 2026-04 | 16, 17, 18, 19 |

Data availability is bounded by what NOAA has reprocessed to the L2 avg1m
product, not by this project:

자료 가용 범위는 NOAA 의 L2 avg1m 재처리 여부에 의해 제한되며 본 프로젝트의
한계가 아니다:

- Legacy XRS science-quality reprocessing starts 2009-2013 per satellite.
- Legacy MAG is only published as `magn-l2-hires` (≥ 0.5 Hz), not as 1-min
  averages.
- SGPS L2 avg1m archive begins 2020-11 for GOES-R.

### 4.2 Continuous multi-satellite merge / 다위성 연속 커버리지

Because multiple satellites operate simultaneously, at least one XRS row
exists for every minute from 2010 onward. Downstream consumers can pick a
preferred satellite (e.g., GOES-East primary) or coalesce across satellites.

다수의 위성이 동시에 운용되므로 2010 년 이후 매 분마다 최소 1 개의 XRS 관측이
존재한다. 하류에서는 선호 위성(예: GOES-East primary) 을 지정하거나 여러
위성을 coalesce 할 수 있다.

---

## 5. Example Queries / 예시 쿼리

### 5.1 Best-available XRS long-band flux at each minute
각 분 단위 최선 XRS 장파장 flux 선택 (위성 간 coalesce)

```sql
SELECT datetime,
       COALESCE(
         MAX(xrs_b_flux_w_m2) FILTER (WHERE satellite = 16),
         MAX(xrs_b_flux_w_m2) FILTER (WHERE satellite = 18),
         MAX(xrs_b_flux_w_m2) FILTER (WHERE satellite = 15),
         MAX(xrs_b_flux_w_m2)
       ) AS xrs_b_flux_w_m2
FROM goes_xrs
WHERE datetime BETWEEN '2024-03-01' AND '2024-03-08'
GROUP BY datetime
ORDER BY datetime;
```

### 5.2 Join GOES features into sw_30min for ML training
ML 학습용으로 GOES 특징을 sw_30min 에 조인

```sql
SELECT s.datetime, s.v_avg, s.bz_avg, s.ap30,
       MAX(x.xrs_b_flux_w_m2)   AS xrs_b_max,
       AVG(p.proton_flux_gt10mev) AS proton_gt10_avg
FROM sw_30min s
LEFT JOIN goes_xrs    x ON x.datetime >= s.datetime
                        AND x.datetime < s.datetime + INTERVAL '30 min'
                        AND x.satellite = 16
LEFT JOIN goes_proton p ON p.datetime >= s.datetime
                        AND p.datetime < s.datetime + INTERVAL '30 min'
                        AND p.satellite = 16
WHERE s.datetime >= '2024-01-01'
GROUP BY s.datetime, s.v_avg, s.bz_avg, s.ap30
ORDER BY s.datetime;
```

### 5.3 Per-year coverage sanity check
연도별 커버리지 확인

```sql
SELECT date_trunc('year', datetime)::date AS year,
       COUNT(DISTINCT satellite) AS sats,
       COUNT(*)                  AS rows
FROM goes_xrs
GROUP BY 1
ORDER BY 1;
```

### 5.4 Identify GOES-R (4-channel) rows vs legacy
GOES-R 4 채널 데이터와 레거시 구분

```sql
SELECT is_goes_r,
       COUNT(*)                      AS rows,
       COUNT(xrs_a1_flux_w_m2)       AS has_a1,
       COUNT(xrs_b2_flux_w_m2)       AS has_b2
FROM goes_xrs
GROUP BY is_goes_r;
```

---

## 6. Known Gaps and Phase 2 / 알려진 결함 및 Phase 2

The current Phase 1 state leaves the following data gaps, all of which are
tracked for follow-up phases.

현재 Phase 1 상태에 남아 있는 자료 결함. 모두 후속 Phase 에서 다룰 예정.

| Gap | Cause | Phase |
|-----|-------|-------|
| Legacy GOES (13/14/15) MAG 1-min | NCEI only publishes hi-res (~0.5 Hz); needs downsampling | Phase 2 |
| Legacy GOES (13/14/15) proton flux | Monthly-aggregated EPEAD/HEPAD files; different schema | Phase 2 |
| GOES-R proton before 2020-11 | Not reprocessed to L2 avg1m by NOAA | Out of scope |
| GOES XRS >500 MeV integral | Schema has no `proton_flux_gt500mev` column; SGPS HEI available | Consider adding |
| SUVI EUV images, SXI X-ray images | File-reference storage only, in `solar_images` DB | Phase 3 |
| Near-real-time updates | SWPC JSON nowcast ingestion not implemented | Optional |

---

## 7. Reproducing Phase 1 / Phase 1 재현

If you drop and rebuild the GOES tables, the following command sequence
recreates the current state on a server with access to NCEI and the archive
mount (`/opt/nas/archive/space_weather/goes/`).

NCEI 및 `/opt/nas/archive/space_weather/goes/` 아카이브 마운트가 가능한 서버에서
다음 명령으로 현재 상태를 재현할 수 있다.

```bash
./swdb db init                  # create tables (idempotent)

# XRS — 7 satellites, full history
./swdb download goes --instrument xrs \
    --satellites 13 14 15 16 17 18 19 \
    --start-date 2010-01-01 --end-date 2026-04-17 --parallel 8
./swdb register goes --instrument xrs --satellites 13 14 15 16 17 18 19

# MAG — GOES-R only
./swdb download goes --instrument mag --satellites 16 17 18 19 \
    --start-date 2017-12-01 --end-date 2026-04-17 --parallel 8
./swdb register goes --instrument mag --satellites 16 17 18 19

# Proton — GOES-R only; effective start 2020-11
./swdb download goes --instrument proton --satellites 16 17 18 19 \
    --start-date 2020-11-01 --end-date 2026-04-17 --parallel 8
./swdb register goes --instrument proton --satellites 16 17 18 19

./swdb db status                # verify row counts and date ranges
```

The `register` step is idempotent — it uses `INSERT ... ON CONFLICT DO NOTHING`
on `(satellite, datetime)`, so re-runs on an already-populated archive are
safe.

`register` 단계는 `(satellite, datetime)` 에 대한 `INSERT ... ON CONFLICT DO
NOTHING` upsert 를 사용하므로, 이미 적재된 상태에서 재실행해도 안전하다.
