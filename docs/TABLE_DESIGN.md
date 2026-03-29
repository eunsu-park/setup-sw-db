# Table Design

Database schema reference for all tables managed by this project.

---

## Databases

| Database | Purpose | Tables |
|----------|---------|--------|
| `solar_images` | Solar observation FITS image metadata | `sdo`, `lasco`, `secchi` |
| `space_weather` | Space weather time series indices | `omni_low_resolution`, `omni_high_resolution`, `omni_high_resolution_5min`, `hpo_hp30`, `hpo_hp60` |

---

## solar_images Database

### `sdo` - Solar Dynamics Observatory (AIA/HMI)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| telescope | VARCHAR(10) | NOT NULL | Telescope name (`aia` or `hmi`) |
| channel | VARCHAR(20) | NOT NULL | Channel/wavelength identifier |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| file_path | VARCHAR(512) | NOT NULL | Absolute path to FITS file |
| quality | INTEGER | NULL | Data quality flag (0 = best) |
| wavelength | INTEGER | NULL | Wavelength in Angstrom |
| exposure_time | REAL | NULL | Exposure time in seconds |

- **Primary Key**: `(telescope, channel, datetime)`
- **Unique**: `file_path`
- **Indexes**: `datetime`, `telescope`, `quality`

### `lasco` - SOHO Coronagraph

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| camera | VARCHAR(4) | NOT NULL | Camera identifier (`c1`, `c2`, `c3`, `c4`) |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| file_path | VARCHAR(512) | NOT NULL | Absolute path to FITS file |
| exposure_time | REAL | NULL | Exposure time in seconds |
| filter | VARCHAR(20) | NULL | Optical filter used |

- **Primary Key**: `(camera, datetime)`
- **Unique**: `file_path`
- **Indexes**: `datetime`

### `secchi` - STEREO/SECCHI

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| datatype | VARCHAR(10) | NOT NULL | `science` or `beacon` |
| spacecraft | VARCHAR(10) | NOT NULL | `ahead` or `behind` |
| instrument | VARCHAR(10) | NOT NULL | `cor1`, `cor2`, `euvi`, `hi_1`, `hi_2` |
| channel | VARCHAR(20) | NULL | Wavelength for EUVI, NULL for others |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| file_path | VARCHAR(512) | NOT NULL | Absolute path to FITS file |
| exposure_time | REAL | NULL | Exposure time in seconds |
| filter | VARCHAR(20) | NULL | Optical filter used |
| wavelength | INTEGER | NULL | Wavelength in Angstrom |

- **Primary Key**: `(datatype, spacecraft, instrument, channel, datetime)`
- **Unique**: `file_path`
- **Indexes**: `datetime`, `spacecraft`, `instrument`

---

## space_weather Database

### `omni_low_resolution` - OMNI Hourly Data

55 columns. Solar wind parameters, IMF, and geomagnetic indices at 1-hour resolution.

| Column Group | Columns | Description |
|-------------|---------|-------------|
| Time (4) | `datetime` (PK), `year`, `decimal_day`, `hour` | Observation time |
| Metadata (5) | `bartels_rotation_number`, `imf_sc_id`, `sw_plasma_sc_id`, `imf_avg_points`, `plasma_avg_points` | Source spacecraft and averaging |
| Magnetic Field (14) | `b_field_magnitude_avg_nt`, `bx_gse_gsm_nt`, `by_gse_nt`, `bz_gse_nt`, `by_gsm_nt`, `bz_gsm_nt`, ... | IMF components in GSE/GSM (nT) |
| Plasma (14) | `proton_temperature_k`, `proton_density_n_cm3`, `plasma_flow_speed_km_s`, `flow_pressure_npa`, ... | Solar wind plasma parameters |
| Indices (10) | `kp_index`, `dst_index_nt`, `ae_index_nt`, `ap_index_nt`, `sunspot_number_r`, proton fluxes | Geomagnetic and solar activity indices |
| Other (8) | `f10_7_index_sfu`, `pc_n_index`, `al_index_nt`, `au_index_nt`, `magnetosonic_mach_number`, ... | Additional indices |

- **Primary Key**: `datetime`
- **Source**: NASA SPDF (`https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/`)
- **Update**: Per-year file replacement

### `omni_high_resolution` - OMNI 1-Minute Data

46 columns. High-cadence solar wind and geomagnetic indices.

| Column Group | Columns | Description |
|-------------|---------|-------------|
| Time (5) | `datetime` (PK), `year`, `day`, `hour`, `minute` | Observation time |
| Metadata (9) | `imf_sc_id`, `sw_plasma_sc_id`, `percent_interp`, `timeshift_sec`, ... | Source spacecraft, time shift, and quality |
| Magnetic Field (8) | `b_magnitude_nt`, `bx_gse_nt`, `by_gse_nt`, `bz_gse_nt`, `by_gsm_nt`, `bz_gsm_nt`, ... | IMF components (nT) |
| Plasma (10) | `flow_speed_km_s`, `vx_gse_km_s`, `proton_density_n_cc`, `temperature_k`, ... | Solar wind parameters |
| Position (6) | `sc_x_gse_re`, `sc_y_gse_re`, `sc_z_gse_re`, `bsn_x_gse_re`, ... | Spacecraft and bow shock nose position (Re) |
| Indices (9) | `ae_index_nt`, `al_index_nt`, `au_index_nt`, `sym_d_nt`, `sym_h_nt`, `asy_d_nt`, `asy_h_nt`, ... | Geomagnetic indices |

- **Primary Key**: `datetime`
- **Source**: NASA SPDF (`https://spdf.gsfc.nasa.gov/pub/data/omni/high_res_omni/`)
- **Update**: Per-year file replacement

### `omni_high_resolution_5min` - OMNI 5-Minute Data

49 columns. Same as 1-minute + 3 proton flux columns.

- Same structure as `omni_high_resolution` plus:
  - `proton_flux_gt10mev`, `proton_flux_gt30mev`, `proton_flux_gt60mev` (GOES proton flux)
- **Primary Key**: `datetime`

### `hpo_hp30` - HPo Hp30 Geomagnetic Index (30-min)

High-resolution geomagnetic activity index from GFZ Potsdam. Successor to the 3-hour Kp index with 30-minute time resolution. Data available since 1985.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| datetime | TIMESTAMP | NOT NULL | Interval start time (UTC) |
| year | SMALLINT | NOT NULL | Year |
| month | SMALLINT | NOT NULL | Month |
| day | SMALLINT | NOT NULL | Day |
| hh_start | REAL | NOT NULL | Starting hour as decimal (e.g., 0.0, 0.5, 1.0, ..., 23.5) |
| hp30 | REAL | NULL | Hp30 index value (0 to 9+, unitless). NULL = missing data |
| ap30 | SMALLINT | NULL | Equivalent amplitude ap30 (0 to 400, unitless). NULL = missing data |

- **Primary Key**: `datetime`
- **Indexes**: `year`
- **Records per day**: 48 (every 30 minutes)
- **Missing values**: Source uses `-1.000` (Hp30) and `-1` (ap30), stored as NULL
- **Source**: GFZ Potsdam (`https://kp.gfz.de/en/hp30-hp60/data`)
- **Update modes**:
  - `complete`: Full series (1985-present), truncate + bulk insert
  - `nowcast`: Last 30 days, upsert (skip duplicates)
- **Reference**: Yamazaki et al. (2024), DOI: `10.5880/Hpo.0003`

### `hpo_hp60` - HPo Hp60 Geomagnetic Index (60-min)

Same as Hp30 but at 60-minute resolution.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| datetime | TIMESTAMP | NOT NULL | Interval start time (UTC) |
| year | SMALLINT | NOT NULL | Year |
| month | SMALLINT | NOT NULL | Month |
| day | SMALLINT | NOT NULL | Day |
| hh_start | REAL | NOT NULL | Starting hour as integer (0.0, 1.0, ..., 23.0) |
| hp60 | REAL | NULL | Hp60 index value (0 to 9+, unitless). NULL = missing data |
| ap60 | SMALLINT | NULL | Equivalent amplitude ap60 (0 to 400, unitless). NULL = missing data |

- **Primary Key**: `datetime`
- **Indexes**: `year`
- **Records per day**: 24 (every 60 minutes)
- **Missing values**: Source uses `-1.000` (Hp60) and `-1` (ap60), stored as NULL
- **Source**: GFZ Potsdam (`https://kp.gfz.de/en/hp30-hp60/data`)
- **Update modes**: Same as `hpo_hp30`

---

## Design Notes

### Column Choices for HPo Tables

The source data contains 10 columns per record. The following were intentionally excluded:

| Excluded Column | Reason |
|----------------|--------|
| `hh_mid` (mid-time hours) | Computable: `hh_start + resolution / 2` |
| `days_start` (days since 1932-01-01) | Redundant with `datetime` |
| `days_mid` (days since 1932-01-01 to midpoint) | Redundant with `datetime` + resolution |
| `D` (reserved flag) | Always 0 in current data, reserved for future use |

### Type Choices

- `SMALLINT` for year/month/day/ap values: range fits within -32768 to 32767, saves storage vs `INTEGER`
- `REAL` for Hp/hh_start: 4-byte float, sufficient precision for index values (3 decimal places)
- `TIMESTAMP` without timezone: all times are UTC by convention

### Primary Key Strategy

- **Solar images**: Composite keys (`telescope + channel + datetime`) because multiple instruments observe simultaneously
- **Space weather indices**: Single `datetime` key per table, since each table stores one index type at one resolution
