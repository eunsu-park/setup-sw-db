# Table Design

Database schema reference for all tables managed by this project.

---

## Databases

| Database | Purpose | Tables |
|----------|---------|--------|
| `solar_images` | Solar observation FITS image metadata | `sdo`, `lasco`, `secchi` |
| `space_weather` | Space weather time series indices | `omni_low_resolution`, `omni_high_resolution`, `omni_high_resolution_5min`, `hpo_hp30`, `hpo_hp60`, `goes_xrs`, `goes_mag`, `goes_proton`, `sw_30min` |

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

56 columns. Solar wind parameters, IMF, and geomagnetic indices at 1-hour
resolution, merged at L1 from IMP-8 / Wind / Geotail / ACE. See
[OMNI hourly documentation](https://omniweb.gsfc.nasa.gov/html/ow_data.html)
for source definitions.

#### Time (4)

| Column | Type | Description |
|--------|------|-------------|
| `datetime` | TIMESTAMP (PK) | Observation timestamp, UTC (hour boundary) |
| `year` | INTEGER | Calendar year |
| `decimal_day` | INTEGER | Day of year (1-366) |
| `hour` | INTEGER | Hour of day (0-23) |

#### Metadata (5)

| Column | Type | Description |
|--------|------|-------------|
| `bartels_rotation_number` | INTEGER | 27-day solar rotation cycle number |
| `imf_sc_id` | SMALLINT | Source spacecraft code for IMF data (50=IMP-8, 51=Wind, 60=Geotail, 71=ACE) |
| `sw_plasma_sc_id` | SMALLINT | Source spacecraft code for plasma data |
| `imf_avg_points` | INTEGER | Count of fine-scale samples in IMF hourly average |
| `plasma_avg_points` | INTEGER | Count of fine-scale samples in plasma hourly average |

#### Magnetic Field (14, nT)

| Column | Type | Description |
|--------|------|-------------|
| `b_field_magnitude_avg_nt` | REAL | Scalar average \|B\| over the hour (`<F>`) |
| `b_magnitude_of_avg_field_vector_nt` | REAL | Magnitude of vector-averaged B (`\|<B>\|`); always ≤ the scalar average |
| `b_lat_angle_avg_field_vector_deg` | REAL | GSE latitude angle of averaged field vector (deg) |
| `b_long_angle_avg_field_vector_deg` | REAL | GSE longitude angle of averaged field vector (deg) |
| `bx_gse_gsm_nt` | REAL | Bx component (GSE ≡ GSM by definition for x-axis) |
| `by_gse_nt` | REAL | By component, GSE |
| `bz_gse_nt` | REAL | Bz component, GSE |
| `by_gsm_nt` | REAL | By component, GSM |
| `bz_gsm_nt` | REAL | Bz component, GSM — primary geoeffective component |
| `sigma_b_magnitude_nt` | REAL | RMS SD of \|B\| over the hour |
| `sigma_b_vector_nt` | REAL | RMS SD of field vector magnitude |
| `sigma_bx_nt` | REAL | RMS SD of Bx (GSE) |
| `sigma_by_nt` | REAL | RMS SD of By (GSE) |
| `sigma_bz_nt` | REAL | RMS SD of Bz (GSE) |

#### Plasma (16)

| Column | Type | Description |
|--------|------|-------------|
| `proton_temperature_k` | REAL | Proton (bulk ion) temperature (K) |
| `proton_density_n_cm3` | REAL | Proton number density (cm⁻³) |
| `plasma_flow_speed_km_s` | REAL | Scalar solar-wind bulk speed (km/s) |
| `plasma_flow_long_angle_deg` | REAL | Azimuthal flow angle φ_V (deg) |
| `plasma_flow_lat_angle_deg` | REAL | Latitudinal flow angle θ_V (deg) |
| `na_np_ratio` | REAL | Alpha-to-proton density ratio (dimensionless) |
| `flow_pressure_npa` | REAL | Dynamic pressure including alphas (nPa) |
| `sigma_temperature_k` | REAL | RMS SD of T |
| `sigma_density_n_cm3` | REAL | RMS SD of Np |
| `sigma_flow_speed_km_s` | REAL | RMS SD of V |
| `sigma_phi_v_deg` | REAL | RMS SD of φ_V |
| `sigma_theta_v_deg` | REAL | RMS SD of θ_V |
| `sigma_na_np` | REAL | RMS SD of alpha ratio |
| `electric_field_mv_m` | REAL | Solar-wind electric field = −V × Bz(GSM) × 10⁻³ (mV/m) |
| `plasma_beta` | REAL | Plasma β = thermal pressure / magnetic pressure |
| `alfven_mach_number` | REAL | V / V_Alfvén |

#### Geomagnetic & Solar-Activity Indices (17)

| Column | Type | Description |
|--------|------|-------------|
| `kp_index` | SMALLINT | Kp × 10 (GFZ Potsdam), 3-hour planetary magnetic activity |
| `sunspot_number_r` | INTEGER | Daily sunspot number R (v2) |
| `dst_index_nt` | INTEGER | Dst (Kyoto WDC), ring-current disturbance (nT) |
| `ae_index_nt` | INTEGER | AE (Kyoto), auroral electrojet envelope (nT) |
| `proton_flux_gt1mev` | REAL | Energetic proton integral flux >1 MeV (cm² s sr)⁻¹ |
| `proton_flux_gt2mev` | REAL | Integral flux >2 MeV |
| `proton_flux_gt4mev` | REAL | Integral flux >4 MeV |
| `proton_flux_gt10mev` | REAL | Integral flux >10 MeV |
| `proton_flux_gt30mev` | REAL | Integral flux >30 MeV |
| `proton_flux_gt60mev` | REAL | Integral flux >60 MeV |
| `flag` | SMALLINT | Magnetospheric contamination flag for proton fluxes (0-6; −1 = missing) |
| `ap_index_nt` | INTEGER | ap (GFZ), equivalent 3-hour amplitude (nT) |
| `f10_7_index_sfu` | REAL | F10.7 cm solar radio flux at 1 AU, SFU = 10⁻²² W/m²/Hz |
| `pc_n_index` | REAL | Polar Cap North index (Thule/Qaanaaq, DTU) |
| `al_index_nt` | INTEGER | AL (Kyoto), auroral lower envelope (nT) |
| `au_index_nt` | INTEGER | AU (Kyoto), auroral upper envelope (nT) |
| `magnetosonic_mach_number` | REAL | V / V_magnetosonic |

- **Primary Key**: `datetime`
- **Source**: NASA SPDF (`https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2_{year}.dat`)
- **Update**: Per-year file replacement (download → delete-by-year → bulk insert)

### `omni_high_resolution` - OMNI 1-Minute Data

47 columns. High-cadence solar-wind and geomagnetic indices, time-shifted from
L1 to the bow shock nose. See
[OMNI HRO documentation](https://omniweb.gsfc.nasa.gov/html/HROdocum.html).

#### Time (5)

| Column | Type | Description |
|--------|------|-------------|
| `datetime` | TIMESTAMP (PK) | Observation timestamp at start of minute (UTC, post-shift) |
| `year` | INTEGER | Calendar year |
| `day` | INTEGER | Day of year (1-366) |
| `hour` | INTEGER | Hour (0-23) |
| `minute` | INTEGER | Minute at start of average (0-59) |

#### Metadata & Time-Shift Quality (9)

| Column | Type | Description |
|--------|------|-------------|
| `imf_sc_id` | SMALLINT | Source spacecraft code for IMF data |
| `sw_plasma_sc_id` | SMALLINT | Source spacecraft code for plasma data |
| `imf_avg_points` | INTEGER | Count of IMF observations in minute average |
| `plasma_avg_points` | INTEGER | Count of plasma observations in minute average |
| `percent_interp` | INTEGER | % (0-100) of IMF points whose phase front normal (PFN) was interpolated — higher = lower quality |
| `timeshift_sec` | INTEGER | Delay applied to propagate L1 observation to bow shock nose (s) |
| `rms_timeshift` | INTEGER | RMS SD of timeshift across component observations (s) |
| `rms_phase_front_normal` | REAL | RMS SD of minute-averaged PFN direction |
| `time_between_obs_sec` | INTEGER | Duration between successive observations (DBOT1); negative = out-of-sequence arrival |

#### Magnetic Field (8, nT)

| Column | Type | Description |
|--------|------|-------------|
| `b_magnitude_nt` | REAL | Scalar \|B\| averaged over the minute |
| `bx_gse_nt` | REAL | Bx, GSE |
| `by_gse_nt` | REAL | By, GSE |
| `bz_gse_nt` | REAL | Bz, GSE |
| `by_gsm_nt` | REAL | By, GSM |
| `bz_gsm_nt` | REAL | Bz, GSM — primary geoeffective component |
| `rms_sd_b_scalar_nt` | REAL | RMS SD of \|B\| |
| `rms_sd_b_vector_nt` | REAL | RMS SD of B vector components (sqrt-of-sum-of-squares) |

#### Plasma (10)

| Column | Type | Description |
|--------|------|-------------|
| `flow_speed_km_s` | REAL | Solar-wind bulk speed (km/s) |
| `vx_gse_km_s` | REAL | Vx, GSE (km/s) |
| `vy_gse_km_s` | REAL | Vy, GSE (km/s) |
| `vz_gse_km_s` | REAL | Vz, GSE (km/s) |
| `proton_density_n_cc` | REAL | Proton number density (cm⁻³) |
| `temperature_k` | REAL | Proton temperature (K) |
| `flow_pressure_npa` | REAL | Dynamic pressure ≈ (2×10⁻⁶) × Np × V² (nPa) |
| `electric_field_mv_m` | REAL | E = −V × Bz(GSM) × 10⁻³ (mV/m) |
| `plasma_beta` | REAL | Plasma β |
| `alfven_mach_number` | REAL | (V × √Np) / (20 × \|B\|) |

#### Position (6, Earth radii in GSE)

| Column | Type | Description |
|--------|------|-------------|
| `sc_x_gse_re` | REAL | Spacecraft X |
| `sc_y_gse_re` | REAL | Spacecraft Y |
| `sc_z_gse_re` | REAL | Spacecraft Z |
| `bsn_x_gse_re` | REAL | Bow shock nose X — target of the time shift |
| `bsn_y_gse_re` | REAL | Bow shock nose Y |
| `bsn_z_gse_re` | REAL | Bow shock nose Z |

#### Geomagnetic Indices (9)

| Column | Type | Description |
|--------|------|-------------|
| `ae_index_nt` | INTEGER | AE (Kyoto), 1-min auroral electrojet envelope (nT) |
| `al_index_nt` | INTEGER | AL (Kyoto), auroral lower envelope (nT) |
| `au_index_nt` | INTEGER | AU (Kyoto), auroral upper envelope (nT) |
| `sym_d_nt` | INTEGER | SYM/D — symmetric disturbance, east-west component (nT) |
| `sym_h_nt` | INTEGER | SYM/H — symmetric disturbance, Dst-like hourly component at 1-min cadence (nT) |
| `asy_d_nt` | INTEGER | ASY/D — asymmetric disturbance, east-west (nT) |
| `asy_h_nt` | INTEGER | ASY/H — asymmetric disturbance, north-south (nT) |
| `pc_n_index` | REAL | Polar Cap North index |
| `magnetosonic_mach_number` | REAL | V / V_magnetosonic |

- **Primary Key**: `datetime`
- **Source**: NASA SPDF (`https://spdf.gsfc.nasa.gov/pub/data/omni/high_res_omni/omni_min{year}.asc`)
- **Update**: Per-year file replacement

### `omni_high_resolution_5min` - OMNI 5-Minute Data

50 columns. Identical schema to `omni_high_resolution` plus three GOES-derived
proton flux columns appended at the end:

| Column | Type | Description |
|--------|------|-------------|
| `proton_flux_gt10mev` | REAL | GOES proton integral flux >10 MeV (cm² s sr)⁻¹ |
| `proton_flux_gt30mev` | REAL | GOES proton integral flux >30 MeV |
| `proton_flux_gt60mev` | REAL | GOES proton integral flux >60 MeV |

- **Primary Key**: `datetime`
- **Source**: NASA SPDF (`https://spdf.gsfc.nasa.gov/pub/data/omni/high_res_omni/omni_5min{year}.asc`)
- **Update**: Per-year file replacement

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

### `goes_xrs` - GOES X-Ray Sensor (1-min)

1-minute averaged solar X-ray irradiance. Unified across legacy (GOES-13/14/15,
2-channel XRS) and GOES-R (16+, 4-channel redundant XRS).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| satellite | SMALLINT | NOT NULL | Satellite number (13..19) |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| xrs_a_flux_w_m2 | REAL | NULL | Short band flux, 0.5-4 A (W/m^2). Filled for all satellites |
| xrs_b_flux_w_m2 | REAL | NULL | Long band flux, 1-8 A (W/m^2). Primary flare band |
| xrs_a1_flux_w_m2 | REAL | NULL | GOES-R redundant channel A1 |
| xrs_a2_flux_w_m2 | REAL | NULL | GOES-R redundant channel A2 |
| xrs_b1_flux_w_m2 | REAL | NULL | GOES-R redundant channel B1 |
| xrs_b2_flux_w_m2 | REAL | NULL | GOES-R redundant channel B2 |
| xrs_a_flag | SMALLINT | NULL | Short band quality flag |
| xrs_b_flag | SMALLINT | NULL | Long band quality flag |
| is_goes_r | BOOLEAN | NOT NULL | True for GOES-16+, False for legacy |

- **Primary Key**: `(satellite, datetime)`
- **Indexes**: `datetime`, `satellite`
- **Source (GOES-R)**: `data.ngdc.noaa.gov/.../goes{NN}/l2/data/xrsf-l2-avg1m/{YYYY}/{MM}/`
- **Source (legacy)**: `www.ncei.noaa.gov/.../science/xrs/goes{NN}/xrsf-l2-avg1m_science/{YYYY}/{MM}/`
- **Query convention**: always read `xrs_b_flux_w_m2` for the long-band flux; the
  column is populated for every satellite and every generation.

### `goes_mag` - GOES Magnetometer (1-min)

1-minute averaged geomagnetic field at geostationary orbit. GOES-R series only
in current scope (Phase 1). Legacy `magn-l2-hires` exists but would require
downsampling — deferred to Phase 2.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| satellite | SMALLINT | NOT NULL | Satellite number (16..19) |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| bx_nt | REAL | NULL | B-field X component (nT) |
| by_nt | REAL | NULL | B-field Y component (nT) |
| bz_nt | REAL | NULL | B-field Z component (nT) |
| bt_nt | REAL | NULL | Total field magnitude (nT) |
| coord_frame | VARCHAR(8) | NULL | Frame tag: `EPN`, `GSE`, `GSM`, `VDH` (native L2 frame, no rotation) |
| mag_flag | INTEGER | NULL | Data-quality bitmask (GOES-R DQF can be wider than 16 bits) |

- **Primary Key**: `(satellite, datetime)`
- **Indexes**: `datetime`, `satellite`
- **Source**: `data.ngdc.noaa.gov/.../goes{NN}/l2/data/magn-l2-avg1m/{YYYY}/{MM}/`

### `goes_proton` - GOES Integral Proton Flux (1-min)

1-minute integral proton flux derived from SGPS differential channels via
partial-channel integration. GOES-R series only (SGPS L2 avg1m is not archived
before 2020-11).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| satellite | SMALLINT | NOT NULL | Satellite number (16..19) |
| datetime | TIMESTAMP | NOT NULL | Observation time (UTC) |
| proton_flux_gt1mev | REAL | NULL | Integral flux > 1 MeV (pfu = p / (cm^2 sr s)) |
| proton_flux_gt5mev | REAL | NULL | Integral flux > 5 MeV |
| proton_flux_gt10mev | REAL | NULL | Integral flux > 10 MeV (SWPC SEP threshold channel) |
| proton_flux_gt30mev | REAL | NULL | Integral flux > 30 MeV |
| proton_flux_gt50mev | REAL | NULL | Integral flux > 50 MeV |
| proton_flux_gt60mev | REAL | NULL | Integral flux > 60 MeV |
| proton_flux_gt100mev | REAL | NULL | Integral flux > 100 MeV |
| proton_flag | SMALLINT | NULL | Data-quality summary (e.g., `IntDQFerrSum`) |

- **Primary Key**: `(satellite, datetime)`
- **Indexes**: `datetime`, `satellite`
- **Source**: `data.ngdc.noaa.gov/.../goes{NN}/l2/data/sgps-l2-avg1m/{YYYY}/{MM}/`
- **Derivation**: Sensor-averaged differential flux is integrated per-channel
  using `integral(>T) = Sum_c diff_flux[c] * max(0, upper[c] - max(T, lower[c]))`.
  Channels above a threshold contribute fully; a channel spanning the threshold
  contributes only its upper portion. Channels above all available energy bins
  leave the column NaN.

### `sw_30min` - 30-min Aggregation (OMNI + HPo)

24 columns. 30-minute aggregation of `omni_high_resolution` (7 solar-wind
variables × 3 statistics) joined with `hpo_hp30`. Consumed downstream by
`regression-sw` via per-event CSV exports. See `sw_30min_spec.md` for
aggregation rules and window boundary conventions.

| Column(s) | Type | Description | Source column |
|-----------|------|-------------|---------------|
| `datetime` | TIMESTAMP (PK) | 30-min window start (UTC) | derived |
| `v_avg`, `v_min`, `v_max` | REAL | Solar-wind bulk speed (km/s) | `omni_high_resolution.flow_speed_km_s` |
| `np_avg`, `np_min`, `np_max` | REAL | Proton number density (cm⁻³) | `omni_high_resolution.proton_density_n_cc` |
| `t_avg`, `t_min`, `t_max` | REAL | Proton temperature (K) | `omni_high_resolution.temperature_k` |
| `bx_avg`, `bx_min`, `bx_max` | REAL | IMF Bx, GSE (nT) | `omni_high_resolution.bx_gse_nt` |
| `by_avg`, `by_min`, `by_max` | REAL | IMF By, GSM (nT) | `omni_high_resolution.by_gsm_nt` |
| `bz_avg`, `bz_min`, `bz_max` | REAL | IMF Bz, GSM (nT) — primary geoeffective | `omni_high_resolution.bz_gsm_nt` |
| `bt_avg`, `bt_min`, `bt_max` | REAL | IMF scalar \|B\| (nT) | `omni_high_resolution.b_magnitude_nt` |
| `ap30` | SMALLINT | Equivalent 30-min amplitude (target variable for `regression-sw`) | `hpo_hp30.ap30` |
| `hp30` | REAL | Hp30 geomagnetic activity index | `hpo_hp30.hp30` |

- **Primary Key**: `datetime`
- **Source tables**: `omni_high_resolution`, `hpo_hp30`
- **Build command**: `swdb build sw-30min --start-year 2010 --end-year 2026`

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
