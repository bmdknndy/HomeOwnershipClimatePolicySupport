-- Reset if needed
DROP TABLE IF EXISTS analysis_model;

-- Create analysis table
CREATE TABLE analysis_model AS
WITH typed AS (
  SELECT
    -- id
    TRY_CAST(caseid AS BIGINT) AS caseid,

    -- weights
    TRY_CAST(commonweight AS DOUBLE) AS weight,

    -- politics
    TRY_CAST(pid7 AS INTEGER) AS pid7,
    TRY_CAST(ideo5 AS INTEGER) AS ideo5,

    -- class proxies
    TRY_CAST(faminc_new AS INTEGER) AS faminc_new,
    TRY_CAST(educ AS INTEGER) AS educ,
    TRY_CAST(ownhome AS INTEGER) AS ownhome,

    -- demographics
    TRY_CAST(birthyr AS INTEGER) AS birthyr,
    TRY_CAST(gender4 AS INTEGER) AS gender4,
    TRY_CAST(race AS INTEGER) AS race,
    TRY_CAST(hispanic AS INTEGER) AS hispanic,

    -- environmental policy
    TRY_CAST(CC24_326a AS INTEGER) AS CC24_326a,
    TRY_CAST(CC24_326b AS INTEGER) AS CC24_326b,
    TRY_CAST(CC24_326c AS INTEGER) AS CC24_326c,
    TRY_CAST(CC24_326d AS INTEGER) AS CC24_326d,
    TRY_CAST(CC24_326e AS INTEGER) AS CC24_326e,
    TRY_CAST(CC24_326f AS INTEGER) AS CC24_326f

  FROM ces_raw
),

recode AS (
  SELECT
    caseid,
    weight,
    pid7,
    ideo5,
    faminc_new,
    educ,
    ownhome,
    birthyr,
    gender4,
    race,
    hispanic,

    -- Derived age assuming 2024
    CASE
      WHEN birthyr BETWEEN 1900 AND 2010 THEN 2024 - birthyr
      ELSE NULL
    END AS age,

    CASE
      WHEN birthyr BETWEEN 1900 AND 2010 THEN
        CASE
          WHEN 2024 - birthyr BETWEEN 18 AND 29 THEN '18_29'
          WHEN 2024 - birthyr BETWEEN 30 AND 44 THEN '30_44'
          WHEN 2024 - birthyr BETWEEN 45 AND 64 THEN '45_64'
          WHEN 2024 - birthyr >= 65 THEN '65_plus'
          ELSE NULL
        END
      ELSE NULL
    END AS age_group,

    -- Homeowner flag 
    CASE WHEN ownhome = 1 THEN 1 WHEN ownhome = 2 THEN 0 ELSE NULL END AS homeowner,

    -- Income bins 
    CASE
      WHEN faminc_new IS NULL THEN NULL
      WHEN faminc_new BETWEEN 1 AND 4 THEN 'low'
      WHEN faminc_new BETWEEN 5 AND 9 THEN 'middle'
      WHEN faminc_new >= 10 THEN 'high'
      ELSE NULL
    END AS income_group,

    -- Education binning
    CASE
      WHEN educ IS NULL THEN NULL
      WHEN educ IN (1,2,3) THEN 'hs_or_less'
      WHEN educ IN (4,5) THEN 'some_college'
      WHEN educ IN (6) THEN 'ba'
      WHEN educ IN (7,8,9) THEN 'postgrad'
      ELSE NULL
    END AS education_group,

    -- Race/ethnicity (if hispanic == 1 treat as Hispanic regardless of race)
    CASE
      WHEN hispanic = 1 THEN 'hispanic'
      WHEN race = 1 THEN 'white_nonhisp'
      WHEN race = 2 THEN 'black_nonhisp'
      WHEN race = 3 THEN 'asian_nonhisp'
      WHEN race IS NULL THEN NULL
      ELSE 'other_nonhisp'
    END AS race_eth,

    -- Recode environment items
    CASE WHEN CC24_326a = 1 THEN 1 WHEN CC24_326a = 2 THEN 0 ELSE NULL END AS epa_regulate_co2,
    CASE WHEN CC24_326b = 1 THEN 1 WHEN CC24_326b = 2 THEN 0 ELSE NULL END AS renewables_20pct,
    CASE WHEN CC24_326c = 1 THEN 1 WHEN CC24_326c = 2 THEN 0 ELSE NULL END AS strengthen_epa_even_if_jobs,
    CASE WHEN CC24_326d = 1 THEN 1 WHEN CC24_326d = 2 THEN 0 ELSE NULL END AS increase_fossil_fuel_production,
    CASE WHEN CC24_326e = 1 THEN 1 WHEN CC24_326e = 2 THEN 0 ELSE NULL END AS halt_new_oil_gas_leases,
    CASE WHEN CC24_326f = 1 THEN 1 WHEN CC24_326f = 2 THEN 0 ELSE NULL END AS prevent_gas_stove_ban

  FROM typed
),

index_build AS (
  SELECT
    *,

    -- Clean energy & regulation index (mean of 3 pro-environment items)
    (
      (epa_regulate_co2 IS NOT NULL)::INT
      + (renewables_20pct IS NOT NULL)::INT
      + (strengthen_epa_even_if_jobs IS NOT NULL)::INT
    ) AS n_index_items,

    CASE
      WHEN (
        (epa_regulate_co2 IS NOT NULL)::INT
        + (renewables_20pct IS NOT NULL)::INT
        + (strengthen_epa_even_if_jobs IS NOT NULL)::INT
      ) >= 2
      THEN (
        COALESCE(epa_regulate_co2, 0)
        + COALESCE(renewables_20pct, 0)
        + COALESCE(strengthen_epa_even_if_jobs, 0)
      )::DOUBLE
      / (
        (epa_regulate_co2 IS NOT NULL)::INT
        + (renewables_20pct IS NOT NULL)::INT
        + (strengthen_epa_even_if_jobs IS NOT NULL)::INT
      )::DOUBLE
      ELSE NULL
    END AS clean_energy_support,

    -- Support for fossil fuel expansion 
    increase_fossil_fuel_production AS support_fossil_fuel_expansion

  FROM recode
)

SELECT
  caseid,
  weight,

  -- politics
  pid7,
  ideo5,

  -- class
  income_group,
  education_group,
  homeowner,

  -- demographics
  age,
  age_group,
  gender4,
  race_eth,

  -- outcomes
  clean_energy_support,
  support_fossil_fuel_expansion,

  -- components
  epa_regulate_co2,
  renewables_20pct,
  strengthen_epa_even_if_jobs,
  increase_fossil_fuel_production,
  halt_new_oil_gas_leases,
  prevent_gas_stove_ban,

  -- QA
  n_index_items

FROM index_build;

-- Check row count
SELECT COUNT(*) AS n_rows FROM analysis_model;

-- Check missingness + mean of DV
SELECT
  SUM(clean_energy_support IS NULL) AS missing_clean_energy_support,
  AVG(clean_energy_support) AS mean_clean_energy_support
FROM analysis_model;

-- Check support rates for each policy item 
SELECT
  AVG(epa_regulate_co2) AS p_epa_regulate_co2,
  AVG(renewables_20pct) AS p_renewables_20pct,
  AVG(strengthen_epa_even_if_jobs) AS p_strengthen_epa_even_if_jobs,
  AVG(increase_fossil_fuel_production) AS p_increase_fossil_fuel_production,
  AVG(halt_new_oil_gas_leases) AS p_halt_new_oil_gas_leases,
  AVG(prevent_gas_stove_ban) AS p_prevent_gas_stove_ban
FROM analysis_model;