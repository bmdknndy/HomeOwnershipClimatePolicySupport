-- Reset if needed
DROP TABLE IF EXISTS ces_raw;

-- Load: make everything VARCHAR so DuckDB never chokes on "NA"
CREATE TABLE ces_raw AS
SELECT *
FROM read_csv(
  'data/raw/CCES24_Common_OUTPUT_vv_topost_final.csv',
  header = true,
  all_varchar = true,
  nullstr = ['NA', '__NA__', '']
);

-- Sanity check
SELECT COUNT(*) AS n_rows FROM ces_raw;

-- The file is 2024-only, so no `year` column.
-- Add it as a constant in your analysis layer.
CREATE OR REPLACE VIEW v_2024 AS
SELECT
  '2024'::INTEGER AS year,

  -- identifiers
  caseid,

  -- weights (keep numeric)
  TRY_CAST(commonweight AS DOUBLE)      AS commonweight,
  TRY_CAST(commonpostweight AS DOUBLE)  AS commonpostweight,
  TRY_CAST(vvweight AS DOUBLE)          AS vvweight,
  TRY_CAST(vvweight_post AS DOUBLE)     AS vvweight_post,

  -- class-ish structure
  TRY_CAST(educ AS INTEGER)             AS educ,
  TRY_CAST(faminc_new AS INTEGER)       AS faminc_new,
  TRY_CAST(ownhome AS INTEGER)          AS ownhome,

  -- politics controls
  TRY_CAST(pid7 AS INTEGER)             AS pid7,
  TRY_CAST(ideo5 AS INTEGER)            AS ideo5,

  -- climate/energy block (placeholder list; verify meanings in the 2024 guide)
  TRY_CAST(CC24_300_1 AS INTEGER)       AS CC24_300_1,
  TRY_CAST(CC24_300_2 AS INTEGER)       AS CC24_300_2,
  TRY_CAST(CC24_300_3 AS INTEGER)       AS CC24_300_3,
  TRY_CAST(CC24_300_4 AS INTEGER)       AS CC24_300_4,
  TRY_CAST(CC24_300_5 AS INTEGER)       AS CC24_300_5,
  TRY_CAST(CC24_300a AS INTEGER)        AS CC24_300a,
  TRY_CAST(CC24_300c AS INTEGER)        AS CC24_300c

FROM ces_raw;

-- Quick peek
SELECT * FROM v_2024 LIMIT 5;