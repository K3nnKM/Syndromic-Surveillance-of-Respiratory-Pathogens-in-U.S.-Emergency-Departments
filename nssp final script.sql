/* **********************************************************************
   NSSP PORTFOLIO PROJECT – MASTER SQL SCRIPT
   Author: Ken (health data scientist)
   Description:
     - Create and clean base table (NSSP)
     - Build core time-series views
     - Build ranking / burden views
     - Support national vs state comparisons and timelapse viz
   Requirements:
     - MySQL 8+ (for window functions and YEARWEEK)
     - CSV file: nssp.csv
       Columns (in order):
         reg, nssp_date (DD-MM-YY), pathogen, geography,
         percent_visits, other_visits
********************************************************************** */


/*======================================================================
  1. CREATE BASE TABLE
  ----------------------------------------------------------------------
  This table will hold the raw NSSP data with a primary key on `reg`.
  We store `nssp_date` as DATE, but you may initially load it as TEXT
  and then convert it using STR_TO_DATE (see section 2).
======================================================================*/

/* -- DROP TABLE IF EXISTS nssp;

CREATE TABLE nssp (
  reg            VARCHAR(20) PRIMARY KEY,  -- unique row identifier
  nssp_date      DATE,                     -- date of observation (ED visit day)
  pathogen       VARCHAR(20),              -- pathogen/syndrome label (e.g., RSV, ARI)
  geography      VARCHAR(50),              -- state name (e.g., Alaska, Alabama)
  percent_visits DECIMAL(5,2),             -- % of ED visits with this pathogen
  other_visits   DECIMAL(5,2)              -- % of ED visits without this pathogen
); */

/*
  At this point, import nssp.csv into `nssp` using MySQL Workbench or CLI.
  Make sure the columns are mapped correctly in order.
  If Workbench imports `nssp_date` as VARCHAR, keep going to step 2.
*/


/*======================================================================
  2. DATE CLEANING AND BASIC SANITY CHECKS
  ----------------------------------------------------------------------
  The CSV uses DD-MM-YY (e.g., 02-12-23). We convert it to DATE.
  If `nssp_date` is already a DATE, skip the UPDATE.
======================================================================*/

-- If nssp_date was loaded as a VARCHAR, convert it
-- (if it's already DATE, this will raise an error; then just omit it).

-- Example (RUN ONLY IF nssp_date IS TEXT):
-- UPDATE nssp
-- SET nssp_date = STR_TO_DATE(nssp_date, '%d-%m-%y');

-- Sanity check: number of rows
SELECT COUNT(*) AS n_rows FROM nssp;

-- Sanity check: date range
SELECT
  MIN(nssp_date) AS min_date,
  MAX(nssp_date) AS max_date
FROM nssp;

-- Sanity check: how many states and pathogens
SELECT
  COUNT(DISTINCT geography) AS n_states,
  COUNT(DISTINCT pathogen)  AS n_pathogens
FROM nssp;


/*======================================================================
  3. CORE TIME-SERIES VIEWS
  ----------------------------------------------------------------------
  These are your main "fact tables" that everything else builds on.
  - v_daily_pathogen_national: national daily prevalence per pathogen
  - v_daily_state_pathogen   : daily prevalence by state and pathogen
  - v_weekly_pathogen_national: weekly national prevalence per pathogen
======================================================================*/

-- 3.1 National daily prevalence by pathogen
-- -----------------------------------------
-- For each date and pathogen, calculate the average percent_visits
-- across all states. This approximates national burden among ED visits.

CREATE OR REPLACE VIEW v_daily_pathogen_national AS
SELECT
  nssp_date,
  pathogen,
  AVG(percent_visits) AS nat_prevalence
FROM nssp
GROUP BY nssp_date, pathogen;


-- 3.2 Daily state-level prevalence by pathogen
-- -------------------------------------------
-- For each date, state, and pathogen, compute the (possibly redundant)
-- average percent_visits. If the data is already at that grain, AVG
-- equals the original value; still safe and generic.

CREATE OR REPLACE VIEW v_daily_state_pathogen AS
SELECT
  nssp_date,
  geography AS state,
  pathogen,
  AVG(percent_visits) AS state_prevalence
FROM nssp
GROUP BY nssp_date, state, pathogen;


-- 3.3 Weekly national prevalence by pathogen
-- ------------------------------------------
-- Aggregate daily data to epidemiologic weeks using YEARWEEK(date, 3):
--   - mode 3: weeks start on Monday, ISO-like definition.
--   - we also keep week_start (MIN(nssp_date)) for plotting.

CREATE OR REPLACE VIEW v_weekly_pathogen_national AS
SELECT
  YEARWEEK(nssp_date, 3) AS yearweek,   -- e.g., 202348 = 48th week of 2023
  MIN(nssp_date)         AS week_start, -- first day in that week in the data
  pathogen,
  AVG(percent_visits)    AS nat_prevalence
FROM nssp
GROUP BY YEARWEEK(nssp_date, 3), pathogen;


/*======================================================================
  4. STATE VS NATIONAL COMPARISON VIEW
  ----------------------------------------------------------------------
  Join daily state-level prevalence with daily national prevalence
  for each pathogen. This is useful to:
    - Compare a given state's curve with the national curve.
    - See whether states are consistently above or below national.
======================================================================*/

CREATE OR REPLACE VIEW v_state_vs_national_daily AS
SELECT
  s.nssp_date,
  s.pathogen,
  s.state,
  s.state_prevalence,
  n.nat_prevalence
FROM v_daily_state_pathogen s
JOIN v_daily_pathogen_national n
  ON n.nssp_date = s.nssp_date
 AND n.pathogen  = s.pathogen;


/*======================================================================
  5. YEARLY (OR SEASONAL) STATE BURDEN
  ----------------------------------------------------------------------
  Many epi questions are "which states had highest/lowest burden
  in a given period?". Here we use calendar year as a simple period.
  - v_state_yearly_pathogen: mean prevalence per state, pathogen, year.
  - v_state_yearly_rank    : rank states within each pathogen-year.
======================================================================*/

-- 5.1 Mean yearly prevalence per state and pathogen
-- ------------------------------------------------
-- For each combination of year, pathogen, and state, compute the mean
-- percent_visits. This smooths daily noise and gives a clearer picture
-- of burden over a year.

CREATE OR REPLACE VIEW v_state_yearly_pathogen AS
SELECT
  YEAR(nssp_date)       AS year,
  pathogen,
  geography             AS state,
  AVG(percent_visits)   AS mean_prevalence
FROM nssp
GROUP BY YEAR(nssp_date), pathogen, state;


-- 5.2 Rank states within each pathogen and year
-- ---------------------------------------------
-- Use window functions to rank states from highest to lowest mean
-- prevalence for each pathogen-year. Also rank from lowest to highest.

CREATE OR REPLACE VIEW v_state_yearly_rank AS
WITH yearly AS (
  SELECT
    YEAR(nssp_date)     AS year,
    pathogen,
    geography           AS state,
    AVG(percent_visits) AS mean_prevalence
  FROM nssp
  GROUP BY YEAR(nssp_date), pathogen, state
)
SELECT
  year,
  pathogen,
  state,
  mean_prevalence,
  RANK() OVER (
    PARTITION BY year, pathogen
    ORDER BY mean_prevalence DESC
  ) AS rnk_high,   -- 1 = highest burden for that pathogen-year
  RANK() OVER (
    PARTITION BY year, pathogen
    ORDER BY mean_prevalence ASC
  ) AS rnk_low     -- 1 = lowest burden for that pathogen-year
FROM yearly;


/*======================================================================
  6. DAILY STATE RANKS ("LEAGUE TABLES")
  ----------------------------------------------------------------------
  For timelapse and more granular views, we also rank states daily:
    - v_state_burden_rank_daily: ranks each state per day & pathogen.
  You can filter to top 3 or bottom 3 in BI tools for animation.
======================================================================*/

CREATE OR REPLACE VIEW v_state_burden_rank_daily AS
WITH daily_state AS (
  SELECT
    nssp_date,
    pathogen,
    geography AS state,
    AVG(percent_visits) AS state_prevalence
  FROM nssp
  GROUP BY nssp_date, pathogen, state
)
SELECT
  nssp_date,
  pathogen,
  state,
  state_prevalence,
  RANK() OVER (
    PARTITION BY nssp_date, pathogen
    ORDER BY state_prevalence DESC
  ) AS rnk_high,  -- 1 = highest state that day for that pathogen
  RANK() OVER (
    PARTITION BY nssp_date, pathogen
    ORDER BY state_prevalence ASC
  ) AS rnk_low    -- 1 = lowest state that day for that pathogen
FROM daily_state;


/*======================================================================
  7. TOP PATHOGEN PER DAY (NATIONAL) + TOP STATE FOR IT
  ----------------------------------------------------------------------
  v_top_pathogen_daily_national:
    - For each date:
        1) Find the pathogen with the highest national prevalence.
        2) For that pathogen, find which state has the highest prevalence.
  This is good for storytelling:
    - "On this day, pathogen X was dominant nationally, with state Y
       having the highest burden for that pathogen."
======================================================================*/

CREATE OR REPLACE VIEW v_top_pathogen_daily_national AS
-- Step 1: national prevalence per date & pathogen
WITH daily AS (
  SELECT
    nssp_date,
    pathogen,
    AVG(percent_visits) AS nat_prevalence
  FROM nssp
  GROUP BY nssp_date, pathogen
),
-- Step 2: pick the top pathogen per day, nationally
top_pathogen AS (
  SELECT nssp_date, pathogen, nat_prevalence
  FROM (
    SELECT
      nssp_date,
      pathogen,
      nat_prevalence,
      RANK() OVER (
        PARTITION BY nssp_date
        ORDER BY nat_prevalence DESC
      ) AS rnk
    FROM daily
  ) t
  WHERE rnk = 1
),
-- Step 3: for that pathogen and date, find the state with the highest prevalence
state_max AS (
  SELECT
    nssp_date,
    pathogen,
    geography AS state,
    AVG(percent_visits) AS state_prevalence,
    RANK() OVER (
      PARTITION BY nssp_date, pathogen
      ORDER BY AVG(percent_visits) DESC
    ) AS rnk_state
  FROM nssp
  GROUP BY nssp_date, pathogen, geography
)
-- Final result: one (or more, if ties) row per date
SELECT
  tp.nssp_date,
  tp.pathogen,
  tp.nat_prevalence,         -- national prevalence for the top pathogen
  sm.state       AS top_state,
  sm.state_prevalence AS top_state_prevalence
FROM top_pathogen tp
JOIN state_max sm
  ON sm.nssp_date = tp.nssp_date
 AND sm.pathogen  = tp.pathogen
WHERE sm.rnk_state = 1;


/*======================================================================
  8. EDA-ORIENTED VIEWS (OPTIONAL BUT USEFUL)
  ----------------------------------------------------------------------
  These are simple views that you can use for EDA and exporting data
  into Python/R/Excel if needed.
======================================================================*/

-- 8.1 Daily national EDA view
CREATE OR REPLACE VIEW v_eda_daily_pathogen AS
SELECT
  nssp_date,
  pathogen,
  AVG(percent_visits) AS nat_prevalence
FROM nssp
GROUP BY nssp_date, pathogen;

-- 8.2 Daily state-level EDA view
CREATE OR REPLACE VIEW v_eda_daily_state_pathogen AS
SELECT
  nssp_date,
  geography AS state,
  pathogen,
  AVG(percent_visits) AS state_prevalence
FROM nssp
GROUP BY nssp_date, state, pathogen;


/*======================================================================
  9. HOW TO USE THESE VIEWS (SHORT NOTES)
  ----------------------------------------------------------------------
  In Tableau / Power BI:
    - Connect to MySQL, import the views you need.

  Examples:

  1) National epidemic curves:
     - Data: v_daily_pathogen_national
     - X: nssp_date
     - Y: nat_prevalence
     - Color: pathogen

  2) State vs national comparison:
     - Data: v_state_vs_national_daily
     - Filter to one pathogen and one/few states.
     - X: nssp_date
     - Y: state_prevalence and nat_prevalence (two lines)

  3) Yearly state maps / rankings:
     - Data: v_state_yearly_rank
     - Filter: year and pathogen
     - Map: state, color by mean_prevalence
     - Table: sort by mean_prevalence, show rnk_high / rnk_low

  4) Timelapse "league tables":
     - Data: v_state_burden_rank_daily
     - Filter: pathogen, rnk_high <= 3
     - Play over nssp_date to see top-3 states move over time.

  5) Daily top pathogen and top state:
     - Data: v_top_pathogen_daily_national
     - Timeline: color by pathogen
     - Supplement: show top_state and top_state_prevalence in tooltips.
======================================================================*/
