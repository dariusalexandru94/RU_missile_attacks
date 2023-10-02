-- Database: missile_uavs

CREATE TABLE attacks (
    time_start TIMESTAMP,
    time_end TIMESTAMP,
    model VARCHAR(50),
    launch_place VARCHAR(80),
    target VARCHAR(80),
    carrier VARCHAR(50),
    launched SMALLINT,
    destroyed SMALLINT,
    source_post TEXT
)
  
COPY attacks(time_start, time_end, model, launch_place, target, carrier, launched, destroyed, source_post)
FROM 'C:\Users\Public\Missile_UAVS\missile_attacks_daily.csv'
DELIMITER ','
CSV HEADER;  

CREATE TABLE features (
    model VARCHAR(50),
    category VARCHAR(50),
    type VARCHAR(50),
    national_origin VARCHAR(25),
    launch_platform VARCHAR(150),
    name VARCHAR(50),
    name_nato VARCHAR(50),
    guidance_system VARCHAR(50),
    manufacturer VARCHAR(170)
)

COPY features(model, category, type, national_origin, launch_platform, name, name_nato, guidance_system, manufacturer)
FROM 'C:\Users\Public\Missile_UAVS\missiles_and_uav.csv'
DELIMITER ','
CSV HEADER;  

--------------- DATA QUALITY ASSESSMENT -----------------------------------------------------------------------------------------------
--------------- DATA QUALITY ASSESSMENT -----------------------------------------------------------------------------------------------

SELECT * FROM features
SELECT * FROM attacks

SELECT DISTINCT model FROM attacks
SELECT DISTINCT launch_place FROM attacks
SELECT DISTINCT target FROM attacks
SELECT DISTINCT carrier FROM attacks

SELECT COUNT(*) FROM attacks WHERE launched IS NULL
SELECT COUNT(*) FROM attacks WHERE destroyed IS NULL

SELECT *
FROM attacks
WHERE model IS NULL OR model::text = '';

SELECT * FROM attacks
WHERE launch_place IS NULL OR launch_place::text = ''; -- 188 missing values

SELECT * FROM attacks
WHERE target IS NULL OR target::text = ''; --32 missing values

SELECT * FROM attacks
WHERE carrier IS NULL OR carrier::text = ''; --275 missing values

--------------- TRANSFORMATION --------------------------------------------------------------------------------------------------------
--------------- TRANSFORMATION --------------------------------------------------------------------------------------------------------

UPDATE attacks
SET model = REPLACE(model, 'X-', 'Kh-')
WHERE model LIKE '%X-%';

UPDATE attacks
SET model = REPLACE(model, 'C-', 'S-')
WHERE model LIKE '%C-%';

UPDATE attacks
SET carrier = REPLACE(carrier, 'TU', 'Tu')
WHERE carrier LIKE '%TU%'

UPDATE attacks
SET model = REPLACE(model, 'Iskander-K', 'Iskander')
WHERE model LIKE '%Iskander-K%'

UPDATE attacks
SET model = REPLACE(model, 'Iskander-M', 'Iskander')
WHERE model LIKE '%Iskander-M%'

ALTER TABLE attacks RENAME TO attacks1

CREATE TABLE attacks AS
SELECT *
FROM attacks1
ORDER BY time_start;

DROP TABLE attacks1

ALTER TABLE attacks DROP COLUMN source_post
ALTER TABLE attacks DROP COLUMN target -- too vague

UPDATE features
SET model = REPLACE(model, 'X-', 'Kh-')
WHERE model LIKE '%X-%';

UPDATE features
SET model = REPLACE(model, 'C-', 'S-')
WHERE model LIKE '%C-%';

UPDATE features
SET model = REPLACE(model, 'Iskander-K', 'Iskander')
WHERE model LIKE '%Iskander-K%'

UPDATE features
SET model = REPLACE(model, 'Iskander-M', 'Iskander')
WHERE model LIKE '%Iskander-M%'

DELETE FROM features
WHERE model = 'Iskander'
AND category = 'cruise missile'

--------------- DATA EXPLORATION AND KNOWLEDGE DISCOVERY -------------------------------------------------------------------------------
--------------- DATA EXPLORATION AND KNOWLEDGE DISCOVERY -------------------------------------------------------------------------------


-- total number of launched missiles/UAVs per each model
SELECT model, SUM(launched) as total
FROM attacks
WHERE model NOT LIKE '%and%'
GROUP BY model
ORDER BY total DESC;
-- AT LEAST!! this no. per each model. There are strikes when multiple models are used and
-- 'launched' column is not agragated properly;


-- number of strikes / model
WITH split_models AS (
    SELECT regexp_split_to_table(model, E'\\sand\\s|,|\\s+|\\s+and\\s+') AS model_part
    FROM attacks
)
SELECT model_part, COUNT(model_part) AS count
FROM split_models
WHERE model_part <> ''
GROUP BY model_part
ORDER BY count DESC;


-- launch place
SELECT launch_place, COUNT(launch_place), SUM(launched) FROM attacks
GROUP BY launch_place
ORDER BY COUNT DESC;
-- although the majority of distinct attacks originate from the Black Sea, the most missiles were launched
-- from the Caspian Sea area, using strategic aviation


-- most used carriers
WITH SplitRecords AS (
  SELECT
    regexp_replace(
      unnest(
        regexp_split_to_array(
          regexp_replace(carrier, '^\d+\s*x\s*', ''),
          ' and '
        )
      ),
      '^\d+\s*x\s*',
      ''
    ) AS cleaned_column
  FROM attacks
)
SELECT
  cleaned_column,
  COUNT(*) AS count
FROM SplitRecords
GROUP BY cleaned_column
ORDER BY count DESC;


-- percentage of destruction / model
SELECT
  atk.model,
  ft.category,
  ft.type,
  SUM(atk.destroyed) AS total_destroyed,
  SUM(atk.launched) AS total_launched,
  ROUND((SUM(atk.destroyed)::numeric / SUM(atk.launched)::numeric) * 100, 2) AS percent
FROM
  attacks atk
  LEFT OUTER JOIN features ft USING (model)
WHERE atk.model NOT LIKE '%and%'
GROUP BY
  model, ft.category, ft.type
ORDER BY
  percent;


-- percentage of destruction / day
SELECT 
    TO_CHAR(DATE_TRUNC('day', time_start), 'YYYY-MM-DD') AS Date,
    SUM(launched) AS total_launched,
    SUM(destroyed) AS total_destroyed,
    ROUND((SUM(destroyed)::numeric / SUM(launched)::numeric) * 100, 0) AS procent
FROM attacks
GROUP BY Date
ORDER BY Date;

----------------------------------------- strategic aviation ----------------------
SELECT * FROM attacks
WHERE carrier LIKE '%Tu%';

-- strikes per each Tupolev 
WITH SplitRecords AS (
  SELECT
    unnest(
      regexp_split_to_array(
        regexp_replace(carrier, '^\d+\s*x\s*', ''),
        ' and '
      )
    ) AS cleaned_column
  FROM attacks
  WHERE carrier LIKE '%Tu%'
)
SELECT
  regexp_replace(cleaned_column, '^\d+\s*x\s*', '') AS cleaned_column,
  COUNT(*) AS count
FROM SplitRecords
WHERE cleaned_column LIKE '%Tu%'
GROUP BY regexp_replace(cleaned_column, '^\d+\s*x\s*', '')
ORDER BY count DESC;

-- aircraft formation
SELECT carrier, COUNT(launch_place)
FROM attacks
WHERE carrier LIKE '%Tu%'
AND carrier LIKE '%x %'
GROUP BY carrier
ORDER BY COUNT DESC;

-- aircraft formation + launch place
SELECT carrier, launch_place, COUNT(launch_place)
FROM attacks
WHERE carrier LIKE '%Tu%'
AND carrier LIKE '%x %'
GROUP BY carrier, launch_place
ORDER BY COUNT DESC;
-- most frequently, strategic aviation uses 4-6 Tu-95MS aircraft launched from the Caspian Sea
-- the largest number of aicrafts used was 14 Tu-95MS

-- Launch platform, type and NATO name of each Kh missile
SELECT launch_platform, name, name_nato, type FROM features
WHERE launch_platform LIKE '%Tu%';

SELECT * FROM attacks
WHERE model IN ('Kh-101/Kh-555', 'Kh-22', 'Kh-47 Kinzhal')
AND carrier NOT LIKE '%MiG%'
-- russians prefer to launch Kinzhal missiles from MiG-31K aircraft rather than from Tu-22M3


-- Engels airbase
SELECT * FROM attacks
WHERE launch_place LIKE '%Engels%';
-- only Tu-95MS aircrafts at Engels airbase

-- Caspian Sea
SELECT * FROM attacks
WHERE launch_place LIKE '%Caspian Sea%'
AND carrier LIKE '%Tu%';
-- most strategic aviation strikes originate from the direction of the Caspian Sea


SELECT ROUND(AVG(launched), 0) FROM attacks
WHERE carrier LIKE '%Tu%';
-- on average, strategic aviation launches approximately 21 missiles per attack

-- Kinzhal
SELECT time_start, model, launched, destroyed FROM attacks
WHERE model = 'Kh-47 Kinzhal';
-- on May 4, 2023, the first "invincible" :)) Kinzhal missile was destroyed

----------------------------------------- Shahed -----------------------------------

SELECT * FROM attacks
WHERE model LIKE '%Shahed%'


SELECT SUM(launched) AS sum_launched,
       SUM(destroyed) AS sum_destroyed,
       MAX(launched) AS max_launched,
       ROUND(AVG(launched), 0) AS mean_launched
FROM attacks
WHERE model = 'Shahed-136/131'

-- Shahed: launch place
SELECT launch_place, COUNT(launch_place) FROM attacks
WHERE model = 'Shahed-136/131'
GROUP BY launch_place
ORDER BY COUNT DESC;
-- mostly unknown

-- Shahed: launched per distinct launch place
SELECT launch_place, COUNT(launch_place), 
                     SUM(launched) AS total_launched, 
                     SUM(destroyed) AS total_destroyed
FROM attacks
WHERE model = 'Shahed-136/131'
GROUP BY launch_place
ORDER BY COUNT DESC;

-- Shahed; duration: average, max
SELECT MAX(time_end - time_start), AVG(time_end - time_start) FROM attacks
WHERE model = 'Shahed-136/131'

----------------------------------------- Navy -----------------------------------

SELECT * FROM attacks
WHERE carrier = 'Navi'
OR carrier = 'Navi and Submarines'
OR model = 'Kalibr'


SELECT carrier, launched, destroyed FROM attacks
WHERE carrier LIKE '%Navi%'
AND carrier NOT LIKE '%MS%'
AND model = 'Kalibr'
ORDER BY launched DESC;

-- mean, max, min
SELECT SUM(launched) AS total_launched, 
       SUM(destroyed) AS total_destroyed, 
       ROUND(AVG(launched), 0) AS mean,
       MAX(launched),
       MIN(launched) FROM attacks
WHERE carrier LIKE '%Navi%'
AND carrier NOT LIKE '%MS%'
AND model = 'Kalibr'

-- submarines
SELECT * FROM attacks
WHERE carrier LIKE '%Submarines%'

-- navi; duration: average, max
SELECT MAX(time_end - time_start), AVG(time_end - time_start) FROM attacks
WHERE carrier LIKE '%Navi%'
AND carrier NOT LIKE '%MS%'
AND model = 'Kalibr'


----------------------------------------- time series analysis -----------------------------------

-- duration of each attack (hh:mm:ss)
SELECT time_start, time_end,
       (time_end - time_start) AS duration
FROM attacks;

-- duration: max, avg, std
SELECT 
    MAX(duration) AS max_duration,
    AVG(duration) AS avg_duration,
    ROUND(STDDEV(EXTRACT(EPOCH FROM duration) / 3600), 2) AS std_dev_duration
FROM (
    SELECT time_start, 
           time_end,
           (time_end - time_start) AS duration
    FROM attacks
) AS subquery;

-- there are 30 outliers whean speaking about duration => median
SELECT COUNT(*) FROM (
    WITH attack_durations AS (
        SELECT EXTRACT(EPOCH FROM (time_end - time_start)) AS duration_seconds
        FROM attacks
    ),
    z_scores AS (
        SELECT duration_seconds,
               ABS((duration_seconds - AVG(duration_seconds) OVER ()) / STDDEV(duration_seconds) OVER ()) AS z_score
        FROM attack_durations
    )
    SELECT duration_seconds
    FROM z_scores
    WHERE z_score > 2
) AS subquery_alias;

-- median of duration
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (time_end - time_start)) AS median_duration
FROM attacks;
-- real duration average: 03:30

SELECT * FROM attacks
WHERE time_end - time_start >= '00:30:00'
AND time_end - time_start <= '11:00:00'

SELECT * FROM attacks
WHERE time_end - time_start > '1 day'

-- average duration of strikes by missile/UAV model (hours)
SELECT model, round(AVG(EXTRACT(EPOCH FROM (time_end - time_start))) / 3600, 0) AS avg_duration_hours
FROM attacks
WHERE MODEL NOT LIKE '%and%'
GROUP BY model
ORDER BY avg_duration_hours DESC;

-- Daytime / Nighttime
CREATE TABLE daytime_nighttime AS
SELECT model, launched, destroyed,
       CASE
           WHEN EXTRACT(HOUR FROM time_start) >= 6 AND EXTRACT(HOUR FROM time_start) < 21 THEN 'Daytime'
           ELSE 'Nighttime'
       END AS time FROM attacks
       
SELECT time, COUNT(*) AS count
FROM daytime_nighttime
GROUP BY time;
-- huge difference; the vast majority of attacks occur at nighttime


-- number of launced missile/UAV per each day
SELECT DATE(time_start) AS date, SUM(launched) AS total_launched 
FROM attacks
GROUP BY DATE(time_start)
ORDER BY total_launched DESC;

SELECT * FROM attacks
WHERE DATE(time_start) = '2022-10-10'

-- distinct attacks per day
SELECT DATE(time_start), COUNT (*) AS COUNT FROM attacks
GROUP BY time_start
ORDER BY COUNT DESC;

SELECT * FROM attacks
WHERE DATE(time_start) = '2023-05-14'

-- strikes/month
SELECT TO_CHAR(time_start, 'YYYY-MM') AS year_month,
       COUNT(*) AS total_attacks
FROM attacks
GROUP BY year_month
ORDER BY total_attacks desc;
-- May 2023


-- strikes by days of the week
SELECT EXTRACT(DOW FROM time_start) AS day_of_week,
       COUNT(*) AS total_attacks, SUM(launched) as total_launched
FROM attacks
GROUP BY day_of_week
ORDER BY day_of_week;
-- monday and thursday, highest probability of strikes


-- strikes by days of the month
SELECT EXTRACT(day FROM time_start) AS day_of_month,
       COUNT(*) AS total_attacks, SUM(launched) as total_launched
FROM attacks
GROUP BY day_of_month
ORDER BY day_of_month;
