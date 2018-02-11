DROP TABLE raw_data;

DROP TABLE summary;

CREATE TABLE raw_data
(
  ssoid         TEXT,
  ts            TEXT,
  grp           TEXT,
  atype         TEXT,
  asubtype      TEXT,
  url           TEXT,
  orgid         TEXT,
  formid        TEXT,
  code          TEXT,
  ltpa          TEXT,
  sudirresponse TEXT,
  ymdh          TEXT
);

CREATE TABLE summary
(
  total    BIGINT,
  selected BIGINT
);

COPY raw_data (ssoid, ts, grp, atype, asubtype, url, orgid, formid, code, ltpa, sudirresponse, ymdh)
FROM '/var/test_case.csv'
DELIMITER ';' CSV HEADER;

SELECT DISTINCT asubtype
FROM raw_data;

SELECT
  formid,
  atype,
  asubtype
FROM raw_data
GROUP BY formid, atype, asubtype
ORDER BY formid, atype, asubtype;

SELECT DISTINCT formid
FROM raw_data;

SELECT
  ssoid,
  ts,
  atype,
  asubtype
FROM raw_data
ORDER BY ssoid, ts;

SELECT DISTINCT formid
FROM raw_data;
SELECT *
FROM raw_data
WHERE formid = '-47'
ORDER BY ssoid, ts;

SELECT DISTINCT grp
FROM raw_data;

SELECT
  grp,
  formid
FROM raw_data
WHERE ssoid <> 'Unauthorized'
      AND ssoid IS NOT NULL
      AND ssoid <> ''
      AND formid IS NOT NULL
      AND formid <> 'null'
      AND formid <> ''
GROUP BY grp, formid
ORDER BY grp;

WITH services AS (
    SELECT
      grp,
      formid
    FROM raw_data
    WHERE ssoid <> 'Unauthorized'
          AND ssoid IS NOT NULL
          AND ssoid <> ''
          AND formid IS NOT NULL
          AND formid <> 'null'
          AND formid <> ''
    GROUP BY grp, formid
    ORDER BY grp
)
SELECT
  ssoid,
  raw_data.grp,
  raw_data.formid,
  atype,
  asubtype,
  ts
FROM raw_data
  INNER JOIN services
    ON raw_data.grp = services.grp
       AND raw_data.formid = services.formid
ORDER BY ssoid, raw_data.ts;


SELECT
  ssoid,
  grp,
  count(*)
FROM raw_data
GROUP BY ssoid, grp;

SELECT
  grp,
  atype,
  asubtype,
  ts,
  ymdh
FROM raw_data
WHERE ssoid = '105e8d6a-03e2-4911-b208-9656d0d88777'
ORDER BY ts;

SELECT TIMESTAMP 'epoch' + 1499765675 * INTERVAL '1 second';

SELECT to_timestamp(1499765675);

SELECT DISTINCT ssoid
FROM raw_data
WHERE ssoid <> 'Unauthorized'
      AND ssoid IS NOT NULL
      AND ssoid <> '';

SELECT DISTINCT ymdh
FROM raw_data;

SELECT
  ssoid,
  count(formid)
FROM raw_data
GROUP BY ssoid
ORDER BY ssoid;

SELECT
  ssoid,
  count(DISTINCT formid)
FROM raw_data
WHERE formid IS NOT NULL
      AND formid <> 'null'
GROUP BY ssoid
ORDER BY ssoid;

SELECT formid
FROM raw_data
WHERE ssoid = '00408b4b-a917-4622-a25c-e445115597a8';

SELECT
  ssoid,
  formid
FROM raw_data
WHERE ssoid <> 'Unauthorized'
      AND ssoid IS NOT NULL
      AND formid IS NOT NULL
      AND formid <> 'null'
GROUP BY ssoid, formid
ORDER BY formid;

SELECT
  formid,
  count(*) AS form_count
FROM raw_data
WHERE ssoid <> 'Unauthorized'
      AND ssoid IS NOT NULL
      AND formid IS NOT NULL
      AND formid <> 'null'
GROUP BY formid
ORDER BY form_count DESC
LIMIT 5;

-- топ-5 форм?
WITH form_counter AS (
    SELECT
      ssoid,
      grp,
      formid
    FROM raw_data
    WHERE ssoid <> 'Unauthorized'
          AND ssoid IS NOT NULL
          AND ssoid <> ''
          AND formid IS NOT NULL
          AND formid <> 'null'
          AND formid <> ''
    GROUP BY ssoid, grp, formid
    ORDER BY grp, formid
)
SELECT
  grp,
  formid,
  count(formid) AS fc
FROM form_counter
GROUP BY grp, formid
ORDER BY fc DESC
LIMIT 5;

SELECT
  ssoid,
  formid
FROM raw_data
WHERE ssoid NOT LIKE 'Unauthorized%'
      AND ssoid IS NOT NULL
      AND formid IS NOT NULL
      AND formid <> 'null'
      AND formid <> ''
ORDER BY ssoid, formid;

-- данные по использованию форм пользователями
SELECT
  ssoid,
  formid
FROM raw_data
GROUP BY ssoid, formid
ORDER BY ssoid;

WITH form_counter AS (
    SELECT
      ssoid,
      formid
    FROM raw_data
    WHERE ssoid NOT LIKE 'Unauthorized%'
          AND ssoid IS NOT NULL
          AND formid IS NOT NULL
          AND formid <> 'null'
          AND formid <> ''
    ORDER BY ssoid, formid
) SELECT
    ssoid,
    formid
  FROM form_counter
  GROUP BY ssoid, formid
  ORDER BY ssoid;

SELECT ssoid
FROM raw_data
GROUP BY ssoid;

SELECT
  ssoid,
  formid,
  atype,
  asubtype,
  ts
FROM raw_data
WHERE ssoid NOT LIKE 'Unauthorized%'
      AND ssoid IS NOT NULL
      AND formid IS NOT NULL
      AND formid <> 'null'
      AND formid <> ''
      AND formid = '51001'
ORDER BY ssoid, ts;

WITH form_steps AS (
    SELECT
      ssoid,
      formid,
      atype,
      asubtype,
      ts
    FROM raw_data
    WHERE ssoid NOT LIKE 'Unauthorized%'
          AND ssoid IS NOT NULL
          AND formid IS NOT NULL
          AND formid <> 'null'
          AND formid <> ''
    ORDER BY ssoid, ts
), usage_maximazer AS (
    SELECT
      ssoid,
      formid,
      count(*) AS usage_count
    FROM form_steps
    GROUP BY ssoid, formid
)

SELECT
  formid,
  max(usage_count)
FROM usage_maximazer
GROUP BY formid;

SELECT
  ssoid,
  grp,
  formid,
  atype,
  asubtype,
  ts
FROM raw_data
ORDER BY ssoid, grp, formid, atype, asubtype, ts;

WITH success_history AS (
    SELECT
      ssoid,
      grp,
      formid,
      atype,
      asubtype,
      ts
    FROM raw_data
    WHERE grp LIKE 'dszn_%'
          AND asubtype = 'send'
    ORDER BY ssoid, grp, formid, atype, asubtype, ts
) SELECT
    ssoid,
    grp,
    formid,
    min(ts)                                 AS started,
    max(ts)                                 AS completed,
    max(ts :: INTEGER) - min(ts :: INTEGER) AS duration,
    count(*)                                AS number_of_steps
  FROM success_history
  GROUP BY ssoid, grp, formid;

-- успехи
SELECT
  ssoid
FROM raw_data
WHERE grp LIKE 'dszn_%'
      AND asubtype = 'send'
ORDER BY ssoid;

WITH history AS (
    SELECT
      ssoid,
      grp,
      formid,
      atype,
      asubtype,
      ts
    FROM raw_data
    WHERE grp LIKE 'dszn_%'
    ORDER BY ssoid, grp, formid, atype, asubtype, ts
) SELECT
    ssoid,
    grp,
    formid,
    min(ts)                                 AS started,
    max(ts)                                 AS completed,
    max(ts :: INTEGER) - min(ts :: INTEGER) AS duration,
    count(*)                                AS number_of_steps
  FROM history
  GROUP BY ssoid, grp, formid;

SELECT ssoid, grp, formid, max(ts::BIGINT) as completed
FROM raw_data
WHERE grp LIKE 'dszn_%'
GROUP BY ssoid, grp, formid
ORDER BY ssoid;

SELECT ssoid, grp, formid, atype, asubtype
FROM raw_data
WHERE ssoid='0151a322-a1de-4ae1-bdca-566c71626f2a'
AND ts='1499766991';

WITH dszn_history AS (
    SELECT
      raw_data.ssoid AS ssoid,
      grp,
      formid,
      atype,
      asubtype,
      ts
    FROM raw_data
    WHERE grp LIKE 'dszn_%'
    ORDER BY raw_data.ssoid, grp, formid, atype, asubtype, ts
) SELECT
    dszn_history.ssoid,
    grp,
    formid,
    min(ts)                                 AS started,
    max(ts)                                 AS completed,
    max(ts :: INTEGER) - min(ts :: INTEGER) AS duration,
    count(*)                                AS number_of_steps
  FROM dszn_history
  GROUP BY dszn_history.ssoid, grp, formid;

WITH dszn_history AS (
    SELECT
      ssoid,
      grp,
      formid,
      atype,
      asubtype,
      ts
    FROM raw_data
    WHERE grp LIKE 'dszn_%'
    ORDER BY ssoid, grp, formid, atype, asubtype, ts
), totals AS (
    SELECT
      ssoid,
      grp,
      formid,
      min(ts)                                 AS started,
      max(ts)                                 AS completed,
      max(ts :: INTEGER) - min(ts :: INTEGER) AS duration,
      count(*)                                AS number_of_steps
    FROM dszn_history
    GROUP BY ssoid, grp, formid
) SELECT sum(number_of_steps)
  FROM totals;

SELECT count(*)
FROM raw_data
WHERE grp LIKE 'dszn_%';


SELECT
  grp,
  formid,
  atype,
  asubtype,
  ts
FROM raw_data
WHERE ssoid = '716da031-92e6-47d7-ad65-ea1337b33b9d'
      AND grp = 'dszn_17'
      AND formid = '1001'
ORDER BY ts;

SELECT
  grp,
  formid
FROM raw_data
GROUP BY grp, formid;

SELECT DISTINCT formid
FROM raw_data;

SELECT DISTINCT grp
FROM raw_data;

SELECT
  grp,
  count(DISTINCT formid)
FROM raw_data
GROUP BY grp;

WITH groups AS (
    SELECT
      grp,
      count(DISTINCT formid) AS form_cnt
    FROM raw_data
    GROUP BY grp
) SELECT sum(form_cnt)
  FROM groups;
