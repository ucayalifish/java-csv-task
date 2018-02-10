DROP TABLE raw_data;

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
      AND ssoid IS NOT NULL;

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

WITH form_counter AS (
    SELECT
      ssoid,
      formid
    FROM raw_data
    WHERE ssoid <> 'Unauthorized'
          AND ssoid IS NOT NULL
          AND formid IS NOT NULL
          AND formid <> 'null'
          AND formid <> ''
    GROUP BY ssoid, formid
    ORDER BY formid
)
SELECT
  formid,
  count(formid) AS fc
FROM form_counter
GROUP BY formid
ORDER BY fc DESC
LIMIT 5;

SELECT
  ssoid,
  formid
FROM raw_data
WHERE ssoid <> 'Unauthorized'
      AND ssoid IS NOT NULL
      AND formid IS NOT NULL
      AND formid <> 'null'
      AND formid <> ''
GROUP BY ssoid, formid
ORDER BY formid;

WITH form_counter AS (
    SELECT
      ssoid,
      formid
    FROM raw_data
    WHERE ssoid NOT LIKE 'Unauthorized%'
          AND ssoid IS NOT NULL
          AND formid IS NOT NULL
          AND formid <> 'null'
    GROUP BY ssoid, formid
    ORDER BY formid
)
SELECT
  ssoid,
  formid
FROM form_counter
GROUP BY ssoid, formid
ORDER BY ssoid;

SELECT ssoid
FROM raw_data
GROUP BY ssoid;
