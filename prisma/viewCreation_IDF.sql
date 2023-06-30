CREATE VIEW idfview AS
SELECT
    t.id,
    t.term,
    LOG10(CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(DISTINCT document) FROM documentterm)) AS IDF
FROM
    terms t
JOIN
    documentterm dt ON t.id = dt.termId
GROUP BY
    t.id, t.term;

