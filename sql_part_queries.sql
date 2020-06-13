-- query 1)
SELECT
    format('%s(%s)', name, left(occupation, 1)) output
FROM occupations
ORDER BY name;

-- query 2) v1
SELECT
    format('There are a total of %s %ss.', t.countOccupation, lower(t.occupation)) output
FROM (
    SELECT
        occupation,
        count(*) countOccupation
    FROM occupations
    GROUP BY occupation
    ORDER BY
        countOccupation,
        occupation
 ) t;

-- query 2) v2
SELECT
    format('There are a total of %s %ss.', count(*), lower(occupation)) output
FROM
    occupations
GROUP BY
    occupation
ORDER BY
    count(*),
    occupation;
