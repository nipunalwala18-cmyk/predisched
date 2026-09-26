-- Data for DB_QUERY_TASK (spec §7.2): 200,000 seeded rows. The values come from a fixed formula,
-- not random(), so every database built from these migrations holds the same data.
CREATE TABLE db_query_data (
    id        int              PRIMARY KEY,
    category  int              NOT NULL,
    value     double precision NOT NULL
);

INSERT INTO db_query_data (id, category, value)
SELECT g, g % 16, ((g * 7919) % 10007) / 100.0
FROM generate_series(1, 200000) AS g;

ANALYZE db_query_data;
