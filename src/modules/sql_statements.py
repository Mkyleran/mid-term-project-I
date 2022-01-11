# [How to derive summary statistics using PostgreSQL](https://towardsdatascience.com/how-to-derive-summary-statistics-using-postgresql-742f3cdc0f44)

# This is a good use case for sql composition

# numeric discriptive statistics
# count, mean, standard deviation, variance, range, minimum, Q1/25%, median/Q2/50%, Q3/75%, maximum, interquartile range (IQR), skewness
numerical_statistics_sql = """
WITH RECURSIVE
summary_stats AS
(
 SELECT
  COUNT(dep_delay) AS count,
  ROUND(AVG(dep_delay)::NUMERIC, 2) AS mean,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dep_delay) AS median,
  MIN(dep_delay) AS min,
  MAX(dep_delay) AS max,
  MAX(dep_delay) - MIN(dep_delay) AS range,
  ROUND(STDDEV(dep_delay)::NUMERIC, 2) AS standard_deviation,
  ROUND(VARIANCE(dep_delay)::NUMERIC, 2) AS variance,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dep_delay) AS q1,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dep_delay) AS q3
   FROM flights
),
row_summary_stats AS
(
SELECT
 1 as sno,
 'count' AS statistic,
  count AS value
  FROM summary_stats
UNION
SELECT
 2,
 'mean',
 mean 
  FROM summary_stats
UNION
SELECT
 3,
 'standard deviation', 
 standard_deviation 
  FROM summary_stats
UNION
SELECT
 4,
 'variance', 
 variance 
  FROM summary_stats
UNION
SELECT 
 5,
 'range', 
 range 
  FROM summary_stats
UNION
SELECT 
 6,
 'minimum', 
 min 
  FROM summary_stats
UNION
SELECT 
 7,
 'Q1 (25%)', 
 q1 
  FROM summary_stats
UNION
SELECT 
 8,
 'median (50%)', 
 median 
  FROM summary_stats
UNION
SELECT
 9,
 'Q3 (75%)', 
 q3 
  FROM summary_stats
UNION
SELECT
 10,
 'maximum', 
 max 
  FROM summary_stats
UNION
SELECT
 11,
 'IQR', 
 (q3 - q1) 
  FROM summary_stats
UNION
SELECT
 12,
 'skewness', 
 ROUND(3 * (mean - median)::NUMERIC / standard_deviation, 2) AS skewness 
  FROM summary_stats
)
SELECT * 
 FROM row_summary_stats
  ORDER BY sno;
"""

# Category frequency and relative frequency
categorical_statistics_sql = """
SELECT 
 mkt_unique_carrier,
 COUNT(mkt_unique_carrier) AS frequency,
 ROUND(COUNT(mkt_unique_carrier)::NUMERIC / SUM(COUNT(mkt_unique_carrier)) OVER(), 4) AS relative_frequency
    FROM flights
     GROUP BY mkt_unique_carrier
      ORDER BY frequency DESC
"""

num_stats_sql = """
 SELECT
  COUNT({feat}) AS count,
  AVG({feat}) AS mean,
  STDDEV({feat}) AS standard_deviation,
  VARIANCE({feat}) AS variance,
  MIN({feat}) AS min,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {feat}) AS q1,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {feat}) AS median,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {feat}) AS q3,
  MAX({feat}) AS max
   FROM {table}
  LIMIT 10;
"""

cat_stats_sql = """
SELECT 
 {feature},
 COUNT({feature}) AS frequency
FROM {table}
GROUP BY {feature}
ORDER BY frequency DESC
"""

if __name__ == '__main__':
    pass