-- Basket-size stats 
-- 1️⃣  Invoice-level totals
WITH invoice_totals AS (
    SELECT
        c.country,
        s.invoice_key,
        SUM(s.revenue_gbp) AS invoice_revenue
    FROM fct_sales        s
    JOIN dim_customer c ON c.customer_key = s.customer_key
    WHERE s.date_key BETWEEN '2010-12-01' AND '2011-12-09'
      AND c.country IN ('United Kingdom', 'France', 'Netherlands')
    GROUP BY c.country, s.invoice_key
),

-- 2️⃣  2.5 % and 97.5 % cut-offs per country
pct_limits AS (
    SELECT
        country,
        percentile_cont(0.025) WITHIN GROUP (ORDER BY invoice_revenue) AS p_low,
        percentile_cont(0.975) WITHIN GROUP (ORDER BY invoice_revenue) AS p_high
    FROM invoice_totals
    GROUP BY country
)

-- 3️⃣  Final stats
SELECT
    it.country,

    /* mean */
    ROUND(AVG(it.invoice_revenue)::numeric, 2)                               AS mean_basket,

    /* median */
    ROUND(
        percentile_cont(0.5) WITHIN GROUP (ORDER BY it.invoice_revenue)::numeric,
        2
    )                                                                        AS median_basket,

    /* 95 % trimmed mean */
    ROUND(
        (
            AVG(it.invoice_revenue)
            FILTER (WHERE it.invoice_revenue BETWEEN pl.p_low AND pl.p_high)
        )::numeric,
        2
    )                                                                        AS trimmed95_basket,

    COUNT(*)                                                                 AS orders
FROM invoice_totals it
JOIN pct_limits   pl USING (country)
GROUP BY it.country
ORDER BY median_basket DESC;


-- Retail-only basket calculation (cap at £1 000)
WITH invoice_totals AS (
  SELECT c.country, s.invoice_key, SUM(s.revenue_gbp) AS invoice_revenue
  FROM fct_sales s
  JOIN dim_customer c ON c.customer_key = s.customer_key
  WHERE s.date_key BETWEEN '2010-12-01' AND '2011-12-09'
    AND c.country IN ('United Kingdom','France','Netherlands')
  GROUP BY c.country, s.invoice_key
)
SELECT
  country,
  ROUND(AVG(invoice_revenue)::numeric,2) AS mean_all,
  ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY invoice_revenue)::numeric,2) AS median_all,
  ROUND(AVG(invoice_revenue) FILTER (WHERE invoice_revenue<1000)::numeric,2)     AS mean_retail,
  ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY invoice_revenue)
        FILTER (WHERE invoice_revenue<1000)::numeric,2)                          AS median_retail,
  COUNT(*) AS orders
FROM invoice_totals
GROUP BY country
ORDER BY median_retail DESC;



--Quarter-over-Quarter growth 

WITH qtr_rev AS (
    SELECT
        c.country,
        date_trunc('quarter', d.date_key)::date AS qtr_start,
        SUM(s.revenue_gbp)                      AS revenue
    FROM fct_sales        s
    JOIN dim_date     d ON d.date_key = s.date_key
    JOIN dim_customer c ON c.customer_key = s.customer_key
    WHERE d.date_key BETWEEN '2010-12-01' AND '2011-12-09'
      AND c.country IN ('United Kingdom', 'France', 'Netherlands')
    GROUP BY c.country, qtr_start
),

qtr_growth AS (
    SELECT
        country,
        qtr_start,
        revenue,
        LAG(revenue) OVER (PARTITION BY country ORDER BY qtr_start) AS prev_rev,
        ROUND(
            100.0 *
            (revenue - LAG(revenue) OVER (PARTITION BY country ORDER BY qtr_start))
            / NULLIF(LAG(revenue) OVER (PARTITION BY country ORDER BY qtr_start),0)
        ::numeric, 1)                                               AS qoq_growth_pct
    FROM qtr_rev
)

SELECT *
FROM qtr_growth
ORDER BY country, qtr_start;
