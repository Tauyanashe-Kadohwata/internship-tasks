SELECT
    c.city,
    SUM(CASE WHEN cust.active = 1 THEN 1 ELSE 0 END) AS active_customer_count,
    SUM(CASE WHEN cust.active = 0 THEN 1 ELSE 0 END) AS inactive_customer_count
FROM
    public.city AS c
INNER JOIN
    public.address AS a ON c.city_id = a.city_id
INNER JOIN
    public.customer AS cust ON a.address_id = cust.address_id
GROUP BY
    c.city_id, c.city
ORDER BY
    inactive_customer_count DESC;
