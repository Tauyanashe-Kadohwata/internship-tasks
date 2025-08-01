(
    SELECT
        c.name AS category_name,
        'City starts with "A"' AS city_filter_type,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_rental_hours
    FROM
        public.category AS c
    INNER JOIN
        public.film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN
        public.film AS f ON fc.film_id = f.film_id
    INNER JOIN
        public.inventory AS i ON f.film_id = i.film_id
    INNER JOIN
        public.rental AS r ON i.inventory_id = r.inventory_id
    INNER JOIN
        public.customer AS cust ON r.customer_id = cust.customer_id
    INNER JOIN
        public.address AS a ON cust.address_id = a.address_id
    INNER JOIN
        public.city AS ct ON a.city_id = ct.city_id
    WHERE
        ct.city LIKE 'A%' -- City name starts with 'A'
    GROUP BY
        c.category_id, c.name
    ORDER BY
        total_rental_hours DESC
    LIMIT 1
)

UNION ALL

(
    SELECT
        c.name AS category_name,
        'City contains "-"' AS city_filter_type,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_rental_hours
    FROM
        public.category AS c
    INNER JOIN
        public.film_category AS fc ON c.category_id = fc.category_id
    INNER JOIN
        public.film AS f ON fc.film_id = f.film_id
    INNER JOIN
        public.inventory AS i ON f.film_id = i.film_id
    INNER JOIN
        public.rental AS r ON i.inventory_id = r.inventory_id
    INNER JOIN
        public.customer AS cust ON r.customer_id = cust.customer_id
    INNER JOIN
        public.address AS a ON cust.address_id = a.address_id
    INNER JOIN
        public.city AS ct ON a.city_id = ct.city_id
    WHERE
        ct.city LIKE '%-%' -- City name contains '-'
    GROUP BY
        c.category_id, c.name
    ORDER BY
        total_rental_hours DESC
    LIMIT 1
);
