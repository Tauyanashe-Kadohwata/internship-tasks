SELECT
    c.name AS category_name,
    SUM(p.amount) AS total_revenue
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
    public.payment AS p ON r.rental_id = p.rental_id
GROUP BY
    c.category_id, c.name 
ORDER BY
    total_revenue DESC
LIMIT 1;
