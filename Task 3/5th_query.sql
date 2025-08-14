SELECT
    a.first_name,
    a.last_name,
    COUNT(f.film_id) AS film_count
FROM
    public.actor AS a
INNER JOIN
    public.film_actor AS fa ON a.actor_id = fa.actor_id
INNER JOIN
    public.film AS f ON fa.film_id = f.film_id
INNER JOIN
    public.film_category AS fc ON f.film_id = fc.film_id
INNER JOIN
    public.category AS c ON fc.category_id = c.category_id
WHERE
    c.name = 'Children'
GROUP BY                
    a.actor_id, a.first_name, a.last_name
ORDER BY
    film_count DESC
LIMIT 3;

