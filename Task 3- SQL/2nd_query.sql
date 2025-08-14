SELECT 
    a.first_name,
	a.last_name,
	COUNT(r.rental_id) AS rental_count
FROM
    public.actor AS a
INNER JOIN
    public.film_actor AS fa ON a.actor_id = fa.actor_id
INNER JOIN
    public.film AS f ON fa.film_id = f.film_id
INNER JOIN
    public.inventory AS i ON f.film_id = i.film_id
INNER JOIN
    public.rental AS r ON i.inventory_id = r.inventory_id
GROUP BY
    a.actor_id, a.first_name, a.last_name
ORDER BY
    rental_count DESC
LIMIT 10;

