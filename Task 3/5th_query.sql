WITH ActorRanks AS (
    SELECT
        a.first_name,
        a.last_name,
        COUNT(f.film_id) AS film_count,
        DENSE_RANK() OVER (ORDER BY COUNT(f.film_id) DESC) AS ranking
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
)
SELECT
    first_name,
    last_name,
    film_count
FROM
    ActorRanks
WHERE
    ranking <= 3
ORDER BY
    film_count DESC;


