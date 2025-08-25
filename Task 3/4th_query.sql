SELECT
    f.title 
FROM 
    public.film AS f
LEFT JOIN
    public.inventory AS i ON f.film_id = i.film_id
WHERE
    i.inventory_id IS NULL;
	
