SELECT
    category_id,
    COUNT(*) AS film_id
FROM
    film_category
GROUP BY
    category_id
ORDER BY
    film_id DESC;