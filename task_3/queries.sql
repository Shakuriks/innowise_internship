--task 1
SELECT c.name AS category_name, COUNT(film_category.film_id) AS film_count
FROM category c
JOIN film_category ON c.category_id = film_category.category_id
GROUP BY c.name
ORDER BY film_count DESC;

--task 2
SELECT a.actor_id, a.first_name, a.lASt_name, COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN inventory i ON fa.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id, a.first_name, a.lASt_name
ORDER BY rental_count DESC
LIMIT 10;

--task 3
SELECT c.name AS category_name, SUM(p.amount) AS total_amount
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN inventory i ON fc.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.name
ORDER BY total_amount DESC
LIMIT 1;

--task 4
SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id is NULL;

--task 5
WITH actor_film_count AS (
    SELECT a.actor_id, a.first_name, a.lASt_name, COUNT(f.film_id) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.lASt_name
),
top_actors AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY film_count DESC) AS rank
    FROM actor_film_count
)
SELECT actor_id, first_name, lASt_name, film_count
FROM top_actors
WHERE rank <= 3
ORDER BY film_count DESC;

--task 6
SELECT c.city AS city_name, sum(cu.active) AS active_customers_count, (count(*) - sum(cu.active)) AS inactive_customers_count
FROM customer cu
INNER JOIN address a ON cu.address_id = a.address_id
INNER JOIN city c ON a.city_id = c.city_id
GROUP BY c.city_id, c.city
ORDER BY inactive_customers_count DESC;

--task 7

-- (
--     SELECT 
--         ca.name AS category_name,
--         SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date))) AS category_summary_rental_time
--     FROM category ca
--     INNER JOIN film_category fc ON ca.category_id = fc.category_id
--     INNER JOIN inventory i ON fc.film_id = i.film_id
--     INNER JOIN rental r ON i.inventory_id = r.inventory_id
--     INNER JOIN customer cu ON r.customer_id = cu.customer_id
--     INNER JOIN address a ON cu.address_id = a.address_id
--     INNER JOIN city c ON a.city_id = c.city_id
--     WHERE r.rental_date IS NOT NULL 
--       AND r.return_date IS NOT NULL
--       AND c.city LIKE 'A%'
--     GROUP BY ca.category_id, ca.name
--     ORDER BY category_summary_rental_time DESC
--     LIMIT 1
-- )

-- UNION ALL

-- (
--     SELECT 
--         ca.name AS category_name,
--         SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date))) AS category_summary_rental_time
--     FROM category ca
--     INNER JOIN film_category fc ON ca.category_id = fc.category_id
--     INNER JOIN inventory i ON fc.film_id = i.film_id
--     INNER JOIN rental r ON i.inventory_id = r.inventory_id
--     INNER JOIN customer cu ON r.customer_id = cu.customer_id
--     INNER JOIN address a ON cu.address_id = a.address_id
--     INNER JOIN city c ON a.city_id = c.city_id
--     WHERE r.rental_date IS NOT NULL 
--       AND r.return_date IS NOT NULL
--       AND c.city LIKE '%-%'
--     GROUP BY ca.category_id, ca.name
--     ORDER BY category_summary_rental_time DESC
--     LIMIT 1
-- );

WITH category_summary_rental_time_filtered AS (
    SELECT 
        ca.name AS category_name,
        CASE
            WHEN c.city LIKE 'A%' THEN 'A%'
            WHEN c.city LIKE '%-%' THEN '%-%'
        END AS city_pattern,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date))) AS category_summary_rental_time
    FROM category ca
    INNER JOIN film_category fc ON ca.category_id = fc.category_id
    INNER JOIN inventory i ON fc.film_id = i.film_id
    INNER JOIN rental r ON i.inventory_id = r.inventory_id
    INNER JOIN customer cu ON r.customer_id = cu.customer_id
    INNER JOIN address a ON cu.address_id = a.address_id
    INNER JOIN city c ON a.city_id = c.city_id
    WHERE r.rental_date IS NOT NULL 
      AND r.return_date IS NOT NULL
      AND (c.city LIKE 'A%' OR c.city LIKE '%-%')
    GROUP BY ca.category_id, ca.name, city_pattern
)
SELECT 
    category_name,
    category_summary_rental_time
FROM category_summary_rental_time_filtered
WHERE city_pattern = 'A%'
ORDER BY category_summary_rental_time DESC
LIMIT 1

UNION ALL

SELECT 
    category_name,
    category_summary_rental_time
FROM category_summary_rental_time_filtered
WHERE city_pattern = '%-%'
ORDER BY category_summary_rental_time DESC
LIMIT 1;



