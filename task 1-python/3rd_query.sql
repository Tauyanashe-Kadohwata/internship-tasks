SELECT r.name, MAX(s.birthday) - MIN(s.birthday) AS age_diff
FROM rooms r
JOIN students s ON r.id = s.room_id
GROUP BY r.id, r.name
ORDER BY age_diff DESC
LIMIT 5;