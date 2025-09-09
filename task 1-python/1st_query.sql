SELECT r.name, COUNT(s.id) AS student_count
FROM rooms r
LEFT JOIN students s ON r.id = s.room_id
GROUP BY r.id, r.name;