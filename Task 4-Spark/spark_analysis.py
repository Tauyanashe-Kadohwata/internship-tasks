from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, desc, when, lit, array_contains, datediff, hour, minute, second
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SakilaPySparkAnalysis") \
    .getOrCreate()

# --- Simulate Demo Database Tables ---
# Using sample data to represent the database structure

# Actor DataFrame
actor_data = [
    (1, "PENELOPE", "GUINESS"), (2, "NICK", "WAHLBERG"), (3, "ED", "CHASE"),
    (4, "JENNIFER", "DAVIS"), (5, "JOHNNY", "LOLLOBRIGIDA"), (6, "JOE", "SWANK"),
    (7, "JON", "FAWCETT"), (8, "ANGELA", "HUDSON"), (9, "KIRSTEN", "PALTROW"),
    (10, "BETTE", "NICHOLSON"), (11, "GRACE", "KELLY"), (12, "JOHN", "TRAVOLTA"),
    (13, "WOODY", "ALLEN"), (14, "SANDRA", "BULLOCK"), (15, "TOM", "HANKS"),
    (16, "MERYL", "STREEP"), (17, "LEONARDO", "DICAPRIO"), (18, "JULIA", "ROBERTS")
]
actor_df = spark.createDataFrame(actor_data, ["actor_id", "first_name", "last_name"])

# Category DataFrame
category_data = [
    (1, "Action"), (2, "Animation"), (3, "Children"), (4, "Classics"), (5, "Comedy"),
    (6, "Documentary"), (7, "Drama"), (8, "Family"), (9, "Foreign"), (10, "Games"),
    (11, "Horror"), (12, "Music"), (13, "New"), (14, "Sci-Fi"), (15, "Sports"), (16, "Travel")
]
category_df = spark.createDataFrame(category_data, ["category_id", "name"])

# Film DataFrame
film_data = [
    (101, "MOVIE A", 3, 4.99, 90, 19.99), (102, "MOVIE B", 7, 2.99, 120, 24.99),
    (103, "MOVIE C", 5, 0.99, 150, 9.99), (104, "MOVIE D", 3, 1.99, 85, 12.99),
    (105, "MOVIE E", 6, 3.99, 110, 15.99), (106, "MOVIE F", 4, 2.99, 100, 17.99),
    (107, "MOVIE G", 3, 4.99, 95, 21.99), (108, "MOVIE H", 7, 0.99, 130, 8.99),
    (109, "MOVIE I", 5, 1.99, 140, 10.99), (110, "MOVIE J", 3, 2.99, 75, 13.99),
    (111, "MOVIE K", 6, 3.99, 105, 16.99), (112, "MOVIE L", 4, 4.99, 115, 20.99),
    (113, "MOVIE M", 3, 0.99, 88, 7.99), (114, "MOVIE N", 7, 1.99, 160, 11.99),
    (115, "MOVIE O", 5, 2.99, 125, 14.99), (116, "MOVIE P", 3, 3.99, 92, 18.99),
    (117, "MOVIE Q", 6, 4.99, 108, 22.99), (118, "MOVIE R", 4, 0.99, 135, 6.99)
]
film_df = spark.createDataFrame(film_data, ["film_id", "title", "rental_duration", "rental_rate", "length", "replacement_cost"])

# Film_Actor DataFrame
film_actor_data = [
    (1, 101), (2, 101), (3, 102), (4, 103), (1, 104), (5, 104), (6, 105), (7, 106),
    (8, 107), (9, 108), (10, 109), (11, 110), (12, 111), (13, 112), (14, 113),
    (15, 114), (16, 115), (17, 116), (18, 117), (1, 118), (3, 103), (5, 105),
    (1, 102), (2, 103), (3, 104), (4, 105), (5, 106), (6, 107), (7, 108), (8, 109),
    (9, 110), (10, 111), (11, 112), (12, 113), (13, 114), (14, 115), (15, 116),
    (16, 117), (17, 118), (18, 101), (1, 103) # Additional entries for 'Children' category for actor 1
]
film_actor_df = spark.createDataFrame(film_actor_data, ["actor_id", "film_id"])

# Film_Category DataFrame
film_category_data = [
    (101, 1), (102, 3), (103, 5), (104, 3), (105, 7), (106, 2), (107, 4), (108, 6),
    (109, 8), (110, 10), (111, 12), (112, 14), (113, 16), (114, 1), (115, 3),
    (116, 5), (117, 7), (118, 9), (103, 3) # MOVIE C is also Children for Actor 1 in Children test
]
film_category_df = spark.createDataFrame(film_category_data, ["film_id", "category_id"])

# Inventory DataFrame (some movies not in inventory for a later task)
inventory_data = [
    (1, 101, 1), (2, 102, 1), (3, 103, 1), (4, 104, 2), (5, 105, 2),
    (6, 101, 1), (7, 102, 1), (8, 103, 1), (9, 104, 2), (10, 105, 2),
    (11, 106, 1), (12, 107, 1), (13, 108, 1), (14, 109, 2), (15, 110, 2),
    (16, 111, 1), (17, 112, 1), (18, 113, 1) # Note: 114, 115, 116, 117, 118 are not in inventory
]
inventory_df = spark.createDataFrame(inventory_data, ["inventory_id", "film_id", "store_id"])

# Customer DataFrame
customer_data = [
    (1, 1, "MARY", "SMITH", "mary.smith@example.com", 1, 1),
    (2, 1, "PATRICIA", "JOHNSON", "patricia.johnson@example.com", 2, 1),
    (3, 1, "LINDA", "WILLIAMS", "linda.williams@example.com", 3, 0),
    (4, 2, "BARBARA", "JONES", "barbara.jones@example.com", 4, 1),
    (5, 2, "ELIZABETH", "BROWN", "elizabeth.brown@example.com", 5, 0),
    (6, 1, "JENNIFER", "DAVIS", "jennifer.davis@example.com", 6, 1),
    (7, 2, "MARIA", "MILLER", "maria.miller@example.com", 7, 0),
    (8, 1, "JOHN", "DOE", "john.doe@example.com", 8, 1),
    (9, 2, "JANE", "SMITH", "jane.smith@example.com", 9, 0)
]
customer_df = spark.createDataFrame(customer_data, ["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "active"])

# Address DataFrame
address_data = [
    (1, "123 Main St", "California", 1), (2, "456 Oak Ave", "Texas", 2),
    (3, "789 Pine Ln", "Florida", 3), (4, "101 Maple Dr", "California", 1),
    (5, "202 Birch Rd", "New York", 4), (6, "303 Cedar Blvd", "California", 1),
    (7, "404 Elm Pk", "Texas", 2), (8, "505 Willow Way", "Florida", 3),
    (9, "606 Aspen Ct", "New-York", 4) # City with hyphen
]
address_df = spark.createDataFrame(address_data, ["address_id", "address", "district", "city_id"])

# City DataFrame
city_data = [
    (1, "A-City"), (2, "Another-Town"), (3, "B-City"), (4, "New York")
]
city_df = spark.createDataFrame(city_data, ["city_id", "city"])

# Rental DataFrame (rental_date and return_date are strings for simplicity, converted to timestamp later)
rental_data = [
    (1, "2023-01-01 10:00:00", 1, 1, "2023-01-03 12:00:00"), # MOVIE A (Film_id 101)
    (2, "2023-01-02 11:00:00", 2, 2, "2023-01-05 15:00:00"), # MOVIE B (Film_id 102)
    (3, "2023-01-03 09:00:00", 3, 3, "2023-01-07 10:00:00"), # MOVIE C (Film_id 103)
    (4, "2023-01-04 14:00:00", 4, 4, "2023-01-06 18:00:00"), # MOVIE D (Film_id 104)
    (5, "2023-01-05 16:00:00", 5, 5, "2023-01-08 20:00:00"), # MOVIE E (Film_id 105)
    (6, "2023-01-06 10:30:00", 6, 1, "2023-01-09 11:30:00"), # MOVIE A (Film_id 101)
    (7, "2023-01-07 12:45:00", 7, 2, "2023-01-11 13:45:00"), # MOVIE B (Film_id 102)
    (8, "2023-01-08 08:00:00", 8, 3, "2023-01-10 09:00:00"), # MOVIE C (Film_id 103)
    (9, "2023-01-09 13:00:00", 9, 4, "2023-01-12 14:00:00"), # MOVIE D (Film_id 104)
    (10, "2023-01-10 15:00:00", 10, 5, "2023-01-13 16:00:00"),# MOVIE E (Film_id 105)
    (11, "2023-01-11 09:00:00", 11, 6, "2023-01-14 10:00:00"),# MOVIE F (Film_id 106)
    (12, "2023-01-12 11:00:00", 12, 7, "2023-01-15 12:00:00"),# MOVIE G (Film_id 107)
    (13, "2023-01-13 13:00:00", 13, 8, "2023-01-16 14:00:00"),# MOVIE H (Film_id 108)
    (14, "2023-01-14 15:00:00", 14, 9, "2023-01-17 16:00:00") # MOVIE I (Film_id 109)
]
rental_df = spark.createDataFrame(rental_data, ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date"]) \
    .withColumn("rental_date", col("rental_date").cast("timestamp")) \
    .withColumn("return_date", col("return_date").cast("timestamp"))

# Payment DataFrame
payment_data = [
    (1, 1, 1, 4.99, "2023-01-03 13:00:00"), (2, 2, 2, 2.99, "2023-01-05 16:00:00"),
    (3, 3, 3, 0.99, "2023-01-07 11:00:00"), (4, 4, 4, 1.99, "2023-01-06 19:00:00"),
    (5, 5, 5, 3.99, "2023-01-08 21:00:00"), (6, 1, 6, 4.99, "2023-01-09 12:30:00"),
    (7, 2, 7, 2.99, "2023-01-11 14:45:00"), (8, 3, 8, 0.99, "2023-01-10 10:00:00"),
    (9, 4, 9, 1.99, "2023-01-12 15:00:00"), (10, 5, 10, 3.99, "2023-01-13 17:00:00"),
    (11, 6, 11, 2.99, "2023-01-14 11:00:00"), (12, 7, 12, 4.99, "2023-01-15 13:00:00"),
    (13, 8, 13, 0.99, "2023-01-16 15:00:00"), (14, 9, 14, 1.99, "2023-01-17 17:00:00")
]
payment_df = spark.createDataFrame(payment_data, ["payment_id", "customer_id", "rental_id", "amount", "payment_date"])


# --- Problem Solutions ---

print("--- 1. Number of movies in each category, sorted in descending order ---")
category_movie_counts = film_category_df.join(category_df, "category_id") \
    .groupBy("name") \
    .agg(count("film_id").alias("movie_count")) \
    .sort(col("movie_count").desc())
category_movie_counts.show()

print("\n--- 2. The 10 actors whose movies rented the most, sorted in descending order ---")
actor_rental_counts = rental_df.join(inventory_df, "inventory_id") \
    .join(film_actor_df, "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("rental_id").alias("rental_count")) \
    .sort(col("rental_count").desc()) \
    .limit(10)
actor_rental_counts.show()

print("\n--- 3. Category of movies on which the most money was spent ---")
most_money_category = payment_df.join(rental_df, "rental_id") \
    .join(inventory_df, "inventory_id") \
    .join(film_category_df, "film_id") \
    .join(category_df, "category_id") \
    .groupBy("name") \
    .agg(sum("amount").alias("total_spent")) \
    .sort(col("total_spent").desc()) \
    .limit(1)
most_money_category.show()

print("\n--- 4. Names of movies that are not in the inventory ---")
# Get all film_ids that ARE in the inventory
films_in_inventory = inventory_df.select("film_id").distinct()
# Find films not in this list
movies_not_in_inventory = film_df.alias("f") \
    .join(films_in_inventory.alias("i"), col("f.film_id") == col("i.film_id"), "left_anti") \
    .select("title")
movies_not_in_inventory.show()

print("\n--- 5. Top 3 actors who have appeared most in movies in the “Children” category (all ties included) ---")
children_category_id = category_df.filter(col("name") == "Children").select("category_id").collect()[0][0]

children_movies_actors = film_category_df.filter(col("category_id") == children_category_id) \
    .join(film_actor_df, "film_id") \
    .join(actor_df, "actor_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("film_id").alias("movie_count")) \
    .withColumn("rank", col("movie_count").cast("long")) # Cast to long for window function tie handling

# Use a window function to get rank for ties
window_spec = Window.orderBy(col("movie_count").desc())
top_children_actors = children_movies_actors.withColumn("rank",
                                                        when(col("movie_count") == lit(0), lit(None))
                                                        .otherwise(col("movie_count"))) \
    .orderBy(col("movie_count").desc())

# Filter for top 3 counts. Collect the top 3 counts first
top_3_counts = top_children_actors.select("movie_count").distinct().orderBy(col("movie_count").desc()).limit(3)
top_3_counts_list = [row.movie_count for row in top_3_counts.collect()]

# Filter original DataFrame for actors whose movie_count is in the top 3 counts list
final_top_children_actors = top_children_actors.filter(col("movie_count").isin(top_3_counts_list)) \
    .select("first_name", "last_name", "movie_count") \
    .orderBy(col("movie_count").desc())

final_top_children_actors.show()


print("\n--- 6. Cities with the number of active and inactive customers (sort by inactive desc) ---")
city_customer_status = customer_df.join(address_df, "address_id") \
    .join(city_df, "city_id") \
    .groupBy("city") \
    .agg(
        sum(when(col("active") == 1, 1).otherwise(0)).alias("active_customers"),
        sum(when(col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
    ) \
    .sort(col("inactive_customers").desc())
city_customer_status.show()

print("\n--- 7. Category of movies with highest total rental hours in cities starting with 'a' and cities with '-' symbol ---")

# Calculate rental duration in hours
rental_duration_df = rental_df.withColumn(
    "rental_duration_hours",
    (col("return_date").cast("long") - col("rental_date").cast("long")) / 3600
)

# Join all necessary tables for city, category, and rental duration
rental_city_category_df = rental_duration_df.join(inventory_df, "inventory_id") \
    .join(film_df, "film_id") \
    .join(film_category_df, "film_id") \
    .join(category_df, "category_id") \
    .join(customer_df, "customer_id") \
    .join(address_df, "address_id") \
    .join(city_df, "city_id") \
    .select(
        col("city"),
        col("name").alias("category_name"),
        col("rental_duration_hours")
    )

# --- For cities starting with 'a' ---
cities_starting_with_a = rental_city_category_df.filter(col("city").startswith("A"))

total_rental_hours_a_cities = cities_starting_with_a.groupBy("category_name") \
    .agg(sum("rental_duration_hours").alias("total_hours")) \
    .sort(col("total_hours").desc()) \
    .limit(1)

print("\nCategory with highest total rental hours in cities starting with 'A':")
total_rental_hours_a_cities.show()

# --- For cities with a '-' symbol ---
cities_with_hyphen = rental_city_category_df.filter(col("city").contains("-"))

total_rental_hours_hyphen_cities = cities_with_hyphen.groupBy("category_name") \
    .agg(sum("rental_duration_hours").alias("total_hours")) \
    .sort(col("total_hours").desc()) \
    .limit(1)

print("\nCategory with highest total rental hours in cities containing a '-':")
total_rental_hours_hyphen_cities.show()

# Stop Spark Session
spark.stop()
