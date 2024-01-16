from pyspark import SparkContext
from datetime import datetime

# Initialize a Spark context
spark_context = SparkContext(appName="MostWatchedMoviesByYear")

# Load the data from the file as an RDD
data_file_path = "C:\\Users\\Hp\\Desktop\\WatchedMovies.txt"
data_rdd = spark_context.textFile(data_file_path)

# Define a function to parse each line and convert it to a tuple
def parse_line(line):
    # Split the line into fields
    fields = line.split(',')
    # Extract relevant information
    username, movie_id, start_timestamp, end_timestamp = fields
    start_date = datetime.strptime(start_timestamp, '%Y/%m/%d_%H:%M').date()
    end_date = datetime.strptime(end_timestamp, '%Y/%m/%d_%H:%M').date()
    return username, movie_id, start_date, end_date

# Apply the parse_line function to the RDD
parsed_data_rdd = data_rdd.map(parse_line)

# Use distinct to consider only distinct users for each movie in each year
distinct_data_rdd = parsed_data_rdd.map(lambda x: ((x[1], x[2].year, x[0]), 1)).distinct()

# Map each row to a key-value pair where the key is (MovieID, year) and the value is 1
key_value_data_rdd = distinct_data_rdd.map(lambda x: ((x[0][0], x[0][1]), 1))

# Reduce by key to count the occurrences
movie_year_counts_rdd = key_value_data_rdd.reduceByKey(lambda a, b: a + b)

# Map to a structure where the key is the year and the value is a list of tuples (MovieID, count)
year_movie_counts_rdd = movie_year_counts_rdd.map(lambda x: (x[0][1], [(x[0][0], x[1])]))

# Reduce by key to find the most popular movie in each year
most_popular_movies_rdd = year_movie_counts_rdd.reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], sorted(x[1], key=lambda y: y[1], reverse=True)))

# Rest of the code...
# Collect the data to the driver program
most_popular_movies_data = most_popular_movies_rdd.collect()

# Create a dictionary to store max views by year
max_views_by_year = {}

# Iterate over the collected data
for year, movie_views_list in most_popular_movies_data:
    max_views = 0
    max_views_movies = []

    # Iterate over movies and views in each year
    for movie, views in movie_views_list:
        if views > max_views:
            max_views = views
            max_views_movies = [movie]
        elif views == max_views:
            max_views_movies.append(movie)

    # Store the result in the dictionary
    max_views_by_year[year] = (max_views_movies, max_views)

# Print the result
print(max_views_by_year)

# Function to save movies occurring in multiple years
def save_movies_in_multiple_years(data):
    movie_occurrences = {}

    # Iterate over years and movies
    for year, (movies, _) in max_views_by_year.items():
        for movie in movies:
            if movie in movie_occurrences:
                movie_occurrences[movie].append(year)
            else:
                movie_occurrences[movie] = [year]

    # Filter movies that occur in at least two years
    movies_in_multiple_years = [movie for movie, years in movie_occurrences.items() if len(years) >= 2]

    return movies_in_multiple_years

# Call the function and print the result
result = save_movies_in_multiple_years(max_views_by_year)
print("Movies that occur in at least two years:", result, type(result))

# Use parallelize to create an RDD from the result
result_rdd = spark_context.parallelize(result)

# Assuming result_rdd is your RDD
coalesced_rdd = result_rdd.coalesce(1)

# Save the coalesced RDD as a text file
coalesced_rdd.saveAsTextFile('C:\\Users\\Hp\\Desktop\\task2_outatik.txt')

# Stop the Spark context
spark_context.stop()