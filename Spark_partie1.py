from pyspark import SparkContext, SparkConf

# Create a Spark configuration and set the application name
LEAST_NUMBER_OCCURRENCES = 4

conf = SparkConf().setAppName("MovieAnalysis")
sc = SparkContext(conf=conf)

# Load the data
watched_movies_rdd = sc.textFile("WatchedMovies.txt")

# Filter data for the last five years
def filter_last_five_years(line):
    try:
        # Extract the year from the StartTimestamp
        year = int(line.split(",")[2].split("/")[0])
        # Check if the year is between 2015 and 2020 (both inclusive)
        return 2015 <= year <= 2020
    except:
        return False

filtered_watched_movies_rdd = watched_movies_rdd.filter(filter_last_five_years)

# Count occurrences of each movie during a specific year
def count_occurrences(line):
    try:
        # Extract MID and year
        username, mid, start_timestamp, end_timestamp = line.split(",")
        year = int(start_timestamp.split("/")[0])
        return ((mid, year), 1)
    except:
        return (("ERROR", 0), 1)  # Placeholder for error handling

# Count total occurrences of each movie per year
total_occurrences_rdd = filtered_watched_movies_rdd.map(count_occurrences).reduceByKey(lambda x, y: x + y)

# Filter movies that have been watched more than 4 times in a single year
# and 0 times in any other year
frequent_movies_rdd = total_occurrences_rdd \
    .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .flatMap(lambda x: [(x[0], year) for year, occurrences in x[1] if (occurrences > LEAST_NUMBER_OCCURRENCES and all(other_occurrences == 0 for other_year, other_occurrences in x[1] if other_year != year))])

# Save the results to the specified output folder
frequent_movies_rdd.saveAsTextFile("Output_folder/")

# Stop the Spark context
sc.stop()
