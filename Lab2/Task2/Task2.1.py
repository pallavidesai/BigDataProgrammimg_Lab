from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import DoubleType

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

dataFrame  = spark.read.csv("WorldCupMatches.csv", header=True);
dataFrame .show()
print(dataFrame .schema)

# Rename Colomns to get rid of space 

dataFrame  = dataFrame .withColumnRenamed('Home Team Name', 'Home_Team')
dataFrame  = dataFrame .withColumnRenamed('Away Team Name', 'Away_Team')
dataFrame  = dataFrame .withColumnRenamed('Away Team Goals', 'Away_Team_Goals')
dataFrame  = dataFrame .withColumnRenamed('Home Team Goals', 'Home_Team_Goals')
dataFrame  = dataFrame .withColumn('Away_Team_Goals', dataFrame ['Away_Team_Goals'].cast(DoubleType()))
dataFrame  = dataFrame .withColumn('Home_Team_Goals', dataFrame ['Home_Team_Goals'].cast(DoubleType()))
print(dataFrame .schema)

# Filter football matches that held in Pocitos Stadium - Query 1
dataFrame .filter(dataFrame .Stadium.like("Pocitos")).show()

# Display all Group 1 matches - Query 2
dataFrame .filter(dataFrame .Stage == 'Group1').show()


# All the times USA was in Group 1 Stage - Query 3
dataFrame .filter(dataFrame.Stage.like("Group 1") & (dataFrame.Home_Team == 'France')).show()

# Describing mean, max, min of the Home_Team_Goals - Query 4
dataFrame .describe(['Home_Team_Goals']).show()

# Sum of home goals scored and grouped by home team country - Query 5
dataFrame .groupBy('Home_Team').sum('Home_Team_Goals').orderBy('Home_Team')

# Count the total no. of matches played in 2010 - Query 6
print(dataFrame .filter(dataFrame .Year.like("2010")).count())

# pairing stage wise matches with Away_Team - Query 7
dataFrame .crosstab('Stage', 'Away_Team').show()

# Display the max Home goals scored year wise - Query 8
dataFrame .groupBy('Year').max('Home_Team_Goals').show()

# Display the Min Home goals scored in each Stadium - Query 9
dataFrame .groupBy('Home_Team').min('Home_Team_Goals').show()

# Sum of home goals scored in each phase stage - Query 10
dataFrame .groupBy('Stage').sum('Home_Team_Goals').orderBy('Stage')




