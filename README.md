# Big-Data-with-PySpark
**Exploring the works of William Shakespeare, analyze Fifa 2018 data and perform clustering on genomic datasets.**

___________________________________________________

- Create an RDD named RDD from a list of words.
- Confirm the object created is RDD.

``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

 # Create an RDD from a list of words
RDD = sc.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))
```
______________________________________________

- Use file from this path: /usr/local/share/datasets/README.md
- Create an RDD named fileRDD from a file_path.
- Print the type of the fileRDD created.
- Find the number of partitions that support fileRDD RDD.
- Create an RDD named fileRDD_part from the file path but create 5 partitions.
- Confirm the number of partitions in the new fileRDD_part RDD

``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

file_path='/usr/local/share/datasets/README.md'

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile(file_path, minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())
```
______________________________________________
### Actions

- Create RDD for list numbers from 1 to 10 and assign the result to a variable called numbRDD.
- Create map() transformation that cubes all of the numbers in numbRDD.
- Collect the results in a numbers_all variable.
- Print the output from numbers_all variable.
- Use file from this path: /usr/local/share/datasets/README.md
- Create an RDD named fileRDD from a file_path.
- Create filter() transformation to select the lines containing the keyword Spark.
- How many lines in fileRDD_filter contains the keyword Spark?
- Print the first four lines of the resulting RDD.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

numbRDD=sc.parallelize(range(1,11))

# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x**3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
print(numbers_all)

file_path='/usr/local/share/datasets/README.md'

# Create a fileRDD from file_path
fileRDD = sc.textFile(file_path)

# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4): 
	print(line)

```
______________________________________________
### Transformations in PairRDD

- Create a pair RDD named Rdd with tuples (1,2),(3,4),(3,6),(4,5).
- Transform the Rdd with reduceByKey() into a pair RDD Rdd_Reduced by adding the values with the same key.
- Collect the contents of pair RDD Rdd_Reduced and iterate to print the output.
- Sort the Rdd_Reduced RDD using the key in descending order.
- Collect the contents and iterate to print the output.

``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])
# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x+y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect(): 
  print("Key {} has {} Counts".format(num[0], num[1]))

# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)

# Iterate over the result and retrieve all the elements of the RDD
for num in Rdd_Reduced_Sort.collect():
  print("Key {} has {} Counts".format(num[0], num[1]))
  
```
______________________________________________

- Create a pair RDD named Rdd with tuples (1,2),(3,4),(3,6),(4,5).
- Count the unique keys and assign the result to a variable called total.
- What is the type of total?
- Iterate over the total and print the keys and their counts.

``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create PairRDD Rdd with key value pairs
Rdd = sc.parallelize([(1,2),(3,4),(3,6),(4,5)])

# Count the unique keys
total = Rdd.countByKey()
print(Rdd.collect())

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items(): 
  print("key", k, "has", v, "counts")
  
```
______________________________________________

- Use file from this path: /usr/local/share/datasets/README.md.
- Create an RDD called baseRDD that reads lines from file_path.
- Transform the baseRDD into a long list of words and create a new splitRDD.
- Count the total words in splitRDD.
- Convert the words in splitRDD in lower case and then remove stop words from stop_words curated list.
- Create a pair RDD tuple containing the word and the number 1 from each word element in splitRDD.
- Get the count of the number of occurrences of each word (word frequency) in the pair RDD.
- The Stop words `['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now']`
- Print the first 10 words and their frequencies from the resultRDD RDD.
- Swap the keys and values in the resultRDD.
- Sort the keys according to descending order.
- Print the top 10 most frequent words and their frequencies from the sorted RDD.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

file_path='/usr/local/share/datasets/README.md'

# Create a baseRDD from the file path
baseRDD = sc.textFile(file_path)

# Split the lines of baseRDD into words
splitRDD = baseRDD.flatMap(lambda x: x.split())

# Count the total number of words
print("Total number of words in splitRDD:", splitRDD.count())

stop_words= ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now']

# Convert the words in lower case and remove stop words from the stop_words curated list
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1 
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

# Display the first 10 words and their frequencies from the input RDD
for word in resultRDD.take(10):
	print(word)

# Swap the keys and values from the input RDD
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies from the sorted RDD
for word in resultRDD_swap_sort.take(10):
	print("{},{}". format(word[1], word[0]))

```

______________________________________________
