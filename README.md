# Using HashSet based indexes in Apache Spark

An Apache Spark based solution to partition data and de-duplicate it while incrementally loading to a table.

The code is described further along with the problem statement in a lot more detail in this blog post [here](https://anish749.github.io/spark/2017/05/13/using-hashset-based-indexes-apache-spark.html).


The problem statement

A table exists in Hive or any destination system and data is loaded every hour (or day) to this table. As per the source systems, it can generate duplicate data. However the final table should de-duplicate the data while loading. We assume that data is immutable, but can be delivered more than once, and we need a logic to filter these duplicates before appending the data to our master store.

_Assumption:_ We have the data partitioned and data doesn’t get repeated across partition. This just makes the problem a simpler to optimize as it would help in reducing shuffles. For other use cases we can very well consider the whole data to be in one partition.

_Sample use case:_ I used this on click stream data with an at least once delivery guarantee. However since the data is immutable, the source timestamp doesn’t change, and I partition the data based on this source timestamp.

Please visit my blog for further understanding of this project