
##Song recommendation


# libraries required

#import sys
#from pyspark import SparkContext
#from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#from pyspark.sql.functions import *
#from pyspark.sql.functions import countDistinct
#from pyspark.sql import SparkSession, functions as F

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

#spark = SparkSession.builder.getOrCreate()
#sc = SparkContext.getOrCreate()

# Compute suitable number of partitions

#conf = sc.getConf()

#N = int(conf.get("spark.executor.instances"))
#M = int(conf.get("spark.executor.cores"))
#partitions = 4 * N * M

##define schema
#mismatches_schema = StructType([
#    StructField("song_id", StringType(), True),
#    StructField("song_artist", StringType(), True),
#    StructField("song_title", StringType(), True),
#    StructField("track_id", StringType(), True),
#    StructField("track_artist", StringType(), True),
#    StructField("track_title", StringType(), True)
#])

#with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
#    lines = f.readlines()
#    sid_matches_manually_accepted = []
#    for line in lines:
#        if line.startswith("< ERROR: "):
#            a = line[10:28]
#            b = line[29:47]
#            c, d = line[49:-1].split("  !=  ")
#            e, f = c.split("  -  ")
#            g, h = d.split("  -  ")
#            sid_matches_manually_accepted.append((a, e, f, b, g, h))

#matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
#matches_manually_accepted.cache()
#matches_manually_accepted.show(10, 20)

#print(matches_manually_accepted.count())  # 488

#with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
#    lines = f.readlines()
#    sid_mismatches = []
#    for line in lines:
#        if line.startswith("ERROR: "):
#            a = line[8:26]
#            b = line[27:45]
#            c, d = line[47:-1].split("  !=  ")
#            e, f = c.split("  -  ")
#            g, h = d.split("  -  ")
#            sid_mismatches.append((a, e, f, b, g, h))

#mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
#mismatches.cache()
#mismatches.show(10, 20)

#print(mismatches.count())  # 19094

#triplets_schema = StructType([
#    StructField("user_id", StringType(), True),
#    StructField("song_id", StringType(), True),
#    StructField("plays", IntegerType(), True)
#])
#triplets = (
#    spark.read.format("csv")
#    .option("header", "false")
#    .option("delimiter", "\t")
#    .option("codec", "gzip")
#    .schema(triplets_schema)
#    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
#    .cache()
#)
#triplets.cache()
#triplets.show(10, 50)

#mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
#triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

#triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

print(mismatches_not_accepted.count())  # 19093
print(triplets.count())                 # 48373586
print(triplets_not_mismatched.count())  # 45795111



##unique songs in taste profile dataset
unique_song_count = triplets_not_mismatched.agg(countDistinct("song_id")).alias('unique_songs')
unique_song_count.show()

#+-----------------------+
#|count(DISTINCT song_id)|
#+-----------------------+
#|                 378310|
#+-----------------------+

##unique users
unique_user_count = triplets_not_mismatched.agg(countDistinct("user_id")).alias('unique_users')
unique_user_count.show()

#+-----------------------+
#|count(DISTINCT user_id)|
#+-----------------------+
#|                1019318|
#+-----------------------+


##registering as temp table for pyspark sql computation
triplets_not_mismatched.registerTempTable('user_song_interaction')
user_interaction = spark.sql("""
		SELECT user_id, sum(plays) AS playcounts
		FROM user_song_interaction
		GROUP BY 1
        ORDER BY 2 DESC
	"""
	)
user_interaction.show(10,False) ##implicit feedback

#+----------------------------------------+----------+
#|user_id                                 |playcounts|
#+----------------------------------------+----------+
#|093cb74eb3c517c5179ae24caf0ebec51b24d2a2|13074     |
#|119b7c88d58d0c6eb051365c103da5caf817bea6|9104      |
#|3fa44653315697f42410a30cb766a4eb102080bb|8025      |
#|a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|6506      |
#|d7d2d888ae04d16e994d6964214a1de81392ee04|6190      |
#|4ae01afa8f2430ea0704d502bc7b57fb52164882|6153      |
#|b7c24f770be6b802805ac0e2106624a517643c17|5827      |
#|113255a012b2affeab62607563d03fbdf31b08e7|5471      |
#|99ac3d883681e21ea68071019dba828ce76fe94d|5385      |
#|6d625c6557df84b60d90426c0116138b617b9449|5362      |
#+----------------------------------------+----------+
#only showing top 10 rows

##most active user 
most_active_user = spark.sql("""
	SELECT 
	a.user_id, 
	COUNT(distinct song_id) AS unique_songs
	FROM 
	user_song_interaction a
	JOIN
		(
			SELECT 
			user_id, 
			ROW_NUMBER() OVER (ORDER BY sum(plays) DESC) rank
			FROM 
			 user_song_interaction
			GROUP BY 1
		) b 
		ON a.user_id = b.user_id AND b.rank = 1
	GROUP BY 1
""")
most_active_user.show(1, False)

#+----------------------------------------+------------+
#|user_id                                 |unique_songs|
#+----------------------------------------+------------+
#|093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195         |
#+----------------------------------------+------------+

user_activity_distribution = (
    triplets_not_mismatched
    .select(['user_id','song_id'])
    .groupBy('user_id')
    .agg({'song_id': 'count'})
    .orderBy('count(song_id)', ascending=False)
    .select(
        F.col('user_id'),
        F.col('count(song_id)').alias('total_play_counts')
    )
)
user_activity_distribution.show(10, False)

#+----------------------------------------+-----------------+
#|user_id                                 |total_play_counts|
#+----------------------------------------+-----------------+
#|ec6dfcf19485cb011e0b22637075037aae34cf26|4316             |
#|8cb51abc6bf8ea29341cb070fe1e1af5e4c3ffcc|1562             |
#|5a3417a1955d9136413e0d293cd36497f5e00238|1557             |
#|fef771ab021c200187a419f5e55311390f850a50|1545             |
#|c1255748c06ee3f6440c51c439446886c7807095|1498             |
#|4e73d9e058d2b1f2dba9c1fe4a8f416f9f58364f|1470             |
#|cbc7bddbe3b2f59fdbe031b3c8d0db4175d361e6|1457             |
#|96f7b4f800cafef33eae71a6bc44f7139f63cd7a|1407             |
#|b7c24f770be6b802805ac0e2106624a517643c17|1364             |
#|119b7c88d58d0c6eb051365c103da5caf817bea6|1362             |
#+----------------------------------------+-----------------+
#only showing top 10 rows

song_popularity = (
    triplets_not_mismatched
    .select(['song_id','plays'])
    .groupBy('song_id')
    .agg({'plays': 'sum'})
    .orderBy('sum(plays)', ascending=False)
    .select(
        F.col('song_id'),
        F.col('sum(plays)').alias('total_play_counts')
    )
)
song_popularity.show(10, False)

#+------------------+-----------------+
#|song_id           |total_play_counts|
#+------------------+-----------------+
#|SOBONKR12A58A7A7E0|726885           |
#|SOSXLTC12AF72A7F54|527893           |
#|SOEGIYH12A6D4FC0E3|389880           |
#|SOAXGDH12A8C13F8A1|356533           |
#|SONYKOW12AB01849C9|292642           |
#|SOPUCYA12A8C13A694|274627           |
#|SOUFTBI12AB0183F65|268353           |
#|SOVDSJC12A58A7A271|244730           |
#|SOOFYTN12A6D4F9B35|241669           |
#|SOHTKMO12AB01843B0|236494           |
#+------------------+-----------------+
#only showing top 10 rows


##plotting user activity distribution
import matplotlib.pyplot as plt
user_activity_distribution_df = user_activity_distribution.toPandas()

user_activity_distribution_df.hist(column='total_play_counts', bins=60)
plt.xlabel('No. of Songs', fontsize=10)
plt.ylabel('No. of Users', fontsize=10)
plt.title('User Activity Distribution')
plt.tight_layout()
plt.savefig(f"user_activity_distribution.png", bbox_inches="tight")

##plotting song popularity
song_popularity_df = song_popularity.toPandas()
song_popularity_df.hist(column='total_play_counts', bins=60)
plt.hist(data, bins=range(min(data), max(data) + binwidth, binwidth))
plt.xlabel('No. of Songs', fontsize=10)
plt.ylabel('Total Play Count', fontsize=10)
plt.title('Distribution of Song Popularity')
plt.tight_layout()
plt.savefig(f"song_popularity_distribution.png", bbox_inches="tight")



##1d

user_counts = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.count(col("song_id")).alias("song_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
user_counts.cache()
user_counts.count()

#Out[29]: 1019318

user_counts.show(10, False)

#+----------------------------------------+----------+----------+
#|user_id                                 |song_count|play_count|
#+----------------------------------------+----------+----------+
#|093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195       |13074     |
#|119b7c88d58d0c6eb051365c103da5caf817bea6|1362      |9104      |
#|3fa44653315697f42410a30cb766a4eb102080bb|146       |8025      |
#|a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|518       |6506      |
#|d7d2d888ae04d16e994d6964214a1de81392ee04|1257      |6190      |
#|4ae01afa8f2430ea0704d502bc7b57fb52164882|453       |6153      |
#|b7c24f770be6b802805ac0e2106624a517643c17|1364      |5827      |
#|113255a012b2affeab62607563d03fbdf31b08e7|1096      |5471      |
#|99ac3d883681e21ea68071019dba828ce76fe94d|939       |5385      |
#|6d625c6557df84b60d90426c0116138b617b9449|1307      |5362      |
#+----------------------------------------+----------+----------+
#only showing top 10 rows

statistics = (
    user_counts
    .select("song_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#              count                mean              stddev min    max
#song_count  1019318   44.92720721109605   54.91113199747304   3   4316
#play_count  1019318  128.82423149596102  175.43956510305134   3  13074


user_counts.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
user_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

#Out[15]: [3.0, 17.0, 30.0, 61.0, 4316.0]


#Out[16]:[3.0, 29.0, 60.0, 135.0, 13074.0]




song_counts = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.count(col("user_id")).alias("user_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
song_counts.cache()
song_counts.count()
#Out[13]: 378310
song_counts.show(10, False)

#+------------------+----------+----------+
#|song_id           |user_count|play_count|
#+------------------+----------+----------+
#|SOBONKR12A58A7A7E0|84000     |726885    |
#|SOSXLTC12AF72A7F54|80656     |527893    |
#|SOEGIYH12A6D4FC0E3|69487     |389880    |
#|SOAXGDH12A8C13F8A1|90444     |356533    |
#|SONYKOW12AB01849C9|78353     |292642    |
#|SOPUCYA12A8C13A694|46078     |274627    |
#|SOUFTBI12AB0183F65|37642     |268353    |
#|SOVDSJC12A58A7A271|36976     |244730    |
#|SOOFYTN12A6D4F9B35|40403     |241669    |
#|SOHTKMO12AB01843B0|46077     |236494    |
#+------------------+----------+----------+
#only showing top 10 rows

statistics = (
    song_counts
    .select("user_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)


#             count                mean              stddev min     max
#user_count  378310  121.05181200602681   748.6489783736835   1   90444
#play_count  378310   347.1038513388491  2978.6053488382167   1  726885

song_counts.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)
song_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

#[1.0, 4.0, 11.0, 62.0, 90444.0]


#[1.0, 7.0, 28.0, 124.0, 726885.0]



##defining threshold

user_song_count_threshold = 26
song_user_count_threshold = 4

##limiting to the records above threshold
triplets_limited = triplets_not_mismatched

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("user_id").count().where(col("count") > user_song_count_threshold).select("user_id"),
        on="user_id",
        how="inner"
    )
)

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("song_id").count().where(col("count") > user_song_count_threshold).select("song_id"),
        on="song_id",
        how="inner"
    )
)

triplets_limited.cache()
triplets_limited.count()

Out[25]: 35559814



(
    triplets_limited
    .agg(
        countDistinct(col("user_id")).alias('user_count'),
        countDistinct(col("song_id")).alias('song_count')
    )
    .toPandas()
    .T
    .rename(columns={0: "value"})
)
#             value
#user_count  498503
#song_count  122536


def get_user_counts(triplets):
    return (
        triplets
        .groupBy("user_id")
        .agg(
            F.count(col("song_id")).alias("song_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )
    
def get_song_counts(triplets):
    return (
        triplets
        .groupBy("song_id")
        .agg(
            F.count(col("user_id")).alias("user_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

print(get_user_counts(triplets_limited).approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
#[1.0, 35.0, 52.0, 81.0, 3173.0]


print(get_song_counts(triplets_limited).approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
#[27.0, 46.0, 89.0, 205.0, 62309.0]




# -----------------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.feature import StringIndexer

# Encoding

user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_encoded")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="song_id_encoded")

user_id_indexer_model = user_id_indexer.fit(triplets_limited)
song_id_indexer_model = song_id_indexer.fit(triplets_limited)

triplets_limited = user_id_indexer_model.transform(triplets_limited)
triplets_limited = song_id_indexer_model.transform(triplets_limited)



# -----------------------------------------------------------------------------
# Splitting
# -----------------------------------------------------------------------------

# Imports

from pyspark.sql.window import *

# Splits

training, test = triplets_limited.randomSplit([0.7, 0.3])

test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

#training:          24888125
#test:              10671689
#test_not_training: 6


test_not_training.show(50, False)

#+----------------------------------------+------------------+-----+---------------+---------------+
#|user_id                                 |song_id           |plays|user_id_encoded|song_id_encoded|
#+----------------------------------------+------------------+-----+---------------+---------------+
#|9f8382df6cdcf0221e334fbe64fcf0bad652003b|SOLFRUD12A8C13CB47|1    |498489.0       |34082.0        |
#|9f8382df6cdcf0221e334fbe64fcf0bad652003b|SOAYYBH12A8C13CB4C|1    |498489.0       |77940.0        |
#|9f8382df6cdcf0221e334fbe64fcf0bad652003b|SOQTNNR12A8C13E344|1    |498489.0       |93453.0        |
#|9f8382df6cdcf0221e334fbe64fcf0bad652003b|SOUEKUH12A8C13D7C2|1    |498489.0       |85020.0        |
#|fcc941a7898dee8c094291877226721611ed54c8|SOJUVNO12AC468AD2C|1    |498498.0       |118143.0       |
#|fcc941a7898dee8c094291877226721611ed54c8|SOJZKMF12AC468B4F5|1    |498498.0       |44129.0        |
#+----------------------------------------+------------------+-----+---------------+---------------+



counts = test_not_training.groupBy("user_id").count().toPandas().set_index("user_id")["count"].to_dict()

temp = (
    test_not_training
    .withColumn("id", monotonically_increasing_id())
    .withColumn("random", rand())
    .withColumn(
        "row",
        row_number()
        .over(
            Window
            .partitionBy("user_id")
            .orderBy("random")
        )
    )
)

for k, v in counts.items():
    temp = temp.where((col("user_id") != k) | (col("row") < v * 0.7))

temp = temp.drop("id", "random", "row")
temp.cache()

temp.show(50, False)

# +----------------------------------------+------------------+-----+
# |user_id                                 |song_id           |plays|
# +----------------------------------------+------------------+-----+
# |a218fef82a857225ae5fcce5db0ec2ac96851cc2|SOENDMN12A58A78493|1    |
# |a218fef82a857225ae5fcce5db0ec2ac96851cc2|SOEPWYH12AF72A4813|1    |
# |a218fef82a857225ae5fcce5db0ec2ac96851cc2|SOSHJHA12AB0181410|1    |
# |a218fef82a857225ae5fcce5db0ec2ac96851cc2|SODQXUK12AF72A13E5|1    |
# |a218fef82a857225ae5fcce5db0ec2ac96851cc2|SORMOAU12AB018956B|1    |
# |42830ed368d1c29396791f0cb1c1bb871f8af06f|SOOYDAZ12A58A7AE08|1    |
# |42830ed368d1c29396791f0cb1c1bb871f8af06f|SOARUPP12AB01842E0|1    |
# |42830ed368d1c29396791f0cb1c1bb871f8af06f|SOAAVUV12AB0186646|1    |
# |42830ed368d1c29396791f0cb1c1bb871f8af06f|SOUOPLF12AB017F40F|1    |
# |42830ed368d1c29396791f0cb1c1bb871f8af06f|SOGWUHI12AB01876BD|1    |
# +----------------------------------------+------------------+-----+

training = training.union(temp.select(training.columns))
test = test.join(temp, on=["user_id", "song_id"], how="left_anti")
test_not_training = test.join(training, on="user_id", how="left_anti")

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

#training:          24888128
#test:              10671686
#test_not_training: 0


# -----------------------------------------------------------------------------
# Modeling
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.recommendation import ALS

from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator

# Modeling

als = ALS(maxIter=5, regParam=0.01, userCol="user_id_encoded", itemCol="song_id_encoded", ratingCol="plays", implicitPrefs=True)
als_model = als.fit(training)
predictions = als_model.transform(test)

predictions = predictions.orderBy(col("user_id"), col("song_id"), col("prediction").desc())
predictions.cache()

predictions.show(20, False)

#+----------------------------------------+------------------+-----+---------------+---------------+------------+
#|user_id                                 |song_id           |plays|user_id_encoded|song_id_encoded|prediction  |
#+----------------------------------------+------------------+-----+---------------+---------------+------------+
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOBJCFV12A8AE469EE|1    |310977.0       |1933.0         |0.035843447 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOBZFSZ12A8C13F2CA|1    |310977.0       |355.0          |0.10580486  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOFFWDQ12A8C13B433|3    |310977.0       |2056.0         |0.037560128 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOGVKXX12A67ADA0B8|1    |310977.0       |339.0          |0.10306478  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOGXWGC12AF72A8F9A|1    |310977.0       |1762.0         |0.04520792  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOGZCOB12A8C14280E|2    |310977.0       |405.0          |0.069713384 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOIGHWG12A8C136A37|2    |310977.0       |1407.0         |0.042275887 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOIXKRK12A8C140BD1|1    |310977.0       |756.0          |0.06993737  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOKEYJQ12A6D4F6132|1    |310977.0       |212.0          |0.12238116  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOKUECJ12A6D4F6129|1    |310977.0       |178.0          |0.1321509   |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOLCSYN12AF72A049D|1    |310977.0       |1329.0         |0.055340037 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOPHBRE12A8C142825|2    |310977.0       |977.0          |0.050390188 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SORRCNC12A8C13FDA9|1    |310977.0       |284.0          |0.041467454 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOYDTRQ12AF72A3D61|2    |310977.0       |245.0          |0.08965379  |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOZAPQT12A8C142821|1    |310977.0       |458.0          |0.067643434 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOZORGY12A8C140382|1    |310977.0       |1062.0         |0.034480624 |
#|00004fb90a86beb8bed1e9e328f5d9b6ee7dc03e|SOZVCRW12A67ADA0B7|1    |310977.0       |28.0           |0.19320878  |
#|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|SOAPSFK12AC46890F8|1    |399721.0       |10331.0        |0.01155064  |
#|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|SOINTRI12AB0183A90|1    |399721.0       |18846.0        |4.8541787E-4|
#|000060ca4e6bea0a5c9037fc1bbd7bbabb98c754|SOJOTLM12A6D4F7515|2    |399721.0       |19932.0        |0.0018776429|
#+----------------------------------------+------------------+-----+---------------+---------------+------------+
#only showing top 20 rows


evaluator = RegressionEvaluator(metricName="rmse", labelCol="plays", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)

print("Root-mean-square error = " + str(rmse))
#Root-mean-square error = 7.091965615119148

#2b sample users


sample_users = triplets_limited.select(als.getUserCol()).filter(triplets_limited["user_id_encoded"].rlike("25[0-5]")).distinct()

sample_users_recommend = als_model.recommendForUserSubset(sample_users, 10)
sample_users_recommend.show(10, False)

#+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|user_id_encoded|recommendations                                                                                                                                                                              |
#+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|254            |[[7, 0.25827312], [15, 0.25534675], [12, 0.25431228], [94, 0.22299737], [0, 0.21788839], [156, 0.21503791], [152, 0.21004863], [17, 0.19626674], [95, 0.19594415], [2, 0.18964852]]          |
#|1250           |[[12, 0.30942306], [187, 0.27333426], [95, 0.25975728], [49, 0.2440859], [282, 0.23866436], [207, 0.23068759], [144, 0.21726795], [132, 0.21224324], [47, 0.19939348], [90, 0.1981295]]      |
#|1251           |[[185, 0.42231154], [69, 0.42104053], [50, 0.3708393], [167, 0.3646331], [2561, 0.34873375], [1730, 0.34091452], [1672, 0.34072903], [267, 0.33415738], [1692, 0.33317307], [669, 0.3300878]]|
#|2250           |[[156, 0.41467345], [17, 0.38792878], [152, 0.3508857], [45, 0.32396483], [60, 0.3220988], [47, 0.31045535], [82, 0.30884838], [49, 0.30691025], [0, 0.30581647], [166, 0.30429378]]         |
#|2254           |[[17, 0.30806252], [14, 0.30562955], [190, 0.2960279], [156, 0.2822315], [135, 0.28187358], [320, 0.27592957], [66, 0.26741987], [11, 0.26361173], [211, 0.24597119], [166, 0.23345134]]     |
#|2255           |[[0, 0.963614], [1, 0.9158456], [3, 0.8594842], [2, 0.851236], [6, 0.8047435], [4, 0.80399513], [7, 0.7601462], [15, 0.74989676], [12, 0.7238014], [9, 0.70708454]]                          |
#|2501           |[[6, 1.1930194], [1, 1.1867826], [12, 1.1529635], [23, 1.1425496], [8, 1.1318175], [11, 1.120616], [9, 1.098233], [0, 1.0978279], [20, 1.0919116], [4, 1.0855896]]                           |
#|2504           |[[12, 0.92563784], [21, 0.90106356], [48, 0.8961294], [23, 0.8851957], [15, 0.88318956], [27, 0.8450347], [20, 0.82209516], [187, 0.7974549], [53, 0.70625454], [135, 0.69915974]]           |
#|2507           |[[0, 0.40399384], [7, 0.3720903], [210, 0.3218864], [224, 0.30415055], [239, 0.2934395], [237, 0.29193178], [2, 0.29031643], [144, 0.284569], [256, 0.28345883], [243, 0.27988532]]          |
#|2508           |[[5, 0.8196302], [23, 0.74661183], [8, 0.7282639], [15, 0.7207441], [41, 0.69159734], [10, 0.6844063], [32, 0.66002446], [152, 0.65816236], [27, 0.65646625], [21, 0.6473797]]               |
#+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#only showing top 10 rows





def recommend(recommendations):
    items = []
    for item, rating in recommendations:
        items.append(item)
    return items

udf_recommend = F.udf(lambda recommendations: recommend(recommendations), ArrayType(IntegerType()))

temp = (
    sample_users_recommend.withColumn('recommends', udf_recommend(F.col('recommendations')))
    .select(['user_id_encoded', 'recommends'])
    )

test_array = test.groupBy("user_id_encoded").agg(F.collect_list("song_id_encoded"))

# Question 2c)

test_metric = temp.join(test_array, on="user_id_encoded", how="left")

test_metric = test_metric.drop("user_id_encoded")

#filter for null as there were records as random split was performed
test_rdd = test_metric.filter(F.col('collect_list(song_id_encoded)').isNotNull()).rdd

from pyspark.mllib.evaluation import RankingMetrics

metrics_2 = RankingMetrics(test_rdd)

metrics_2.precisionAt(5)
Out[58]: 0.05091471616895345

metrics_2.ndcgAt(10)
Out[59]: 0.04792769882442554

metrics_2.meanAveragePrecision
Out[60]: 0.010670163653693911
