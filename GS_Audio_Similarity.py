##Gajalakshmi Sakthivel
##Audio Similarity

##imports
import pandas as pd
from pyspark.mllib.stat import Statistics

##define attribute and schema for loading data
audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

def audio_dataset_schema(path):
    audio_dataset_path = f"/scratch-network/courses/2020/DATA420-20S2/data/msd/audio/attributes/{path}-v1.0.attributes.csv"
    with open(audio_dataset_path, "r") as f:
        rows = [line.strip().split(",") for line in f.readlines()]

    audio_dataset_schema = StructType([StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows])
    return audio_dataset_schema

def load_dataset(path):
    dataFrame = (
        spark.read.format("csv")
        .option("header", "false")
        .option("codec", "gzip")
        .schema(audio_dataset_schema(path))
        .load(f"hdfs:///data/msd/audio/features/{path}-v1.0.csv/")
        )

    return dataFrame

paths=(["msd-jmir-area-of-moments-all","msd-jmir-lpc-all",
"msd-jmir-methods-of-moments-all","msd-jmir-mfcc-all","msd-jmir-spectral-all-all","msd-jmir-spectral-derivatives-all-all",
"msd-marsyas-timbral","msd-mvd","msd-rh","msd-rp","msd-ssd","msd-trh","msd-tssd"])


datasets = {path : load_dataset(path) for path in paths}

datasets.get("msd-jmir-methods-of-moments-all").describe().show()

##descriptive statistics
##+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
##|summary|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|         MSD_TRACKID|
##+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+
##|  count|                                        994623|                                        994623|                                        994623|                                        994623|                                        994623|                             994623|                             994623|                             994623|                             994623|                             994623|              994623|
##|   mean|                            0.1549817600174655|                            10.384550576951835|                             526.8139724398112|                             35071.97543290272|                             5297870.369577217|                 0.3508444432531261|                  27.46386798784021|                 1495.8091812075486|                 143165.46163257837|                2.396783048473542E7|                null|
##| stddev|                           0.06646213086143041|                            3.8680013938747018|                             180.4377549977511|                            12806.816272955532|                            2089356.4364557962|                 0.1855795683438387|                  8.352648595163698|                 505.89376391902437|                 50494.276171032136|                  9307340.299219608|                null|
##|    min|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                0.0|                                0.0|                                0.0|                          -146300.0|                                0.0|'TRAAAAK128F9318786'|
##|    max|                                         0.959|                                         55.42|                                        2919.0|                                      407100.0|                                       4.657E7|                              2.647|                              117.0|                             5834.0|                           452500.0|                            9.477E7|'TRZZZZO128F428E2D4'|
##+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+--------------------+

##finding correlation between features
feature_moments = datasets.get("msd-jmir-methods-of-moments-all")
feature_moments_data= feature_moments
feature_moments_data = feature_moments_data.drop("MSD_TRACKID")
moment_columns = feature_moments_data.columns
feature = feature_moments_data.rdd.map(lambda row: row[0:])
correlation_matrix = Statistics.corr(feature, method="pearson")
correlation_df = pd.DataFrame(correlation_matrix)
correlation_df.index, correlation_df.columns = moment_columns, moment_columns
print(correlation_df.to_string())

#                                                Method_of_Moments_Overall_Standard_Deviation_1  Method_of_Moments_Overall_Standard_Deviation_2  Method_of_Moments_Overall_Standard_Deviation_3  Method_of_Moments_Overall_Standard_Deviation_4  Method_of_Moments_Overall_Standard_Deviation_5  Method_of_Moments_Overall_Average_1  Method_of_Moments_Overall_Average_2  Method_of_Moments_Overall_Average_3  Method_of_Moments_Overall_Average_4  Method_of_Moments_Overall_Average_5
#Method_of_Moments_Overall_Standard_Deviation_1                                        1.000000                                        0.426280                                        0.296306                                        0.061039                                       -0.055336                             0.754208                             0.497929                             0.447565                             0.167466                             0.100407
#Method_of_Moments_Overall_Standard_Deviation_2                                        0.426280                                        1.000000                                        0.857549                                        0.609521                                        0.433797                             0.025228                             0.406923                             0.396354                             0.015607                            -0.040902
#Method_of_Moments_Overall_Standard_Deviation_3                                        0.296306                                        0.857549                                        1.000000                                        0.803010                                        0.682909                            -0.082415                             0.125910                             0.184962                            -0.088174                            -0.135056
#Method_of_Moments_Overall_Standard_Deviation_4                                        0.061039                                        0.609521                                        0.803010                                        1.000000                                        0.942244                            -0.327691                            -0.223220                            -0.158231                            -0.245034                            -0.220873
#Method_of_Moments_Overall_Standard_Deviation_5                                       -0.055336                                        0.433797                                        0.682909                                        0.942244                                        1.000000                            -0.392551                            -0.355019                            -0.285966                            -0.260198                            -0.211813
#Method_of_Moments_Overall_Average_1                                                   0.754208                                        0.025228                                       -0.082415                                       -0.327691                                       -0.392551                             1.000000                             0.549015                             0.518503                             0.347112                             0.278513
#Method_of_Moments_Overall_Average_2                                                   0.497929                                        0.406923                                        0.125910                                       -0.223220                                       -0.355019                             0.549015                             1.000000                             0.903367                             0.516499                             0.422549
#Method_of_Moments_Overall_Average_3                                                   0.447565                                        0.396354                                        0.184962                                       -0.158231                                       -0.285966                             0.518503                             0.903367                             1.000000                             0.772807                             0.685645
#Method_of_Moments_Overall_Average_4                                                   0.167466                                        0.015607                                       -0.088174                                       -0.245034                                       -0.260198                             0.347112                             0.516499                             0.772807                             1.000000                             0.984867
#Method_of_Moments_Overall_Average_5                                                   0.100407                                       -0.040902                                       -0.135056                                       -0.220873                                       -0.211813                             0.278513                             0.422549                             0.685645                             0.984867                             1.000000

##plotting correlation matrix as heat map
import matplotlib.pyplot as plt
import seaborn as sns

plot, axis = plt.subplots(dpi=300, figsize=(10, 8))
axis = sns.heatmap(
    correlation_df,
    vmin=-1, vmax=1, center=0,
    cmap=sns.diverging_palette(10, 320, n=200),
    square=True
)
axis.set_xticklabels(
    axis.get_xticklabels(),
    rotation=45,
    horizontalalignment='right'
)

#Out[32]:
#[Text(0.5, 0, 'Method_of_Moments_Overall_Standard_Deviation_1'),
#Text(1.5, 0, 'Method_of_Moments_Overall_Standard_Deviation_2'),
#Text(2.5, 0, 'Method_of_Moments_Overall_Standard_Deviation_3'),
#Text(3.5, 0, 'Method_of_Moments_Overall_Standard_Deviation_4'),
#Text(4.5, 0, 'Method_of_Moments_Overall_Standard_Deviation_5'),
#Text(5.5, 0, 'Method_of_Moments_Overall_Average_1'),
#Text(6.5, 0, 'Method_of_Moments_Overall_Average_2'),
#Text(7.5, 0, 'Method_of_Moments_Overall_Average_3'),
#Text(8.5, 0, 'Method_of_Moments_Overall_Average_4'),
#Text(9.5, 0, 'Method_of_Moments_Overall_Average_5')]

plt.tight_layout()
plt.savefig("correlation_plot1.png", bbox_inches="tight")
plt.close()


from pyspark.sql.types import *
from pyspark.sql import functions as F

# Defining schema
schema_msd_genre = StructType([ \
    StructField("MAGD_TrackID", StringType(), True), \
    StructField("Genre", StringType(), True), \
])

# Loading the MAGD Profile
msd_genre = ( \
    spark.read.format("com.databricks.spark.csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("delimiter", "\t") \
    .schema(schema_msd_genre) \
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv") \
)

msd_genre.count()
#Out[38]: 422714



# Reducing manually matched song id from total mismatches
mismatches_no_approval = ( \
    mismatches \
    .join(matches_manually_accepted, on="song_id", how="left_anti") \
)

# Reducing genre records with mismatches that were not accepted on track id
msd_genre_match = ( \
    msd_genre \
    .join( \
        mismatches_no_approval, \
        mismatches_no_approval.track_id == msd_genre.MAGD_TrackID, \
        how="left_anti" \
        )
    )
msd_genre_match.count()
#Out[41]: 415350



# Visulization of  Genre distribution
import matplotlib.pyplot as plt

count_msd_genre_match = ( \
    msd_genre_match \
    .groupby("Genre") \
    .agg({"MAGD_TrackID":"count"}) \
)
count_msd_genre_match.show()

#+--------------+-------------------+
#|         Genre|count(MAGD_TrackID)|
#+--------------+-------------------+
#|         Blues|               6776|
#|          Folk|               5777|
#| International|              14094|
#|        Stage |               1604|
#|      Children|                468|
#|      Pop_Rock|             234107|
#|           Rap|              20606|
#|         Vocal|               6076|
#|           RnB|              13874|
#|     Religious|               8754|
#|       Country|              11492|
#|   Avant_Garde|               1000|
#|         Latin|              17475|
#| Comedy_Spoken|               2051|
#|       New Age|               3935|
#|Easy_Listening|               1533|
#|     Classical|                542|
#|    Electronic|              40430|
#|          Jazz|              17673|
#|        Reggae|               6885|
#+--------------+-------------------+
#only showing top 20 rows


count_genres_df = count_msd_genre_match.toPandas()
count_genres_df.plot(kind = 'bar', x= 'Genre', y='count(MAGD_TrackID)')
##Out[48]: <matplotlib.axes._subplots.AxesSubplot at 0x7f40bf65dd30>


plt.xlabel('Genre', fontsize=20)
plt.ylabel('Song Count', fontsize=20)
plt.title('Genre Distribution')
plt.savefig("Genre Distribution", bbox_inches='tight')

1c)

#Joing methods of moments with genres table to associate song with genre
method_moments = datasets.get("msd-jmir-methods-of-moments-all")
datasets.update({"msd-jmir-methods-of-moments-all":method_moments.withColumn('MSD_TRACKID',F.regexp_replace(F.col('MSD_TRACKID'), "\'", ""))})

method_moments = datasets.get("msd-jmir-methods-of-moments-all")
datasets.update({"msd-jmir-methods-of-moments-all": method_moments.join(msd_genre,method_moments.MSD_TRACKID==msd_genre.MAGD_TrackID,how="inner")})
method_moments = datasets.get("msd-jmir-methods-of-moments-all")
method_moments = method_moments.drop("MAGD_TrackID")
method_moments.show(5)

#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+--------------+
#|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|       MSD_TRACKID|         Genre|
#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+--------------+
#|                                        0.1308|                                         9.587|                                         459.9|                                       27280.0|                                     4303000.0|                             0.2474|                              26.02|                             1067.0|                            67790.0|                          8281000.0|TRAAABD128F429CF47|      Pop_Rock|
#|                                       0.08392|                                         7.541|                                         423.7|                                       36060.0|                                     5662000.0|                             0.1522|                              24.07|                             1326.0|                           166000.0|                            2.977E7|TRAAADT12903CCC339|Easy_Listening|
#|                                        0.1199|                                         9.381|                                         474.5|                                       26990.0|                                     4353000.0|                             0.3047|                              30.76|                              977.5|                            45890.0|                          5469000.0|TRAAAEF128F4273421|      Pop_Rock|
#|                                        0.2284|                                         12.99|                                         730.3|                                       38580.0|                                     5683000.0|                             0.5962|                              39.32|                             1993.0|                           109800.0|                            1.542E7|TRAAAGR128F425B14B|      Pop_Rock|
#|                                        0.1241|                                         7.492|                                         481.9|                                       35450.0|                                     5158000.0|                             0.3627|                              22.21|                             1351.0|                           158500.0|                            2.715E7|TRAAAGW12903CC1049|         Blues|
#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+--------------+
#only showing top 5 rows

########Q2 ########################

#a) Logistic Regression, Naive Bayes, Random Forest

from pyspark.ml import Pipeline

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler,Normalizer, VectorIndexer


datasets.update({"msd-jmir-methods-of-moments-all":method_moments.withColumn("is_Rap",F.when(F.col("genre").contains("Rap"),"1").when(F.col("genre").isNull(),"0").otherwise("0"))})

method_moments = datasets.get("msd-jmir-methods-of-moments-all")

print((method_moments.filter(F.col("is_Rap")==1).count()*100)/method_moments.count())
#4.968617754742999


label_SI = StringIndexer(inputCol = "is_Rap", outputCol = "label")

pipeline_stages = Pipeline(stages=[label_SI])
pipeline_fit = pipeline_stages.fit(method_moments)
method_moment_df = pipeline_fit.transform(method_moments)

method_moment_df = method_moment_df.repartition(100)
cols=[x for x in method_moment_df.columns if x not in ["label","MSD_TRACKID","is_Rap","genre"]]

assembler = VectorAssembler(inputCols=cols,outputCol="features")
columns=[x for x in method_moment_df.columns if x not in ["label","MSD_TRACKID","is_Rap","Genre"]]
assembler = VectorAssembler(inputCols=columns,outputCol="features")
method_moment_df = assembler.transform(method_moment_df)
normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
method_moment_df = normalizer.transform(method_moment_df)


#########################Class Imbalance ##############################

#80/20 test train split and  handling class imbalance in the training data
from pyspark.sql.window import *
from pyspark.sql.functions import *

temp = (
    method_moment_df
    .withColumn("id", monotonically_increasing_id())
    .withColumn("Random", rand())
    .withColumn(
        "Row",
        row_number()
        .over(
            Window
            .partitionBy("genre")
            .orderBy("Random")
        )
    )
)

genre_df=method_moment_df.select(F.col("genre")).distinct().collect()
genre_df = [row.genre for row in genre_df]

genre_count = {genre:method_moment_df.filter(F.col("genre")==genre).count() for genre in genre_df}

condition = ' or '.join([f" (genre ==\"{genre}\" and (Row < {genre_count.get(genre)*0.8})) " for genre in genre_df])

training_data = temp.where(condition)
training_data.cache()

training_data.groupBy("genre").count().show(5)
#+-------------+-----+
#|        genre|count|
#+-------------+-----+
#|        Blues| 5440|
#|     Children|  370|
#|         Folk| 4631|
#|International|11355|
#|       Stage | 1290|
#+-------------+-----+
#only showing top 5 rows


test_data = temp.join(training_data, on="id", how="left_anti")
test_data.cache()

test_data.groupBy("genre").count().show(5)
#+-------------+-----+
#|        genre|count|
#+-------------+-----+
#|        Blues| 1343|
#|     Children|   91|
#|         Folk| 1168|
#|International| 2897|
#|       Stage |  328|
#+-------------+-----+

#only showing top 5 rows



sample_fraction ={genre:0.5 for genre in genre_df}
sample_fraction.update({'Rap':1})
stratified_sample = training_data.sampleBy("genre",sample_fraction)

sample_rap = stratified_sample[stratified_sample['genre'] == 'Rap']
training_data = stratified_sample.union(sample_rap)
training_data.cache()

training_data.groupBy("genre").count().show(5)
#+-------------+-----+
#|        genre|count|
#+-------------+-----+
#|        Blues| 2623|
#|     Children|  189|
#|         Folk| 2319|
#|International| 5647|
#|       Stage |  641|
#+-------------+-----+

#only showing top 5 rows


datasize=float(training_data.select("label").count())
positive_count=training_data.select("label").where('label == 1.0').count()
percent=(float(positive_count)/float(datasize))*100
negative_count=float(datasize-positive_count)
class_balance= negative_count/datasize

class_balance
##Out[32]: 0.8270302146215801



#######################LogisticRegression ######################

# Performing Logistic Regression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.classification import LogisticRegression

training_data=training_data.withColumn("classWeights",when(training_data.label == '1.0',class_balance).otherwise(1-class_balance))

logistic_regression = LogisticRegression(maxIter=20, elasticNetParam=0.0,labelCol='label',featuresCol="normFeatures",weightCol="classWeights")
logistic_regression_model = logistic_regression.fit(training_data)
Log_reg_predict = logistic_regression_model.transform(test_data)

evaluate_model1 = BinaryClassificationEvaluator()

evaluate_model1.evaluate(Log_reg_predict)
##Out[33]: 0.8348672840678726



results = Log_reg_predict.select(['prediction', 'label'])
result_rdd=results.rdd

metrics = MulticlassMetrics(result_rdd)

c_matrix=metrics.confusionMatrix().toArray()
print(c_matrix)
#[[61160. 18822.]
 #[  901.  3254.]]


 
def binary_class_metrics(Log_reg_predict, labelCol="is_Rap", predictionCol="prediction", rawPredictionCol="rawPrediction"):

    count1 = Log_reg_predict.count()
    positive_count1 = Log_reg_predict.filter((col(labelCol) == 1)).count()
    negative_count1 = Log_reg_predict.filter((col(labelCol) == 0)).count()
    nP = Log_reg_predict.filter((col(predictionCol) == 1)).count()
    nN = Log_reg_predict.filter((col(predictionCol) == 0)).count()
    TP = Log_reg_predict.filter((col(predictionCol) == 1) & (col(labelCol) == 1)).count()
    FP = Log_reg_predict.filter((col(predictionCol) == 1) & (col(labelCol) == 0)).count()
    FN = Log_reg_predict.filter((col(predictionCol) == 0) & (col(labelCol) == 1)).count()
    TN = Log_reg_predict.filter((col(predictionCol) == 0) & (col(labelCol) == 0)).count()
    Precision = TP / (TP + FP)
    Recall = TP / (TP + FN)
    F1_Score = 2*(Recall * Precision) / (Recall + Precision)
    binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol=labelCol, metricName="areaUnderROC")
    auroc = binary_evaluator.evaluate(Log_reg_predict)

    print('actual total:    {}'.format(count1))
    print('actual positive: {}'.format(positive_count1))
    print('actual negative: {}'.format(negative_count1))
    print('nP:              {}'.format(nP))
    print('nN:              {}'.format(nN))
    print('TP:              {}'.format(TP))
    print('FP:              {}'.format(FP))
    print('FN:              {}'.format(FN))
    print('TN:              {}'.format(TN))
    print('precision:       {}'.format(Precision))
    print('recall:          {}'.format(Recall))
    print('F1 Score:        {}'.format(F1_Score))
    print('accuracy:        {}'.format((TP + TN) / count1))
    print('auroc:           {}'.format(auroc))


binary_class_metrics(Log_reg_predict.withColumn("is_Rap",F.col("is_Rap").cast(IntegerType())))
#actual total:    84137
#actual positive: 4155
#actual negative: 79982
#nP:              22076
#nN:              62061
#TP:              3254
#FP:              18822
#FN:              901
#TN:              61160
#precision:       0.14739989128465303
#recall:          0.7831528279181709
#F1 Score:        0.24810338911974383
#accuracy:        0.7655847011421848
#auroc:           0.8348672840678636


##################################### Random Forest ###########################
#Performing Random Forest

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes, LinearSVC

Random_forest = RandomForestClassifier(labelCol="label",featuresCol="normFeatures",numTrees=10)
Random_Forest_Model = Random_forest.fit(training_data)
rf_predict = Random_Forest_Model.transform(test_data)

evaluate_model2 = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

rf_accuracy = evaluate_model2.evaluate(rf_predict)
print(rf_accuracy)
#0.8085702195147945



rf_results = rf_predict.select(['prediction', 'label'])
results_rdd=rf_results.rdd

# metrics object
rf_metrics = MulticlassMetrics(results_rdd)

rf_matrix=rf_metrics.confusionMatrix().toArray()
print(rf_matrix)
[[77969.  2013.]
 [ 3074.  1081.]]



binary_class_metrics(rf_predict.withColumn("is_Rap",F.col("is_Rap").cast(IntegerType())))

#actual total:    84137
#actual positive: 4155
#actual negative: 79982
#nP:              3094
#nN:              81043
#TP:              1081
#FP:              2013
#FN:              3074
#TN:              77969
#precision:       0.3493859082094376
#recall:          0.2601684717208183
#F1 Score:        0.29824803421161544
#accuracy:        0.9395390850636463
#auroc:           0.8085702195147947

##parameter tuning without cv for randomforest

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes, LinearSVC

Random_forest_hp = RandomForestClassifier(labelCol="label",featuresCol="normFeatures",maxDepth=5,  maxBins=30, numTrees=20, seed=100)
Random_Forest_Model_hp = Random_forest_hp.fit(training_data)
rf_predict_hp = Random_Forest_Model_hp.transform(test_data)

evaluate_model_hp = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

rf_hp_accuracy = evaluate_model_hp.evaluate(rf_predict_hp)
print(rf_hp_accuracy)
#0.8235596818948527


rf_results_hp = rf_predict_hp.select(['prediction', 'label'])
results_rdd_hp=rf_results_hp.rdd

# Instantiate metrics object
rf_metrics_hp = MulticlassMetrics(results_rdd_hp)

rf_matrix_hp=rf_metrics.confusionMatrix().toArray()
print(rf_matrix_hp)

#[[78030.  1915.]
#[ 3154.  1038.]]

binary_class_metrics(rf_predict_hp.withColumn("is_Rap",F.col("is_Rap").cast(IntegerType())))
#actual total:    84137
#actual positive: 4192
#actual negative: 79945
#nP:              2953
#nN:              81184
#TP:              1038
#FP:              1915
#FN:              3154
#TN:              78030
#precision:       0.351506942092787
#recall:          0.2476145038167939
#F1 Score:        0.2905528341497551
#accuracy:        0.9397530218572091
#auroc:           0.8235596818948525


################################# Naive Bayes #####################################

#Perfoming Naive Bayes Classification
naive_bayes = NaiveBayes(smoothing=1.0, modelType="multinomial",featuresCol="normFeatures",weightCol="classWeights")
naive_bayes_model = naive_bayes.fit(training_data)
nb_predict = naive_bayes_model.transform(test_data)

evaluate_model3 = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
nb_accuracy = evaluate_model3.evaluate(nb_predict)
print(nb_accuracy)
#0.4551674355778241


nb_results = nb_predict.select(['prediction', 'label'])
nb_results_rdd=nb_results.rdd

# Instantiate metrics object
nb_metrics = MulticlassMetrics(nb_results_rdd)
nb_matrix=nb_metrics.confusionMatrix().toArray()
print(nb_matrix)
#[[47383. 32675.]
# [ 2622.  1457.]]



binary_class_metrics(nb_predict.withColumn("is_Rap",F.col("is_Rap").cast(IntegerType())))
#actual total:    84137
#actual positive: 4079
#actual negative: 80058
#nP:              34132
#nN:              50005
#TP:              1457
#FP:              32675
#FN:              2622
#TN:              47383
#precision:       0.042687214344310324
#recall:          0.35719539102721254
#F1 Score:        0.07626076260762607
#accuracy:        0.5804818332006133
#auroc:           0.45516743557783246

###################################### Cross Validation ################################
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

#Perform Cross Validation on the logistic Regression model with different hyper parameters
parameter_grid = ParamGridBuilder() \
    .addGrid(logistic_regression.threshold, [0.45, 0.52, 0.60,0.43,0.72]) \
    .addGrid(logistic_regression.elasticNetParam, [0.0, 1.0, 0.3]) \
    .addGrid(logistic_regression.maxIter, [10, 20, 50]) \
    .build()

pipeline = Pipeline(stages=[logistic_regression])

logistic_reg_cv = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=parameter_grid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=10)

log_reg_cv_model = logistic_reg_cv.fit(training_data)

parameter_dict = log_reg_cv_model.bestModel.stages[-1].extractParamMap()

best_parameter = {}
for k, v in parameter_dict.items():
  best_parameter[k.name] = v

best_reg_param = best_parameter["regParam"]
best_elastic_net = best_parameter["elasticNetParam"]
best_max_iter = best_parameter["maxIter"]
best_threshold = best_parameter["threshold"]

print("Best Regression Parameter         : "+str(best_reg_param))
print("Best elasticNet Parameter  : "+str(best_elastic_net))
print("Best maximun iteration    : "+str(best_max_iter))
print("Best threshold   : "+str(best_threshold))
#Best Regression Parameter         : 0.0
#Best elasticNet Parameter  : 0.3
#Best maximun iteration    : 50
#Best threshold   : 0.52


#Predictions
log_reg_cv_predict = log_reg_cv_model.bestModel.transform(test_data)

log_reg_cv_results = log_reg_cv_predict.select(['prediction', 'label'])
log_reg_cv_rdd=log_reg_cv_results.rdd

# Instantiate metrics object
log_reg_cv_metrics = MulticlassMetrics(log_reg_cv_rdd)
log_reg_cv_matrix=log_reg_cv_metrics.confusionMatrix().toArray()
print(log_reg_cv_matrix)
#[[63860. 16085.]
#[  950.  3242.]]


binary_class_metrics(log_reg_cv_predict.withColumn("is_Rap",F.col("is_Rap").cast(IntegerType())))
#actual total:    84137
#actual positive: 4192
#actual negative: 79945
#nP:              19327
#nN:              64810
#TP:              3242
#FP:              16085
#FN:              950
#TN:              63860
#precision:       0.16774460599161795
#recall:          0.7733778625954199
#F1 Score:        0.2756919937072155
#accuracy:        0.7975325956475748
#auroc:           0.8475961019718214

##multiclass
from pyspark.ml.feature import OneHotEncoder, StringIndexer

#Encoding the genre column and sampling the dataset
string_indexer = StringIndexer(inputCol="Genre", outputCol="genreIndex")
model = string_indexer.fit(method_moment_df)
indexed_model = model.transform(method_moment_df)


keys = indexed_model.select(F.col("genreIndex")).distinct().collect()
genreIndex = [row.genreIndex for row in keys]
sample_fraction ={genreIndex:1 for genreIndex in genreIndex}
sample_fraction.update({0.0:0.1})
sample_fraction.update({1.0:0.5})
multiple_strat_sample = indexed_model.sampleBy("genreIndex",sample_fraction)



train_sample_fraction ={genreIndex:0.7 for genreIndex in genreIndex}
stratified_train = multiple_strat_sample.sampleBy("genreIndex",train_sample_fraction)
stratified_test = multiple_strat_sample.join(stratified_train,on="MSD_TRACKID", how="left_anti")


stratified_train_class_20 = stratified_train[stratified_train['genreIndex'] == 20.0]
stratified_train = stratified_train.union(stratified_train_class_20.sample(True,9.0))

stratified_train_class_18 = stratified_train[stratified_train['genreIndex'] == 18.0]
stratified_train = stratified_train.union(stratified_train_class_18.sample(True,9.0))

stratified_train_class_19 = stratified_train[stratified_train['genreIndex'] == 19.0]
stratified_train = stratified_train.union(stratified_train_class_19.sample(True,9.0))

stratified_train_class_17 = stratified_train[stratified_train['genreIndex'] == 17.0]
stratified_train = stratified_train.union(stratified_train_class_17.sample(True,9.0))

encoder = OneHotEncoder(inputCol="genreIndex", outputCol="genreVec")
encoded_train_sample = encoder.transform(stratified_train)
encoded_train_sample.show()

#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+-------------+------+-----+--------------------+--------------------+----------+---------------+
#|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|       MSD_TRACKID|        Genre|is_Rap|label|            features|        normFeatures|genreIndex|       genreVec|
#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+-------------+------+-----+--------------------+--------------------+----------+---------------+
#|                                        0.1997|                                         12.33|                                         416.2|                                       31250.0|                                     3735000.0|                             0.6444|                               42.5|                             2334.0|                           193700.0|                            3.277E7|TRQZEQK128F425D841|          Rap|     1|  1.0|[0.1997,12.33,416...|[5.43656459331457...|       2.0| (20,[2],[1.0])|
#|                                        0.2858|                                         15.79|                                         666.0|                                       33530.0|                                     4699000.0|                             0.5851|                              32.02|                             1433.0|                            82130.0|                            1.067E7|TRGYATW128F42503D4|          Rap|     1|  1.0|[0.2858,15.79,666...|[1.84544165517390...|       2.0| (20,[2],[1.0])|
#|                                       0.05165|                                         9.047|                                         580.4|                                       52070.0|                                     8332000.0|                            0.06381|                              15.75|                              956.6|                           117100.0|                            2.063E7|TRSCQOP128F92C7111|International|     0|  0.0|[0.05165,9.047,58...|[1.77291989488666...|       6.0| (20,[6],[1.0])|
#|                                        0.2485|                                         18.17|                                         740.0|                                       46410.0|                                     6307000.0|                             0.3468|                              27.58|                             1166.0|                            62110.0|                          8342000.0|TRIAABS128F428AC0E|Comedy_Spoken|     0|  0.0|[0.2485,18.17,740...|[1.68366452530487...|      14.0|(20,[14],[1.0])|
#|                                        0.1379|                                         10.99|                                         663.8|                                       44040.0|                                     6600000.0|                              0.319|                              24.56|                             1522.0|                           162000.0|                            2.751E7|TRCAAIB128F1468183|         Jazz|     0|  0.0|[0.1379,10.99,663...|[4.01826877992620...|       3.0| (20,[3],[1.0])|
#|                                       0.09228|                                         6.044|                                         310.1|                                       30550.0|                                     5285000.0|                             0.2538|                              23.67|                             1268.0|                           165300.0|                            2.983E7|TRNNLQV128F92FED8D|        Blues|     0|  0.0|[0.09228,6.044,31...|[2.61324203434584...|      10.0|(20,[10],[1.0])|
#|                                        0.1133|                                         10.25|                                         573.4|                                       42330.0|                                     6626000.0|                             0.2324|                              21.35|                             1437.0|                           156100.0|                            2.584E7|TRAYDWZ128F92FF349|   Electronic|     0|  0.0|[0.1133,10.25,573...|[3.46838797899504...|       1.0| (20,[1],[1.0])|
#|                                        0.1579|                                         6.619|                                         345.0|                                       18460.0|                                     2705000.0|                              0.664|                              28.45|                             1567.0|                           186900.0|                            3.304E7|TRMLYTQ128F92EB94D|        Latin|     0|  0.0|[0.1579,6.619,345...|[4.39192945496460...|       4.0| (20,[4],[1.0])|
#|                                       0.07089|                                         9.157|                                         527.6|                                       30430.0|                                     4374000.0|                             0.1328|                              23.42|                             1548.0|                           164800.0|                            2.695E7|TRFKETD128F9309ACA|         Jazz|     0|  0.0|[0.07089,9.157,52...|[2.24895272985703...|       3.0| (20,[3],[1.0])|
#|                                        0.2572|                                         14.61|                                         647.5|                                       38910.0|                                     5196000.0|                             0.5149|                              30.65|                             1727.0|                           171500.0|                            2.951E7|TRCSXZT128F425E36F|International|     0|  0.0|[0.2572,14.61,647...|[7.36565331887511...|       6.0| (20,[6],[1.0])|
#|                                       0.05528|                                         6.683|                                         399.0|                                       33250.0|                                     5603000.0|                             0.1018|                              22.98|                             1329.0|                           163500.0|                             2.88E7|TROSIAP128F4255534|          RnB|     0|  0.0|[0.05528,6.683,39...|[1.59761823928146...|       5.0| (20,[5],[1.0])|
#|                                         0.142|                                         5.975|                                         391.9|                                       40290.0|                                     7146000.0|                             0.1838|                              15.94|                              807.3|                           115900.0|                             2.23E7|TRTUVPD128F1495DEC|     Pop_Rock|     0|  0.0|[0.142,5.975,391....|[4.79674446588587...|       0.0| (20,[0],[1.0])|
#|                                        0.1626|                                          9.17|                                         518.0|                                       36510.0|                                     5644000.0|                             0.2574|                              26.42|                             1077.0|                            71640.0|                          9051000.0|TREXNYX128F42A555F|International|     0|  0.0|[0.1626,9.17,518....|[1.09829385446613...|       6.0| (20,[6],[1.0])|
#|                                        0.1668|                                         18.19|                                         726.5|                                       46100.0|                                     6161000.0|                             0.2686|                              38.47|                             2283.0|                           171500.0|                            2.884E7|TRTRLHQ128EF3607BF|International|     0|  0.0|[0.1668,18.19,726...|[4.73572139320018...|       6.0| (20,[6],[1.0])|
#|                                        0.3076|                                          13.3|                                         589.4|                                       39400.0|                                     4714000.0|                             0.5207|                              38.87|                             1848.0|                            93820.0|                            1.298E7|TREQTJQ128F93090EA|        Latin|     0|  0.0|[0.3076,13.3,589....|[1.72521029858863...|       4.0| (20,[4],[1.0])|
#|                                         0.102|                                          5.43|                                         310.3|                                       24390.0|                                     3970000.0|                             0.2362|                              21.37|                             1183.0|                           154500.0|                            2.812E7|TRWHXZL128F92D707D|International|     0|  0.0|[0.102,5.43,310.3...|[3.16079029101091...|       6.0| (20,[6],[1.0])|
#|                                        0.1728|                                         8.074|                                         389.9|                                       16270.0|                                     2439000.0|                             0.5708|                               35.3|                             1921.0|                           203200.0|                            3.491E7|TRTGOEP128F14624B8|          RnB|     0|  0.0|[0.1728,8.074,389...|[4.59931342788059...|       5.0| (20,[5],[1.0])|
#|                                        0.1496|                                         8.423|                                         495.5|                                       39800.0|                                     6195000.0|                             0.2331|                              23.59|                             1344.0|                           163100.0|                            2.897E7|TRPMXLE128E078357A|         Jazz|     0|  0.0|[0.1496,8.423,495...|[4.22960036160672...|       3.0| (20,[3],[1.0])|
#|                                       0.08942|                                         9.638|                                         453.3|                                       26220.0|                                     3497000.0|                             0.1996|                               17.8|                              791.4|                            59990.0|                          7369000.0|TRXUGLA128F933CD10|      Country|     0|  0.0|[0.08942,9.638,45...|[8.16361377267736...|       7.0| (20,[7],[1.0])|
#|                                        0.1937|                                         17.47|                                         719.0|                                       48500.0|                                     6995000.0|                             0.3801|                              32.89|                             1862.0|                           167900.0|                            2.849E7|TRGHUFG128F4227016|          Rap|     1|  1.0|[0.1937,17.47,719...|[5.42515759450208...|       2.0| (20,[2],[1.0])|
#+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+-------------+------+-----+--------------------+--------------------+----------+---------------+
#only showing top 20 rows




from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics

########################### LogisticRegression ##################

logreg_model_multi = LogisticRegression(maxIter=20, elasticNetParam=0.0,labelCol='genreIndex')
logreg_multiclass_model = logreg_model_multi.fit(encoded_train_sample)
logreg_multiclass_predict = logreg_multiclass_model.transform(stratified_test)

logreg_multiclass_evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
logreg_multiclass_accuracy = logreg_multiclass_evaluator.evaluate(logreg_multiclass_predict)
print(logreg_multiclass_accuracy)
0.30209546728792513

logreg_multi_results = logreg_multiclass_predict.select(['prediction', 'label'])
logreg_multi_rdd=logreg_multi_results.rdd
logreg_metrics = MulticlassMetrics(logreg_multi_rdd)

print("Weighted recall              : %s" % logreg_metrics.weightedRecall)
print("Weighted precision           : %s" % logreg_metrics.weightedPrecision)
print("Weighted F(1) Score          : %s" % logreg_metrics.weightedFMeasure())
print("Weighted F(0.5) Score        : %s" % logreg_metrics.weightedFMeasure(beta=0.5))
print("Weighted false positive rate : %s" % logreg_metrics.weightedFalsePositiveRate)
Weighted recall              : 0.3028917179450946
Weighted precision           : 0.9056744909681764
Weighted F(1) Score          : 0.44907185839693975
Weighted F(0.5) Score        : 0.6422564513657373
Weighted false positive rate : 0.08642979734380989

##onevsrest
one_vs_rest = OneVsRest(classifier=logreg_model_multi)
ovr_multiclass = one_vs_rest.fit(encoded_train_sample)
ovr_multiclass_predict = ovr_multiclass.transform(stratified_test)


ovr_multiclass_evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
ovr_multiclass_accuracy = ovr_multiclass_evaluator.evaluate(ovr_multiclass_predict)
print(ovr_multiclass_accuracy)
0.9133089917975882


ovr_results = ovr_multiclass_predict.select(['prediction', 'label'])
ovr_results_rdd=ovr_results.rdd
ovr_metrics = MulticlassMetrics(ovr_results_rdd)
# Overall statistics
ovr_precision = ovr_metrics.precision()
ovr_recall = ovr_metrics.recall()
ovr_f1Score = ovr_metrics.fMeasure()
print("Summary Statistics")
print("Precision = %s" % ovr_precision)
print("Recall = %s" % ovr_recall)
print("F1 Score = %s" % ovr_f1Score)
Summary Statistics
Precision = 0.9135780356434631
Recall = 0.9135780356434631
F1 Score = 0.9135780356434631


# Weighted stats
print("Weighted recall = %s" % ovr_metrics.weightedRecall)
print("Weighted precision = %s" % ovr_metrics.weightedPrecision)
print("Weighted F(1) Score = %s" % ovr_metrics.weightedFMeasure())
print("Weighted F(0.5) Score = %s" % ovr_metrics.weightedFMeasure(beta=0.5))
print("Weighted false positive rate = %s" % ovr_metrics.weightedFalsePositiveRate)

Weighted recall = 0.9135780356434631
Weighted precision = 0.88422615044317
Weighted F(1) Score = 0.8930202515157615
Weighted F(0.5) Score = 0.8849037580882616
Weighted false positive rate = 0.8054422439997383



