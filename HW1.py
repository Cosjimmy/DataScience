from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.ml.classification import LogisticRegression,DecisionTreeClassifier,RandomForestClassifier
from pyspark.sql.functions import lit, col,udf #udf for user-defined-function map on "Name"
from pyspark.sql.types import StringType
from pyspark.mllib.linalg import DenseVector



train_path="./train.csv"
test_path="./test.csv"
sc = SparkContext()
#load csv to rdd
train_rdd = sc.textFile('file:///home/hadoop/DS/DataScienc/train.csv')
#test_rdd = sc.textFile(test_path)
#test_rdd.take(30)
train_rdd.take(30)

#RDD to DataFrame
def parseTrain(rdd):
    header = rdd.first()#get header
    body = rdd.filter(lambda r:r!=header) #remove header
    #print("Hello")
    def parseRow(row):
        #remove " and split rows by ,
        row_list = row.replace('&quot;','').split('&quot;','&quot;') #check quot uses 
        #convert python list to tuple
        row_tuple = tuple(row_list)
        return row_tuple

    rdd_parsed = body.map(parseRow)
   
    colnames = header.split
    colnames.insert(3,'FirstName')

    return rdd_parsed.toDF(colnames)

def parseTest(rdd):
    header = rdd.first()
    body = rdd.filter(lambda r: r != header)

    #same parseRow as parse TrainData
    def parseRow(row):
        row_list = row.replace('&quot;','').split('&quot;','&quot;')  #check quot
        row_tuple = tuple(row_list)
        return row_tuple

    rdd_parsed = body.map(parseRow)

    colnames = header.split(''',''')
    colnames.insert(2,'FirstName')

    return rdd_parsed.toDF(colnames)


train_df = parseTrain(train_rdd)
test_df = parseTest(test_rdd)

#Combine Train and Test Data
train_df = train_df.withColumn('Mark',lit('train'))
test_df = (test_df.withColumn('Survived',lit(0)).withColum('Mark',lit('test')))
test_df = test_df[train_df.columns]
#Append test data to train data
df = train_df.unionAll(test_df)


#data formatting
#covert features to numbers
#print(df.take(10))
df = (df.withColumn('Age',df['Age'].cast('double')).withColumn('SibSp',df['SibSp'].cast('double')).withColumn('Parch',df['Parch'].cast('double')).withColumn('Survived',df['Survived'].cast('double')).withColumn('Fare',df['Fare'].cast('double')))

df.printSchema()

#in some case there are null data so I made it to average
numberVars = ['Survived','Age','SibSp','Parch','Fare']
def countNull(df,var):
    return df.where(df[var].isNull()).count()

missing = {var: countNull (df,var) for var in numverVars}
age_mean = df.groupBy().mean('Age').first()[0]
fare_mean = df.groupBy().mean('Fare').first()[0]
df = df.na.fill({'Age':age_mean,'Fare':fare_mean})


#Seperate Title 
getTitle = udf(lambda name: name.split('.')[0]/strip(),StringType())
df = df.withColumn('Title',getTitle(df['Name']))

df.select('Name','Title').show(3)

#enum variables
enumVars = ['Pclass','Sex','Embarked','Title']

from pyspark.ml.feature import StringIndexer
si = StringIndexer(inputCol = 'Sex', outputCol = 'Sex_indexed')
df_indexed = si.fit(df).transform(df).drop('Sex').withColumnRenamed('Sex_indexed','Sex')

## make use of pipeline to index all enum vars
def indexbuilder(df,col):
    si = StringIndexer(inputCol = col, outputCol = col +'_indexed').fit(df)
    return si
indexers = [indexer(df,col) for col in enumVars]

from pyspark.ml import Pipeline
pipeline = Pipeline(stages = indexers)
df_indexed = pipeline.fit(df).transform(df)

#S=>0 C=>1 Q=>2
df_indexed.select('Embarked','Embarked_indexed').show(3)

#covert features to vectors
enumVarsIndexed = [i + '_indexed' for i in enumVars]
featuresCol = numVars + catVarsIndexed
featuresCol.remove('Survived')
lableCol = ['Mark','Survived']
row = Row('mark','label','features')

#0 1 2 map
df_indexed = df_indexed[labelCol+featuresCol]
lf = df_indexed.map(lambda r:(row(r[0], r[1],DenseVector(r[2:])))).toDF()

lf = StringIndexer(inputCol = 'label',output = 'index').fit(lf).transform(lf)

lf.show(3)

#seperate train/test data
train = lf.where(lf.mark == 'train')
test = lf.where(lf.mark == 'test')

train, validation = train.randomSplit([0.8,0.2],seed = 110)

print 'Training Data Number: ' + str(train.count())
print 'Validation Date Number '+ str(validation.count())
print 'Test Data Number '+ str(test.count())


#Model for logistic Regression
lr =LogisticRegression(maxIter = 80 , regParam = 0.05, labelCol = 'index' ).fit(train)
#evaluate by auc ROC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
def testModel(model, validation = valadation):
    pred = model.transform (validation)
    evaluator = BinaryClassificationEvaluator(labelCol = 'index')
    return evaluator.evaluate(prod)
#show result
print 'AUC ROC of Logistic Regression model is: '+str(testModel(lr))

#decision tree model
dt = DecisionTreeClassifier(maxDepth = 3, labelCol = 'index')
rf = RandomForestClassifier(numTrees = 100, labelCol = 'index').fit(train)

models = {'LogisticRegression': lr, 'DecisionTree': dt, 'RandomForest' : rf}

#compare model perfermance 
modelPerf = {k:testModel(v) for k, v in models.iteritems()}

print modelPerf
