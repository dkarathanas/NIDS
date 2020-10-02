import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.RegressionMetrics
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark
import org.apache.spark.mllib.evaluation.RegressionMetrics



def stringToLabeledPoint(str: String):LabeledPoint = {
    
    val lala = str.split(",").map(x=>x.toDouble).toList
    val vector0 = lala(0)
    val vector :Vector = Vectors.dense(lala.drop(1).toArray)

    LabeledPoint(vector0, vector)
}

def baseLineModel(point:LabeledPoint):Int = {
    averageTrainData
}


def calcRmse(rdd:RDD[(Double,Double)]):Double = {

    // Instantiate metrics object
    val metrics = new RegressionMetrics(rdd)

    metrics.rootMeanSquaredError
}

val baseRdd = sc.textFile("/home/mitsos/Desktop/hy543/Assignment2/hw2_files/dataset.csv")
baseRdd.take(1).map(line => stringToLabeledPoint(line))

baseRdd.count()
baseRdd.take(5).foreach(println)

val parsedPointsRdd = baseRdd.map(stringToLabeledPoint)


println("---------------  1.3.3 ---------------")
println(parsedPointsRdd.take(1).foreach(x=>println(x.label)))

println("---------------  1.3.4 ---------------")
println(parsedPointsRdd.take(1).foreach(x=>println(x.features)))

println("---------------  1.3.5 ---------------")
println(parsedPointsRdd.take(1).foreach(x=>println(x.features.size)))

println("---------------  1.3.6 ---------------")
val max = parsedPointsRdd.map(x=>(x.label)).max
println(max)

println("---------------  1.3.6 ---------------")
val min = parsedPointsRdd.map(x=>(x.label)).min
println(min)

val shiftedPointsRdd = parsedPointsRdd.map(x=>(LabeledPoint((x.label)-min,x.features)))

shiftedPointsRdd.take(1).map(line => println(line))

// println("------import org.apache.spark.mllib.evaluation.RegressionMetrics-------")
// val maxNew = shimport org.apache.spark.mllib.evaluation.RegressionMetricslabel)).max
// println(maxNew)import org.apache.spark.mllib.evaluation.RegressionMetrics

println("---------------  1.4.2 ---------------")
val minNew = shiftedPointsRdd.map(x=>(x.label)).min
println(minNew)

println("---------------  1.4.2 ---------------")
val maxNew = shiftedPointsRdd.map(x=>(x.label)).max
println(maxNew)


val weights = Array(.8, .1, .1)
val seed = 42

val Array(trainData, valData, testData) = shiftedPointsRdd.randomSplit(weights,seed)
trainData.cache
valData.cache
testData.cache

println("---------------  1.5.3 ---------------")
println("shiftedPointsRdd count = "+shiftedPointsRdd.count())
println("trainData count = "+trainData.count())
println("valData count = "+valData.count())
println("testData count = "+testData.count())
println("They add up to shiftedPointsRdd count so we are good")


val averageTrainData = (trainData.map(x=>(x.label)).sum / trainData.count).toInt

println("---------------    2.1.2   ---------------")
val stupidPrediction = baseLineModel(valData.take(1).head)


val valuesAndPreds = shiftedPointsRdd.map{ point =>
  (stupidPrediction.toDouble, point.label)
}

println("---------------    2.2   ---------------")
println("RMSE = "+ calcRmse(valuesAndPreds))

val predsNLabelsTrain = trainData.map{ point =>
  (stupidPrediction.toDouble, point.label)
}

val predsNLabelsVal = valData.map{ point =>
  (stupidPrediction.toDouble, point.label)
}

val predsNLabelsTest = testData.map{ point =>
  (stupidPrediction.toDouble, point.label)
}

println("---------------    2.3   ---------------")
println("RMSE of predsNLabelsTrain = "+ calcRmse(predsNLabelsTrain))
println("RMSE of predsNLabelsVal = "+ calcRmse(predsNLabelsVal))
println("RMSE of predsNLabelsTest = "+ calcRmse(predsNLabelsTest))







import breeze.linalg.DenseVector


def gradientSummand(weights: DenseVector[Double], lp: LabeledPoint):DenseVector[Double] = {
  val tempDense = DenseVector(lp.features.toArray)
  val tempDot = weights dot tempDense
  val tempGradientSummand = (tempDot-lp.label)*tempDense
  tempGradientSummand
}


//Test gradientSummand
val example_w = DenseVector(1.0, 1.0, 1.0)
val example_lp = LabeledPoint(2.0, Vectors.dense(3, 1, 4))
println(gradientSummand(example_w, example_lp))

val example_w = DenseVector(.24, 1.2, -1.4)
val example_lp = LabeledPoint(3.0, Vectors.dense(-1.4, 4.2, 2.1))
println(gradientSummand(example_w, example_lp))

def getLabeledPrediction(weights: DenseVector[Double], lp :LabeledPoint):(Double,Double) = {
  val tempDense = DenseVector(lp.features.toArray)
  val predict = weights dot tempDense
  (predict,lp.label)
}
println(getLabeledPrediction(example_w,example_lp))


/*Unfinished implementation of lrgd. The function takes as input an
RDD of LabeledPoints and the number of Iterations and returns the model
parameters, and the list of rmse for each iteration. Fill with code the 
question marks (?) and debug.
*/
import scala.collection.mutable.ListBuffer
def lrgd(trData: RDD[LabeledPoint], numIter: Int): (DenseVector[Double], List[Double]) = {
  val n = trData.count
  val d = trData.first.features.size
  val alpha =  pow(2,-10)
  val errorTrain = new ListBuffer[Double] 
  var weights = new DenseVector(Array.fill[Double](d)(0.0))
  for (i <- 0 until numIter){ 
    val gradient = trData.map(x => gradientSummand(weights,x)).reduce(_+_)
    val alpha_i = alpha / (n * Math.sqrt(i+1))
    weights = weights - (alpha_i * gradient)
    //update errorTrain
    val predsNLabelsTrain = trData.map(x => getLabeledPrediction(weights,x)) //convert the training set into an RDD of (predictions, labels)
    println("Per iteration RMSE:"+calcRmse(predsNLabelsTrain))
    errorTrain += calcRmse(predsNLabelsTrain)
  }
  (weights, errorTrain.toList)
}

/*********   test lrgd *********/
val exampleN = 4
val exampleD = 3
val exampleData = sc.parallelize(trainData.take(exampleN)).map(lp => LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.slice(0, exampleD))))
val exampleNumIters = 50
val (exampleWeights, exampleErrorTrain) = lrgd(exampleData, exampleNumIters)

println("--------------------   3.3.5   --------------------")
println("NUMBER OF ITERATIONS = "+ exampleNumIters)
println("WEIGHTS = "+ exampleWeights)
println("ERROR TRAIN = "+ exampleErrorTrain)


val valiData = sc.parallelize(valData.take(exampleN)).map(lp => LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.slice(0, exampleD))))
val predsNLabelsVal = valiData.map(x => ((new DenseVector(x.features.toArray) dot exampleWeights), x.label))
val calc = calcRmse(predsNLabelsVal)
println("RMSE on validation set = "+ calc)
/*******************************/

/********** MLLib + grid search*****************************/
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.ml.feature.{LabeledPoint => MLabeledPoint}
import org.apache.spark.ml.evaluation.RegressionEvaluator


/*********************RDD conversion to Dataframe*****************/
val trainDataDF = trainData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val valDataDF = valData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val testDataDF = testData.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
/*******************************************************************/

/******Linear Regression Demo*********/
val lr = new LinearRegression().setMaxIter(50).setRegParam(1).setFitIntercept(true)
val lrModel = lr.fit(trainDataDF)
// lrModel.evaluate(valDataDF).rootMeanSquaredError

println("---------------    4.1.1   ---------------")
println("lrModel:  coefficient = " + lrModel.coefficients + "   intercept = "+ lrModel.intercept)

val trainingSummary = lrModel.summary

println("---------------    4.1.2   ---------------")
val predictions = lrModel.transform(valDataDF)
val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println("RMSE on validation data is: " + rmse);
println("The first 10 predictions are:");
predictions.select("prediction", "label", "features").show(10)

val lr1 = new LinearRegression().setMaxIter(50).setRegParam(1e-10).setFitIntercept(true)
val lrModel1 = lr1.fit(trainDataDF)
println("RMSE for 1e-10 = "+lrModel1.evaluate(valDataDF).rootMeanSquaredError)


val lr2 = new LinearRegression().setMaxIter(50).setRegParam(1e-5).setFitIntercept(true)
val lrModel2 = lr2.fit(trainDataDF)
println("RMSE for 1e-5 = "+lrModel2.evaluate(valDataDF).rootMeanSquaredError)


/***************************************/


/***** Quadratic Feature extraction for 5.1 ********/
implicit class Crossable[X](xs: Traversable[X]) {
  def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
}

def quadFeatures(lp: LabeledPoint) = {
  val crossFeatures = lp.features.toArray.toList cross lp.features.toArray.toList
  val sqFeatures = crossFeatures.map(x => x._1 * x._2).toArray
  LabeledPoint(lp.label, Vectors.dense(sqFeatures))
}
val crossTrainDataRDD = trainData.map(lp => quadFeatures(lp))
val crossValDataRDD = valData.map(lp => quadFeatures(lp))
val crossTestDataRDD = testData.map(lp => quadFeatures(lp))

val crossTrainDataDF = crossTrainDataRDD.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val crossValDataDF = crossValDataRDD.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF
val crossTestDataDF = crossTestDataRDD.map(lp => MLabeledPoint(lp.label, MLVectors.dense(lp.features.toArray))).toDF

val newlr = new LinearRegression().setMaxIter(500).setRegParam(1e-10).setFitIntercept(true)
val newlrModel = lr.fit(crossTrainDataDF)
newlrModel.evaluate(crossValDataDF).rootMeanSquaredError


println("RMSE for crossModel:"+newlrModel.evaluate(crossValDataDF).rootMeanSquaredError)

println("RMSE for baseLineModel:"+calcRmse(predsNLabelsVal))

val tempTestset = newlrModel.transform(crossTestDataDF)
tempTestset.select("prediction").show(50)


/*****************************************************/


/**** Use of pipelines ******************************/
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator

val numIters = 500
val reg = 1e-10
val alpha = .2
val useIntercept = true
val polynomial_expansion = (new PolynomialExpansion).setInputCol("features").setOutputCol("polyFeatures").setDegree(2)
val lr3 = new LinearRegression()
lr3.setMaxIter(numIters).setRegParam(reg).setElasticNetParam(alpha).setFitIntercept(useIntercept).setFeaturesCol("polyFeatures")

val pipeline = new Pipeline()


pipeline.setStages(Array(polynomial_expansion,lr3)) //there are two stages here that you have to set.

val model = pipeline.fit(trainDataDF) //need to fit. Use the train Dataframe
val predictionsDF = model.transform(testDataDF)//Produce predictions on the test set. Use method transform.
val evaluator = new RegressionEvaluator()
evaluator.setMetricName("rmse")
val rmseTestPipeline = evaluator.evaluate(predictionsDF)
println(rmseTestPipeline)
