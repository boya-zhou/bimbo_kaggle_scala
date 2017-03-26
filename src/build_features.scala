/**
  * Created by boyazhou on 3/15/17.
  */
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.functions.{avg, col, lag, max, min, sum}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

object build_features {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("build_features")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val path = "/Users/boyazhou/bimbo_kaggle_scala"
    val origin_train = spark.read.option("header","true").option("inferSchema","true").csv(path + "/data/interm/train_sample.csv")
//    val origin_test = spark.read.option("header","true").option("inferSchema","true").csv(path + "/data/interm/test_sample.csv")

    /**
      * train_df ==> pivot_table ==> gen_se_dist  ==> merge info(train holdout and test)
      * |                                                  |
      * |                                                  |
      * |===> create_lag ==================================|
      * |                                                  |
      * |==> gen_pro_cli_dist =============================|
      * |                                                  |
      * |                                                  |
      * |==> external_info ================================|
      */

    // TODO : add external_info
    // TODO : package in sbt
    // TODO : deploy on AMS EMR

    def pivot_df(origin_train : DataFrame, type_df : String) : DataFrame ={
      // select column, prepare for pivot, ["Semana", "Cliente_ID", "Producto_ID", "Demanda_uni_equil"]
      val week_cli_pro_demand = origin_train.select($"Semana", $"Cliente_ID", $"Producto_ID", $"Demanda_uni_equil")
      val week_cli_pro_demand_pivot = week_cli_pro_demand
        .groupBy($"Cliente_ID",$"Producto_ID")
        .pivot("Semana").agg(avg($"Demanda_uni_equil"))

      val se_cli_pro_pivot = {
        if (type_df == "train"){
          week_cli_pro_demand_pivot.select($"Cliente_ID",$"Producto_ID", $"3",$"4",$"5",$"6",$"7",$"9".alias("label"))
        }else if (type_df == "holdout"){
          week_cli_pro_demand_pivot.select($"Cliente_ID",$"Producto_ID",$"4",$"5",$"6",$"7",$"8")
        }else{
          week_cli_pro_demand_pivot.select($"Cliente_ID",$"Producto_ID",$"5",$"6",$"7",$"8",$"9")
        }
      }
      se_cli_pro_pivot
    }

    def gen_se_dist(week_cli_pro_demand_pivot :DataFrame, type_df: String): List[DataFrame] = {
      // gen_se_dist, generate distribution info for week-productId pair and week-clientId pair
      val mapping: Map[String, Column => Column] = Map(
        "min" -> min, "max" -> max, "avg" -> avg, "sum" -> sum)

      val group_cli = Seq("Cliente_ID")
      val group_pro = Seq("Producto_ID")
      val group_cli_pro = Seq("Cliente_ID", "Producto_ID")

      val aggregate = {
        if (type_df == "train"){
          Seq("3", "4", "5", "6", "7")
        }else if(type_df == "holdout"){
          Seq("4", "5", "6", "7", "8")
        }else{
          Seq("5", "6", "7", "8", "9")
        }
      }
      val operations = Seq("min", "max", "avg", "sum")
      val exprs = aggregate.flatMap(c => operations.map(f => mapping(f)(col(c))))

      val cli_id = "Cliente_ID"
      val pro_id = "Producto_ID"

      val se_cli_demand = week_cli_pro_demand_pivot.groupBy(group_cli.map(col): _*).agg(exprs.head, exprs.tail: _*)
      val se_pro_demand = week_cli_pro_demand_pivot.groupBy(group_pro.map(col): _*).agg(exprs.head, exprs.tail: _*)
      val se_cli_pro_demand = week_cli_pro_demand_pivot.groupBy(group_cli_pro.map(col): _*).agg(exprs.head, exprs.tail: _*)

      def transfer_name(origin_name : String, postfix : String) :String = {
        if (origin_name.charAt(3) == '(') {
          origin_name.charAt(4) + "_" + origin_name.take(3) + "_" + postfix
        }
        else{
          origin_name
        }
      }

      val sc_name = se_cli_demand.columns.map(transfer_name(_, "sc"))
      val sp_name = se_pro_demand.columns.map(transfer_name(_, "sp"))
      val scp_name = se_cli_pro_demand.columns.map(transfer_name(_, "spc"))

      val sc_dist = se_cli_demand.toDF(sc_name: _*)
      val sp_dist = se_pro_demand.toDF(sp_name: _*)
      val scp_dist = se_cli_pro_demand.toDF(scp_name: _*)

      List(sc_dist, sp_dist, scp_dist)
    }

    def gen_pro_cli_dist(origin_train : DataFrame, type_df : String) : DataFrame = {
      /**train data only need info from week3 to week7, and holdout data only need week4 to week8**/

      //    get dist info about producto_id and cliente_id pairs
      val mapping: Map[String, Column => Column] = Map(
        "min" -> min, "max" -> max, "avg" -> avg, "sum" -> sum)

      val group_cli_pro = Seq("Cliente_ID", "Producto_ID")
      val aggregate = Seq("Semana")
      val operations = Seq("min", "max", "avg", "sum")
      val exprs = aggregate.flatMap(c => operations.map(f => mapping(f)(col(c))))

      def transfer_name(origin_name : String, postfix : String) :String = {
        if (origin_name.charAt(3) == '(') {
          origin_name.slice(4, 4 + 6) + "_" + origin_name.take(3) + "_" + postfix
        }
        else{
          origin_name
        }
      }

      val cli_pro_demand = {
        if (type_df == "train"){
          origin_train.filter(($"Semana" !== 9) || ($"Semana" !== 8)).groupBy(group_cli_pro.map(col): _*).agg(exprs.head, exprs.tail: _*)
        }else if (type_df == "holdout"){
          origin_train.filter(($"Semana" !== 3) || ($"Semana" !== 9)).groupBy(group_cli_pro.map(col): _*).agg(exprs.head, exprs.tail: _*)
        }else{
          origin_train.filter(($"Semana" !== 3) || ($"Semana" !== 4)).groupBy(group_cli_pro.map(col): _*).agg(exprs.head, exprs.tail: _*)
        }
      }

      val sc_name = cli_pro_demand.columns.map(transfer_name(_, "cp"))
      val cli_pro_dist = cli_pro_demand.toDF(sc_name: _*)
      cli_pro_dist
    }

    def create_lag(origin_train : DataFrame,type_df : String) : List[DataFrame] = {
      /**create time lag between demand among each week
         |if type_df == train : lag1 and lag2 for week3 to week7
         |if type_df == holdout : lag1 and lag2 for week4 to week8
         |if type_df == test : lag1 and lag2 for week5 to week9
      **/

      val lag_window = Window.partitionBy("Cliente_ID", "Producto_ID").orderBy("Semana")

      val train_interim = {
        if (type_df == "train"){
          origin_train.filter(($"Semana" !== 8) && ($"Semana" !== 9))
        }else if (type_df == "holdout"){
          origin_train.filter(($"Semana" !== 3) && ($"Semana" !== 9))
        }else{
          origin_train.filter(($"Semana" !== 3) && ($"Semana" !== 4))
        }
      }
      val train_offset1 = train_interim.withColumn("offset1", lag('Semana, 1) over lag_window).na.fill(0)
      val train_offset2 = train_interim.withColumn("offset2", lag('Semana, 2) over lag_window).na.fill(0)

      val train_lag1 = train_offset1.withColumn("lag1", train_offset1("Semana") - train_offset1("offset1")).select($"Semana", $"Cliente_ID",$"Producto_ID", $"lag1")
      val train_lag2 = train_offset2.withColumn("lag2", train_offset2("Semana") - train_offset2("offset2")).select($"Semana", $"Cliente_ID",$"Producto_ID", $"lag2")

      val train_lag1_pivot = train_lag1
        .groupBy($"Cliente_ID",$"Producto_ID")
        .pivot("Semana").agg(sum($"lag1"))

      val train_lag2_pivot = train_lag2
        .groupBy($"Cliente_ID",$"Producto_ID")
        .pivot("Semana").agg(sum($"lag2"))

      def transfer_name(origin_name : String, postfix : String) :String = {
        if (origin_name.length == 1) {
          origin_name + "_" + postfix
        }
        else{
          origin_name
        }
      }

      val train_lag1_new_name = train_lag1_pivot.columns.map(transfer_name(_, "lag1"))
      val train_lag2_new_name = train_lag2_pivot.columns.map(transfer_name(_, "lag2"))

      val train_lag1_renamed = train_lag1_pivot.toDF(train_lag1_new_name: _*)
      val train_lag2_renamed = train_lag2_pivot.toDF(train_lag2_new_name: _*)

      List(train_lag1_renamed, train_lag2_renamed)
    }

    def merge_info(origin_train : DataFrame, type_df : String) : DataFrame = {
      /**right now do not use external data**/
      val week_cli_pro_demand_pivot = pivot_df(origin_train, type_df)
      // se_dist : sc_dist, sp_dist, scp_dist
      val se_dist = gen_se_dist(week_cli_pro_demand_pivot, type_df)
      val cli_pro_dist_list = gen_pro_cli_dist(origin_train, type_df)
      val train_lag_list = create_lag(origin_train, type_df)

      val pivot_sc = week_cli_pro_demand_pivot.join(se_dist(0), Seq("Cliente_ID"))
      val pivot_sc_sp = pivot_sc.join(se_dist(1), Seq("Producto_ID"))
      val pivot_sc_sp_scp = pivot_sc_sp.join(se_dist(2), Seq("Cliente_ID", "Producto_ID"))
      val pivot_sc_sp_scp_cp = pivot_sc_sp_scp.join(cli_pro_dist_list, Seq("Cliente_ID", "Producto_ID"))
      val pivot_sc_sp_scp_cp_lag1 = pivot_sc_sp_scp_cp.join(train_lag_list(0), Seq("Cliente_ID", "Producto_ID"))
      val pivot_sc_sp_scp_cp_lag1_lag2 = pivot_sc_sp_scp_cp_lag1.join(train_lag_list(1), Seq("Cliente_ID", "Producto_ID"))

      val train_joined = pivot_sc_sp_scp_cp_lag1_lag2.drop("Cliente_ID", "Producto_ID")

      train_joined
    }

    val train_joined = merge_info(origin_train, "train")

    def shou_data(df_to_show: DataFrame) : Unit = {
      df_to_show.printSchema()

      for(line <- df_to_show.head(10)){
        println(line)
        }
    }

    /**begin the machine learning model
      * due to the sepciality of Kaggle, the holdout result and test result need to get from Kaggle website
      * so here first use local train-test split to test the model
      */

    val assembler = (new VectorAssembler()
      .setInputCols(train_joined.columns.toList.filter(_!= "label").toArray)
      .setOutputCol("features"))

    val Array(training, test) = train_joined.randomSplit(Array(0.7, 0.3), seed = 423)

    // TODO : difference between GBTRegressor and GBTRegressionModel
    val gbt = new GBTRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(5)

    val pipeline = new Pipeline().setStages(Array(assembler, gbt))

    val model = pipeline.fit(training)

    val prediction = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(prediction)

    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }

}