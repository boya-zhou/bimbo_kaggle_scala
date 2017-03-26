/**
  * Created by boyazhou on 3/20/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.log4j._
import org.apache.spark.sql.functions._


// Set the log level to only print errors
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().appName("build_features_sc").master("local[2]").getOrCreate()

import spark.implicits._

val path = "/Users/boyazhou/bimbo_kaggle_scala"
val origin_train = spark.read.option("header","true").option("inferSchema","true").csv(path + "/data/interm/train_sample.csv")


// select column, prepare for pivot, ["Semana", "Cliente_ID", "Producto_ID", "Demanda_uni_equil"]
val week_cli_pro_demand = origin_train.select($"Semana", $"Cliente_ID", $"Producto_ID", $"Demanda_uni_equil")
val week_cli_pro_demand_pivot = (week_cli_pro_demand
  .groupBy($"Cliente_ID",$"Producto_ID")
  .pivot("Semana").agg(avg($"Demanda_uni_equil")))


//val cli_id = "Cliente_ID"
//val avg_cli_demand = (week_cli_pro_demand_pivot.columns.toSet - cli_id).map(col_name => avg(col_name).as(col_name + "_avg_sc")).toList
//val sc_dist = week_cli_pro_demand_pivot.groupBy(cli_id).agg(avg_cli_demand.head, avg_cli_demand.tail:_*)
//
//val pro_id = "Producto_ID"
//val avg_pro_demand = (week_cli_pro_demand_pivot.columns.toSet - pro_id).map(col_name => avg(col_name).as(col_name + "_avg_sp")).toList
//val sp_dist = week_cli_pro_demand_pivot.groupBy(pro_id).agg(avg_pro_demand.head, avg_pro_demand.tail:_*)


