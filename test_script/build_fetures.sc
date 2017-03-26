import org.apache.log4j._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, min, max, mean}
import org.apache.spark.sql.expressions.Window


// Set the log level to only print errors
Logger.getLogger("org").setLevel(Level.ERROR)

val list_test = Seq("Cliente_ID", "Producto_ID", "3", "4", "5","6" , "7", "label", "3_min_sc")
list_test.filter(_ != "Cliente_ID").filter(_ != "Producto_ID")
