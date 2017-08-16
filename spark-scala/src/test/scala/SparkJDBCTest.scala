//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit.Test
//
//class SparkJDBCTest {
//
//	@Test
//	def test() {
//
//		val conf = new SparkConf().setAppName("").setMaster("")
//		val context = new SparkContext(conf)
//		val sales = context.parallelize(List(
//			("West", "Apple", 2.0, 10),
//			("West", "Apple", 3.0, 15),
//			("West", "Orange", 5.0, 15),
//			("South", "Orange", 3.0, 9),
//			("South", "Orange", 6.0, 18),
//			("East", "Milk", 5.0, 5))
//		)
//
//		sales.map{
//			case (store, prod, amt, units) => {
//				((store, prod), (amt, amt, amt, units))
//			}
//		}
//		.reduceByKey((x, y) => {
//			(x._1 + y._1, math.min(x._2, y._2), math.max(x._3, y._3), x._4 + y._4)
//		}).foreach({ ((x, y), ())
//			print(it)
//		})
//	}
//
//}
