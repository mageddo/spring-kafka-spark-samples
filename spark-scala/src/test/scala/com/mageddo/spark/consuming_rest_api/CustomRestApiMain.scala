package com.mageddo.spark.consuming_rest_api

import java.lang.Math._
import java.lang.String.valueOf
import java.lang.System._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

object CustomRestApiMain {

	/**
		* Withdraw users request on batches
		*
		* @param args
		*/
	def main(args: Array[String]) {

		val sc = getContext

		val restPageSize = 10
		// count restPages GET /page/count
		val restPages = 100

		// fileBatchSize is 100 registers
		val fileBatchSize = 100
		val sparkFiles = ceil(restPages * restPageSize / fileBatchSize).toInt

		// will generate n files
		for(fileNumber <- 1 to sparkFiles){

			val startRestPage = (fileNumber - 1) * restPageSize + 1
			val endRestPage = fileNumber * restPageSize
			sc.range(startRestPage, endRestPage)
			.flatMap{ page =>

				// download page withdraws
				// myRest?page=1

				List((fileNumber, "Marjorie", 10.95), (fileNumber, "Mark", 8.98))
			}
			//.saveAsTextFile(s"${nanoTime()}.txt") // here you can save every batch to a separated file

			.foreach { case (fileNumber, name, value) =>
				println(fileNumber, name, value)
			}
			println("===============================")

		}

	}

	def getContext: SparkContext = {
		val conf = new SparkConf()
		conf.setMaster("local")
		conf.setAppName("rest api")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		return sc
	}
}
