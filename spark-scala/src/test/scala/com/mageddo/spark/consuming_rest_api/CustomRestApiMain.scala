package com.mageddo.spark.consuming_rest_api

import java.lang.Math._

import org.apache.spark.{SparkConf, SparkContext}
;

object CustomRestApiMain {

	/**
		* Withdraw users request on batches of 100k
		*
		* @param args
		*/
	def main(args: Array[String]) {
		val ctx = getContext

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
			val rdd = ctx.range(startRestPage, endRestPage)

			rdd.map{ page =>

				// download page withdraws
				// myRest?page=1

				(fileNumber, "Marjorie", 10.95) //, ("Mark", 8.98)
			}
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
