/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ctm.sparkwordcount

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

    def main(args: Array[String]) {
      val conf = new SparkConf()
      conf.set("spark.app.name", "SparkWordCount")
      conf.set("spark.master", "local[4]")
      conf.set("spark.ui.port", "36000") // Override the default port

      // Create a SparkContext with this configuration
      val sc = new SparkContext(conf.setAppName("SparkWordCount"))
    val textFile = sc.textFile("src/main/resources/inputfile.txt")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)
    counts.take(50).foreach(println)
  }
}
