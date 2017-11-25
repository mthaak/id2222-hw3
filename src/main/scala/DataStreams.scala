import java.util.Random

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

class TriestBase(M: Int) extends RichMapFunction[(Int, Int), Double] {
  private val reservoir = ArrayBuffer[(Int, Int)]() // reservoir of max size M
  private val random = new Random()
  private var t = 0
  private var tau = 0

  override def map(newEdge: (Int, Int)): Double = {
    t += 1
    if (t <= M) {
      // Append new edge and update counter
      reservoir.append(newEdge)
      updateCounter(+1, newEdge)
    }
    else if (flipBiasedCoin(M.toFloat / t)) {
      // Pick edge to be replaced
      val index = random.nextInt(M)
      val replaceEdge = reservoir(index)

      // Remove edge and update counter
      reservoir.update(index, (0, 0))
      updateCounter(-1, replaceEdge)

      // Put new edge and update counter
      reservoir.update(index, newEdge)
      updateCounter(+1, newEdge)
    }

    // Calculate xi (the cast to Long is important to prevent integer overflow)
    val a = t.toLong * (t.toLong - 1) * (t.toLong - 2)
    val b = M.toLong * (M.toLong - 1) * (M.toLong - 2)
    val xi = Math.max(1, a.toDouble / b)

    xi * tau // approximation of number of global triangles
  }

  private def updateCounter(diff: Int, edge: (Int, Int)): Unit = {
    // Increment or decrement tau for each common neighbour
    val neighbours = (u: Int) => reservoir.collect({ case (s, t) if s == u => t; case (s, t) if t == u => s }).toSet
    val commonNeighbours = neighbours(edge._1) intersect neighbours(edge._2)
    tau += commonNeighbours.size * diff
  }

  private def flipBiasedCoin(prob: Float) = random.nextFloat() < prob

}

object DataStreams {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging

    // http://konect.uni-koblenz.de/networks/arenas-pgp
    val text = env.readTextFile("data/arenas-pgp/out.arenas-pgp")

    val s = 1000 // size of sample
    val w = 1000 // size of window for printing results

    // Count number of global triangles using TRIÈST-BASE algorithm
    val countGlobalTriangles = text
      .filter(line => line.nonEmpty && !line.startsWith("%")) // filter out non-relevant lines
      .map(_.split("\\s+") match { case Array(u, v) => (u.toInt, v.toInt) }) // convert lines to edge tuples
      .map(new TriestBase(s)).setParallelism(1) // run algorithm in sequence

    // Print number of triangles every w edges (there are supposed to be exactly 54,788 triangles)
    countGlobalTriangles
      .countWindowAll(w)
      .max(0)
      .print().setParallelism(1)

    // Run pipeline
    env.execute("Approximation global triangles from stream")
  }

}
