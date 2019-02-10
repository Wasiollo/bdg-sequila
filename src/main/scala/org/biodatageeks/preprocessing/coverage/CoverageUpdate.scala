package org.biodatageeks.preprocessing.coverage

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

case class RightCovEdge(contigName:String,minPos:Int,startPoint:Int,cov:Array[Short], cumSum:Short)

case class ContigRange(contigName:String, minPos: Int, maxPos:Int)


class CovUpdate(var right:ArrayBuffer[RightCovEdge],var left: ArrayBuffer[ContigRange]) extends Serializable {

  def reset(): Unit = {
    right = new ArrayBuffer[RightCovEdge]()
    left = new ArrayBuffer[ContigRange]()
  }
  def add(p:CovUpdate): CovUpdate = {
    right = right ++ p.right
    left = left ++ p.left
    return this
  }

}

class CoverageAccumulatorV2(var covAcc: CovUpdate) extends AccumulatorV2[CovUpdate, CovUpdate] {
  //private val covAcc = new CovUpdate(new ArrayBuffer[RightCovEdge](),new ArrayBuffer[ContigRange]())

  def reset(): Unit = {
    covAcc =  new CovUpdate(new ArrayBuffer[RightCovEdge](),new ArrayBuffer[ContigRange]())
  }

  def add(v: CovUpdate): Unit = {
    covAcc.add(v)
  }
  def value():CovUpdate = {
    return covAcc
  }
  def isZero(): Boolean = {
    return (covAcc.right.isEmpty && covAcc.left.isEmpty)
  }
  def copy():CoverageAccumulatorV2 = {
    return new CoverageAccumulatorV2 (covAcc)
  }
  def merge(other:AccumulatorV2[CovUpdate, CovUpdate]) = {
    covAcc.add(other.value)
  }
}

case class RightCovSumEdge(contigName: String, maxPos: Int, cov: Array[Short])


class CovSumUpdate(var sumArray: ArrayBuffer[RightCovSumEdge]) extends Serializable {

  def reset(): Unit = {
    sumArray = new ArrayBuffer[RightCovSumEdge]()
  }

  def add(p: CovSumUpdate): CovSumUpdate = {
    sumArray = sumArray ++ p.sumArray
    return this
  }

}

class CovSumAccumulatorV2(var covAcc: CovSumUpdate) extends AccumulatorV2[CovSumUpdate, CovSumUpdate] {

  def reset(): Unit = {
    covAcc = new CovSumUpdate(new ArrayBuffer[RightCovSumEdge]())
  }

  def add(v: CovSumUpdate): Unit = {
    covAcc.add(v)
  }

  def value(): CovSumUpdate = {
    return covAcc
  }

  def isZero(): Boolean = {
    return covAcc.sumArray.isEmpty
  }

  def copy(): CovSumAccumulatorV2 = {
    return new CovSumAccumulatorV2(covAcc)
  }

  def merge(other: AccumulatorV2[CovSumUpdate, CovSumUpdate]) = {
    covAcc.add(other.value)
  }
}

