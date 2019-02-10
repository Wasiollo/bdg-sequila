package pl.edu.pw.ii.biodatageeks.tests

import java.io.{OutputStreamWriter, PrintWriter}

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{SequilaSession, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.preprocessing.coverage.CoverageStrategy
import org.biodatageeks.utils.{BDGInternalParams, SequilaRegister}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CoverageTestSuite extends FunSuite with DataFrameSuiteBase with BeforeAndAfter with SharedSparkContext {

  val bamPath = getClass.getResource("/NA12878.slice.bam").getPath
  val bamMultiPath = getClass.getResource("/multichrom/NA12878.multichrom.bam").getPath
  val adamPath = getClass.getResource("/NA12878.slice.adam").getPath
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  val cramPath = getClass.getResource("/test.cram").getPath
  val refPath = getClass.getResource("/phix-illumina.fa").getPath
  val bedPath = getClass.getResource("/test-target.bed").getPath
  val tableNameBAM = "reads"
  val tableNameMultiBAM = "readsMulti"
  val tableNameADAM = "readsADAM"
  val tableNameCRAM = "readsCRAM"
  val tableNameTargets = "targets"
  val splitSize = "1000000"

  before {

    Metrics.initialize(sc)
    sc.addSparkListener(metricsListener)
    System.setSecurityManager(null)
    spark.sql(s"DROP TABLE IF EXISTS ${tableNameBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameMultiBAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameMultiBAM}
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamMultiPath}")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameCRAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameCRAM}
         |USING org.biodatageeks.datasources.BAM.CRAMDataSource
         |OPTIONS(path "${cramPath}", refPath "${refPath}")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameADAM}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameADAM}
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "${adamPath}")
         |
      """.stripMargin)

    spark.sql(s"DROP TABLE IF EXISTS ${tableNameTargets}")
    spark.sql(
      s"""
         |CREATE TABLE ${tableNameTargets}(contigName String, start Integer, end Integer, targetId String)
         |USING csv
         |OPTIONS (path "file:///${bedPath}", delimiter "\t")
         |
       """.stripMargin)

  }


  /*

      for InputSplitSize = 1000000 and table tableNameMultiBAM partition boundaries are as follows:
      chr1 : 34
      chrM : 7
      chrM : 7882
      chrM : 14402

   */

  test("BAM - bdg_coverage - windows") {

    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val windowLength = 100
    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks', '${windowLength.toString}')")

    assert(bdg.count == 267)
    assert(bdg.first().getInt(1) % windowLength == 0) // check for fixed window start position
    assert(bdg.first().getInt(2) % windowLength == windowLength - 1) // // check for fixed window end position
    assert(bdg.where("contigName == 'chr1' and start == 2700").first().getFloat(3) == 4.65.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 3200").first().getFloat(3) == 166.79.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 10000").first().getFloat(3) == 1.5522388.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 7800").first().getFloat(3) == 253.03001.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 14400").first().getFloat(3) == 134.7.toFloat) //value check [partition boundary]
    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check
  }

  test("BAM - bdg_coverage - windows with targets from table") {

    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val windowLength = 100
    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks', '${tableNameTargets.toString}')")

    assert(bdg.count == 34) // test-target.bed contains 34 lines (targets)

    /*
    * group of tests which check the same values as for const windows with 100 size
    * in bed file are only three chr1 targets -> it tests gaps between targets also
    */
    assert(bdg.where("contigName == 'chr1' and start == 2700").first().getFloat(3) == 4.65.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 3200").first().getFloat(3) == 166.79.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 10000").first().getFloat(3) == 1.5522388.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 7800").first().getFloat(3) == 253.03001.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 14400").first().getFloat(3) == 134.7.toFloat) //value check [partition boundary]

    //checking values for crossing targets
    assert(bdg.where("contigName == 'chrM' and start == 6800").first().getFloat(3) == 96.545.toFloat) //6800-6999
    assert(bdg.where("contigName == 'chrM' and start == 6900").first().getFloat(3) == 78.945.toFloat) //6900-7099

    //checking values for target inside target
    assert(bdg.where("contigName == 'chrM' and start == 7600").first().getFloat(3) == 155.69.toFloat) //7600 -> 7999 -> this contains next one inside -> [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start = 7700").first().getFloat(3) == 134.41.toFloat) //7700 - > 7799 -> after returning - from 7999 to 7799

    // other gap checks
    assert(bdg.where("contigName == 'chrM' and start = 14600").first().getFloat(3) == 63.68.toFloat) //14600 -> 14799
    assert(bdg.where("contigName == 'chrM' and start = 14900").first().getFloat(3) == 196.81.toFloat) //14900 -> 14999

    assert(bdg.where("contigName == 'chrM' and start = 15200").first().getFloat(3) == 169.02.toFloat) //15200 -> 15399 -> no target between 15100 and 15200 - after gap check
    assert(bdg.where("contigName == 'chrM' and start = 15000").first().getFloat(3) == 144.75.toFloat) //15000 -> 15099 -> returning in BED FILE


    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check
  }

  test("BAM - bdg_coverage - windows optimized") {

    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)
    spark.sqlContext.setConf(BDGInternalParams.OptimizationWindow, "true")

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val windowLength = 100
    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks', '${windowLength}')")

    assert (bdg.count == 267)
    assert (bdg.first().getInt(1) % windowLength == 0) // check for fixed window start position
    assert (bdg.first().getInt(2) % windowLength == windowLength - 1) // // check for fixed window end position
    assert(bdg.where("contigName == 'chr1' and start == 2700").first().getFloat(3)==4.65.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 3200").first().getFloat(3)== 166.79.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 10000").first().getFloat(3)== 1.5522388.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 7800").first().getFloat(3)== 253.03.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 14400").first().getFloat(3)== 134.7.toFloat) //value check [partition boundary]
    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check

  }

  test("BAM - bdg_coverage - windows with targets from table optmized") {

    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)
    spark.sqlContext.setConf(BDGInternalParams.OptimizationWindow, "true")

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val windowLength = 100
    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks', '${tableNameTargets.toString}')")

    assert(bdg.count == 34) // test-target.bed contains 34 lines (targets)

    /*
    * group of tests which check the same values as for const windows with 100 size
    * in bed file are only three chr1 targets -> it tests gaps between targets also
    */
    assert(bdg.where("contigName == 'chr1' and start == 2700").first().getFloat(3) == 4.65.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 3200").first().getFloat(3) == 166.79.toFloat)
    assert(bdg.where("contigName == 'chr1' and start == 10000").first().getFloat(3) == 1.5522388.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 7800").first().getFloat(3) == 253.03.toFloat) //value check [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start == 14400").first().getFloat(3) == 134.7.toFloat) //value check [partition boundary]

    //checking values for crossing targets
    assert(bdg.where("contigName == 'chrM' and start == 6800").first().getFloat(3) == 96.545.toFloat) //6800-6999
    assert(bdg.where("contigName == 'chrM' and start == 6900").first().getFloat(3) == 78.945.toFloat) //6900-7099

    //checking values for target inside target
    assert(bdg.where("contigName == 'chrM' and start == 7600").first().getFloat(3) == 155.69.toFloat) //7600 -> 7999 -> this contains next one inside -> [partition boundary]
    assert(bdg.where("contigName == 'chrM' and start = 7700").first().getFloat(3) == 134.41.toFloat) //7700 - > 7799 -> after returning - from 7999 to 7799

    // other gap checks
    assert(bdg.where("contigName == 'chrM' and start = 14600").first().getFloat(3) == 63.68.toFloat) //14600 -> 14799
    assert(bdg.where("contigName == 'chrM' and start = 14900").first().getFloat(3) == 196.81.toFloat) //14900 -> 14999

    assert(bdg.where("contigName == 'chrM' and start = 15200").first().getFloat(3) == 169.02.toFloat) //15200 -> 15399 -> no target between 15100 and 15200 - after gap check
    assert(bdg.where("contigName == 'chrM' and start = 15000").first().getFloat(3) == 144.75.toFloat) //15000 -> 15099 -> returning in BED FILE


    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check

  }

  test("BAM - bdg_coverage - blocks - allPositions") {
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)

    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)
    session.experimental.extraStrategies = new CoverageStrategy(session) :: Nil

    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions, "true")

    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks')")

    assert(bdg.count() == 12865)
    assert(bdg.first().get(1) == 1) // first position check (should start from 1 with ShowAllPositions = true)
    assert(bdg.where("contigName='chr1' and start == 35").first().getShort(3) == 2) // value check
    assert(bdg.where("contigName='chrM' and start == 7").first().getShort(3) == 1) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7881").first().getShort(3) == 248) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7882").first().getShort(3) == 247) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7883").first().getShort(3) == 246) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 14402").first().getShort(3) == 182) // value check [partition boundary]
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chr1'").first().get(1) == 247249719) // max value check
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chrM'").first().get(1) == 16571) // max value check
    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check
  }

  test("BAM - bdg_coverage - blocks notAllPositions") {
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions, "false")

    val bdg = session.sql(s"SELECT *  FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blocks')")

    assert(bdg.count() == 12861) // total count check
    assert(bdg.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
    assert(bdg.where("contigName='chr1' and start == 35").first().getShort(3) == 2) // value check
    assert(bdg.where("contigName='chrM' and start == 7").first().getShort(3) == 1) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7881").first().getShort(3) == 248) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7882").first().getShort(3) == 247) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7883").first().getShort(3) == 246) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 14402").first().getShort(3) == 182) // value check [partition boundary]
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chr1'").first().get(1) == 10066) // max value check
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chrM'").first().get(1) == 16571) // max value check
    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check
  }

  test("BAM - bdg_coverage - bases - notAllPositions") {
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, splitSize)
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    session.sqlContext.setConf(BDGInternalParams.ShowAllPositions, "false")
    val bdg = session.sql(s"SELECT contigName, start, end, coverage FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'bases')")

    assert(bdg.count() == 26598) // total count check // was 26598
    assert(bdg.first().get(1) != 1) // first position check (should not start from 1 with ShowAllPositions = false)
    assert(bdg.where("contigName='chr1' and start == 35").first().getShort(3) == 2) // value check
    assert(bdg.where("contigName='chr1' and start == 88").first().getShort(3) == 7)
    assert(bdg.where("contigName='chrM' and start == 7").first().getShort(3) == 1) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7881").first().getShort(3) == 248) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7882").first().getShort(3) == 247) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 7883").first().getShort(3) == 246) // value check [partition boundary]
    assert(bdg.where("contigName='chrM' and start == 14402").first().getShort(3) == 182) // value check [partition boundary]
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chr1'").first().get(1) == 10066) // max value check
    assert(bdg.groupBy("contigName").max("end").where("contigName == 'chrM'").first().get(1) == 16571) // max value check
    assert(bdg.groupBy("contigName", "start").count().where("count != 1").count == 0) // no duplicates check

  }

  test("CRAM - bdg_coverage - show") {
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    val bdg = session.sql(s"SELECT * FROM bdg_coverage('${tableNameCRAM}','test', 'blocks') ")

    assert(bdg.count() == 49)
    assert(bdg.where("start == 107").first().getShort(3) == 459)
  }

  test("BAM - bdg_coverage - wrong param, Exception should be thrown") {
    val session: SparkSession = SequilaSession(spark)
    SequilaRegister.register(session)

    assertThrows[Exception](
      session.sql(s"SELECT * FROM bdg_coverage('${tableNameMultiBAM}','NA12878', 'blaaaaaah')").show())

  }

  after {

    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.flush()
    Metrics.stopRecording()

  }

}
