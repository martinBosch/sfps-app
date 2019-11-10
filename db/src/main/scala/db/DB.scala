package db

import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts

import cats._
import cats.data._
import cats.effect._
import cats.implicits._

import fs2.Stream

//import java.sql.Timestamp
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter


object DB {
  // We need a ContextShift[IO] before we can construct a Transactor[IO]. The passed ExecutionContext
  // is where nonblocking operations will be executed. For testing here we're using a synchronous EC.
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)

  // A transactor that gets connections from java.sql.DriverManager and executes blocking operations
  // on an our synchronous EC. See the chapter on connection handling for more info.
  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",     // driver classname
    "jdbc:postgresql:sfps",      // connect URL (driver-specific)
    "postgres",                  // user
    "postgres",                  // password
    //Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  def createDB(): Unit = {
    val drop = sql"""
      DROP TABLE IF EXISTS train
    """.update.run

    val create = sql"""
      CREATE TABLE train (
        id   SERIAL,
        fecha VARCHAR,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        last FLOAT,
        cierre FLOAT,
        aj_dif FLOAT,
        mon CHAR,
        ol_vol INT,
        ol_dif INT,
        vol_ope INT,
        unidad CHAR(4),
        dolar_bn FLOAT,
        dolar_itau FLOAT,
        dif_sem FLOAT
      )
    """.update.run

    (drop, create).mapN(_ + _).transact(xa).unsafeRunSync
  }

  def insert1(dolar: DolarInfo): Update0 = {
    val id: Int = dolar.id
    val fecha: String = dolar.fecha
    val open: Float = dolar.open
    val high: Float = dolar.high
    val low: Float = dolar.low
    val last: Float = dolar.last
    val cierre: Float = dolar.cierre
    val aj_dif: Float = dolar.aj_dif
    val mon: String = dolar.mon
    val ol_vol: Int = dolar.ol_vol
    val ol_dif: Int = dolar.ol_dif
    val vol_ope: Int = dolar.vol_ope
    val unidad: String = dolar.unidad
    val dolar_bn: Float = dolar.dolar_bn
    val dolar_itau: Option[Float] = dolar.dolar_itau
    val dif_sem: Float = dolar.dif_sem
    println(s"inserting row $id...")

    sql"insert into train (id, fecha, open, high, low, last, cierre, aj_dif, mon, ol_vol, ol_dif, vol_ope, unidad, dolar_bn, dolar_itau, dif_sem) values ($id, $fecha, $open, $high, $low, $last, $cierre, $aj_dif, $mon, $ol_vol, $ol_dif, $vol_ope, $unidad, $dolar_bn, $dolar_itau, $dif_sem)".update
  }

  @scala.annotation.tailrec
  def insertMany(ds: List[DolarInfo]): Unit = {
    ds match {
      case Nil => {}
      case dolarInfo :: tail =>  {
        insert1(dolarInfo).run.transact(xa).unsafeRunSync
        insertMany(tail)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    createDB()

    println("Loadind data to db...")
    val dolarInfoCSVReader = DolarInfoCSVReader("data/train.csv")
    val rows: List[DolarInfo] = dolarInfoCSVReader.readDolarInfo()

    insertMany(rows)
    //    for( r <- rows) insert1(r).run.transact(xa).unsafeRunSync
    //    insertMany(rows).run.transact(xa).unsafeRunSync
  }
}
