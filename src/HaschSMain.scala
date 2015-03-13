
import scala.io.Source

object HaschSMain extends App {
  case class Line[A](n: Int, x: A)
  type Row = Int
  type Slot = Int
  type Size = Int
  type Capacity = Int

  val lines = Source.fromFile("dc.in").getLines().toList
  val pLine = lines.head
  val params = pLine.split(" ").map(_.toInt)
  println(params.toList)
  val R = params(0) //# rows
  val S = params(1) //# slots = cols
  val U = params(2) //# unavailable slots
  val P = params(3) //# pools to be created
  val M = params(4) //# servers to be allocated
  
  val unAv =
    lines.drop(1).take(U).toList //take range U
      .map(_.split(" ").map(_.toInt)).map { list => (list(0).asInstanceOf[Row], list(1).asInstanceOf[Slot]) }.sortBy(_._1) //create Int tuples
  println("unavailable: " + unAv.take(10))
  val serv = lines.drop(1).drop(U).take(M) //M
    .map(_.split(" ").map(_.toInt)).zipWithIndex.map { case (list, i) => Server(i, -1, -1, -1, list(0), list(1)) }
  println()
  println("servers" + serv.take(10))
  val linesWithUnAv = unAv.groupBy(_._1).toList.sortBy(_._1).map(t => Line(t._1, t._2.map(_._2).sortBy(x => x))) //List[List[(Int, Int)]]
  println(linesWithUnAv.take(5).foreach { println(_) })

  case class Range(min: Int, max: Int) //extends R

  def range(unAva: List[Int]): List[Range] = {
    def rec(r: Range, acc: List[Int]): List[Range] = acc match {
      case Nil => r :: Nil
      case x :: xs =>
        assert(x >= r.min && x <= r.max)
        if (x == r.min && x == r.max && x == r.min) Nil //single last cell removed
        else if (x == r.min && x == r.max) r :: Nil
        else if (x == r.min) rec(Range(r.min + 1, r.max), xs)
        else if (x == r.max) rec(Range(r.min, r.max - 1), xs)
        else Range(r.min, x - 1) :: rec(Range(x + 1, r.max), xs)
    }
    rec(Range(0, S - 1), unAva)
  }

  val linesRanges = linesWithUnAv.map { l: Line[List[Int]] => Line(l.n, range(l.x.sortBy { x => x })) }
  println("lines")
  linesRanges.take(10).foreach(println(_))
  println("lines:" + linesRanges.size)

  case class Server(id: Int, row:Int, col: Int, pool:Int, size: Int, weight: Int)

  def assignRec(tr:Int, lr: List[Range], ls: List[Server]): List[Server] = (lr, ls) match {
    case (Nil, x) =>
      println("no more range, servers: "+x); Nil
    case (x, Nil) =>
      println("no more server, range:"+x); Nil
    case (lrh :: lrt, lss) =>
      val rsize = lrh.max - lrh.min + 1
      lss.find { server => {server.size < rsize} } match {
        case None => assignRec(tr, lrt, lss)
        case Some(se) => Server(se.id, tr, lrh.min, -1, se.size, se.weight) ::
          {
            val index = lss.indexOf(se)
            val nlss = lss.take(index) ::: lss.drop(index + 1)
            if (rsize == se.size) {
              assignRec(tr, lrt, nlss)
            } else {
              assignRec(tr, Range(lrh.min + se.size, lrh.max) :: lrt, nlss)
            }
          }
      }
  }
  
  val size = M / R
  println(size)
  println(linesRanges.size)
  println(R)
  val servLinAss = serv.grouped(size).toList.map(_.sortWith((s1, s2) => s1.weight > s2.weight))
  //servLinAss.foreach { println(_) }
  
  val res = linesRanges.zip(servLinAss).map{case (line, servList) => assignRec(line.n, line.x, servList)}.map(_.map(server => server.copy(pool = (server.col + server.row) % P)))
  res.foreach { println(_) }

  def printt(index:Int, ss: List[Server]):Unit = ss match {
    case x :: xs =>
      if (x.id == index) {println(x.row+" "+x.col+" "+x.pool); printt(index+1, xs)}
      else {println("x"); printt(index+1, x :: xs)}
    case Nil if (index < M) => println("x"); printt(index+1, Nil)
    case Nil => println("") //the end
  }
  printt(0, res.flatten.sortBy(s => s.id))
  
  def poolGuaranteedCapacity(servs:List[Server]):Int = {
    servs.groupBy(_.row).map{
      case (row, list) => servs.filterNot(_.row == row).map(_.weight).reduce{(x, y) => x + y}
    }.toList.min
  }
  val guaranteedCapacityList = res.flatten.groupBy(_.pool).map{case (pool, servers) => poolGuaranteedCapacity(servers)}.toList
  guaranteedCapacityList.foreach { println(_) }
  val minGuaranteedCapacity = guaranteedCapacityList.min
  
  println("min guar cap: "+minGuaranteedCapacity)
  
  //for(i <- 1 to n) yield s).toList
}