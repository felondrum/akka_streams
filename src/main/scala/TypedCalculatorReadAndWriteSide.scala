import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.{ClosedShape, Graph}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.TypedCalculatorWriteSide.{Add, Added, Command, Divide, Divided, Multiplied, Multiply}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}
import akka_typed.CalculatorRepository.{createSession, getLatestsOffsetAndResult, initDatabase, updatedResultAndOffset}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

object  akka_typed{

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  case class Result(state: Double, offset: Long)

  object TypedCalculatorWriteSide{
    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id:Int, amount: Double) extends Event
    case class Multiplied(id:Int, amount: Double) extends Event
    case class Divided(id:Int, amount: Double) extends Event

    final case class State(value:Double) extends CborSerialization
    {
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    object State{
      val empty = State(0.0)
    }


    def handleCommand(
                       persistenceId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]){
    initDatabase

    implicit val materializer = system.classicSystem
    var (offset, latestCalculatedResult) = getLatestsOffsetAndResult
    val startOffset: Long = if (offset == 1) 1 else offset + 1

    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    /*
/**
 * В read side приложения с архитектурой CQRS (объект TypedCalculatorReadSide в TypedCalculatorReadAndWriteSide.scala) необходимо разделить бизнес логику и запись в целевой получатель, т.е.
 * 1) Persistence Query должно находиться в Source
 * 2) Обновление состояния необходимо переместить в отдельный от записи в БД флоу
 * 3) ! Задание со звездочкой: вместо CalculatorRepository создать Sink c любой БД (например Postgres из docker-compose файла).
 * Для последнего задания пригодится документация - https://doc.akka.io/docs/alpakka/current/slick.html#using-a-slick-flow-or-sink
 * Результат выполненного д.з. необходимо оформить либо на github gist либо PR к текущему репозиторию.
 *
 * */

как делать:
1. в типах int заменить на double
3. добавить функцию updateState в которой будет паттерн матчинг событий Added Multiplied Divided
4.создаете graphDsl  в котором: builder.add(source)
5. builder.add(Flow[EventEnvelope].map( e => updateState(e.event, e.seqNr)))
 */

    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    /*
    // homework, spoiler
      def updateState(event: Any, seqNum: Long): Result ={
        val newStste = event match {
          case Added(_amount)=>
            ???
          case Multiplied(_,amount)=>
            ???
          case Divided(_amount)=>
            ???
        }
        Result(newState, seqNum)
      }

      val graph = GraphDSL.create(){
        implicit builder: GraphDSL.Builder[NotUsed] =>
          //1.
          val input = builder.add(source)
          val stateUpdater = builder.add(Flow[EventEnvelope].map(e=> updateState(e.event, e.sequenceNr)))
          val localSaveOutput = builder.add(Sink.foreach[Result]{
            r=>
              latestCalculatedResult = r.state
              println("something to print")
          })

          val dbSaveOutput = builder.add(
            Slick.sink[Result](r=> updatedResultAndOffset(r))
          )

          // надо разделить builder на 2  c помощью Broadcats
          //см https://blog.rockthejvm.com/akka-streams-graphs/

          //надо будет сохранить flow(разделенный на 2) в localSaveOutput и dbSaveOutput
          //в конце закрыть граф и запустить его RunnableGraph.fromGraph(graph).run()




      }*/

    def updateState(event: Any, seqNum: Long): Result = {
      val newState = event match {
        case Added(_, amount) =>
          latestCalculatedResult + amount
        case Multiplied(_, amount) =>
          latestCalculatedResult * amount
        case Divided(_, amount) =>
          latestCalculatedResult / amount
      }
      Result(newState, seqNum)
    }

    implicit val session: SlickSession = createSession()
    implicit val ec: ExecutionContextExecutor = system.executionContext

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val input = builder.add(source)
      val stateUpdater = builder.add(Flow[EventEnvelope].map(e => updateState(e.event, e.sequenceNr)))
      val broadcast = builder.add(akka.stream.scaladsl.Broadcast[Result](2))

      val localSaveOutput = builder.add(Sink.foreach[Result] { r =>
        latestCalculatedResult = r.state
        println(s"Updated local state: $latestCalculatedResult")
      })

      val dbSaveOutput = builder.add(
        Slick.sink[Result]((r: Result) => updatedResultAndOffset(r.state, r.offset))
      )

      input ~> stateUpdater ~> broadcast ~> localSaveOutput
      broadcast ~> dbSaveOutput

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
    system.whenTerminated.onComplete(_ => session.close())
  }

  object CalculatorRepository{

    //homework how to do
    //1.
    /*    def createSession(): SlickSession ={
          //создайте сессию согласно документации
        }*/


    def createSession(): SlickSession = {
      SlickSession.forConfig("slick-postgres")
    }

    def initDatabase: Unit = {
      Class.forName("org.postgresql.Driver")
      val poolSettings = ConnectionPoolSettings(initialSize = 10, maxSize = 100)
      ConnectionPool.singleton("jdbc:postgresql://localhost:5432/demo", "docker", "docker", poolSettings)
    }

    def updatedResultAndOffset(calculated: Double, offset: Long): DBIO[Int] = {
      sqlu"update public.result set calculated_value = $calculated, write_side_offset = $offset where id = 1"
    }

    def getLatestsOffsetAndResult: (Long, Double) = {
      val session = createSession()
      import session.profile.api._
      
      val query = sql"select * from public.result where id = 1;".as[(Long, Double)]
      val result = Await.result(session.db.run(query), 5.seconds)
      session.close()
      
      result.headOption.getOrElse((1L, 0.0))
    }
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! Add(10.0)
        writeAcorRef ! Multiply(2.0)
        writeAcorRef ! Divide(5.0)
        writeAcorRef ! Add(3.0)

        Behaviors.same
    }

  def execute(command: Command): Behavior[NotUsed] =
    Behaviors.setup{ ctx =>
      val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
      writeAcorRef ! command
      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit  val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")

    TypedCalculatorReadSide(system)
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
  }

}