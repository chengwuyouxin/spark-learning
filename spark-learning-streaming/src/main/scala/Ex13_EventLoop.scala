import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import scala.util.control.NonFatal

case class EventClass()

class Ex13_EventLoop {

  private val timer = new RecurringTimer(new SystemClock(), 5000,
    longTime => eventLoop.post(new EventClass), "JobGenerator")

  // eventLoop is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventLoop: EventLoop[EventClass] = null


  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // See SPARK-10125

    eventLoop = new EventLoop[EventClass]("JobGenerator") {
      override protected def onReceive(event: EventClass): Unit = processEvent(event)
      override protected def onError(e: Throwable): Unit = {
      }
    }
    eventLoop.start()
    timer.start()

    Thread.sleep(200000)
  }
  def processEvent(clazz: EventClass): Unit ={
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+" receive a message")
  }
}
object Ex13_EventLoop{
  def main(args:Array[String]): Unit ={
    val ex13=new Ex13_EventLoop()
    ex13.start()
  }
}

trait Clock {
  def getTimeMillis(): Long
  def waitTillTime(targetTime: Long): Long
}
class SystemClock extends Clock {

  val minPollTime = 25L

  /**
    * @return the same time (milliseconds since the epoch)
    *         as is reported by `System.currentTimeMillis()`
    */
  def getTimeMillis(): Long = System.currentTimeMillis()

  /**
    * @param targetTime block until the current time is at least this value
    * @return current system time when wait has completed
    */
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = System.currentTimeMillis()

    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

    while (true) {
      currentTime = System.currentTimeMillis()
      waitTime = targetTime - currentTime
      if (waitTime <= 0) {
        return currentTime
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    -1
  }
}

abstract class EventLoop[E](name: String){

  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  private val stopped = new AtomicBoolean(false)

  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => null
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => null
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
    * Put the event into the event queue. The event thread will process it later.
    */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  /**
    * Return if the event thread has already been started but not yet stopped.
    */
  def isActive: Boolean = eventThread.isAlive

  /**
    * Invoked when `start()` is called but before the event thread starts.
    */
  protected def onStart(): Unit = {}

  /**
    * Invoked when `stop()` is called and the event thread exits.
    */
  protected def onStop(): Unit = {}

  /**
    * Invoked in the event thread when polling events from the event queue.
    *
    * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
    * and cannot process events in time. If you want to call some blocking actions, run them in
    * another thread.
    */
  protected def onReceive(event: E): Unit

  /**
    * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
    * will be ignored.
    */
  protected def onError(e: Throwable): Unit={}

}

class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  {

  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)
    override def run() { loop }
  }

  @volatile private var prevTime = -1L
  @volatile private var nextTime = -1L
  @volatile private var stopped = false

  /**
    * Get the time when this timer will fire if it is started right now.
    * The time will be a multiple of this timer's period and more than
    * current system time.
    */
  def getStartTime(): Long = {
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
    * Get the time when the timer will fire if it is restarted right now.
    * This time depends on when the timer was started the first time, and was stopped
    * for whatever reason. The time must be a multiple of this timer's period and
    * more than current time.
    */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
    * Start at the given start time.
    */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    nextTime
  }

  /**
    * Start at the earliest time it can start based on the period.
    */
  def start(): Long = {
    start(getStartTime())
  }

  /**
    * Stop the timer, and return the last time the callback was made.
    *
    * @param interruptTimer True will interrupt the callback if it is in progress (not guaranteed to
    *                       give correct time in this case). False guarantees that there will be at
    *                       least one callback after `stop` has been called.
    */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
    }
    prevTime
  }

  private def triggerActionForNextInterval(): Unit = {
    clock.waitTillTime(nextTime)
    callback(nextTime)
    prevTime = nextTime
    nextTime += period
  }

  /**
    * Repeatedly call the callback every interval.
    */
  private def loop() {
    try {
      while (!stopped) {
        triggerActionForNextInterval()
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
  }
}

object RecurringTimer {

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      lastRecurTime = currentTime
    }
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}