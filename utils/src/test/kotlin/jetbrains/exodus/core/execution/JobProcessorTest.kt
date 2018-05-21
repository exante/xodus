/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.core.execution

import jetbrains.exodus.core.dataStructures.Priority
import org.junit.Assert
import org.junit.Before
import org.junit.Test

open class JobProcessorTest {

    private lateinit var processor: ThreadJobProcessor

    private fun createProcessor() {
        processor = ThreadJobProcessor("Test Job Processor")
    }

    @Before
    open fun setUp() {
        System.setProperty(JobProcessorQueueAdapter.CONCURRENT_QUEUE_PROPERTY, "false")
    }

    @Test
    fun sleep() {
        createProcessor()
        processor.start()
        val ticks = System.currentTimeMillis()
        val sleepTicks = 2000L
        SleepJob(processor, sleepTicks)
        processor.queueFinish()
        processor.waitUntilFinished()
        val current = System.currentTimeMillis()
        Assert.assertTrue("Current time = $current, ticks = $ticks",
                current - sleepTicks >= ticks)
    }

    @Test
    fun testWaitForJobs() {
        createProcessor()
        processor.start()
        for (i in 0..99) { // this should be fast when everything waitForJobs works properly
            var time = System.currentTimeMillis()
            processor.queue(IncrementJob(processor, 1), Priority.lowest)
            processor.waitForJobs(1000)
            time = System.currentTimeMillis() - time
            Assert.assertTrue("Waiting took: $time on iteration $i", time < 500)
        }
    }

    @Test
    fun testWaitForTimedJobs() {
        createProcessor()
        processor.start()
        for (i in 0..9) { // this should be fast when everything waitForJobs works properly
            var time = System.currentTimeMillis()
            processor.queueIn(IncrementJob(processor, 1), 100)
            processor.waitForTimedJobs(1000)
            time = System.currentTimeMillis() - time
            Assert.assertTrue("Waiting took: $time on iteration $i", time < 500)
        }
    }

    @Test
    fun testSuspend() {
        createProcessor()
        processor.start()
        processor.suspend()
        count = 0
        processor.queue(IncrementJob(processor, 1))
        sleep(200)
        Assert.assertEquals(0, count)
        processor.resume()
        processor.waitForJobs(10)
        Assert.assertEquals(1, count)
    }

    @Test
    fun testSuspend2() {
        createProcessor()
        processor.start()
        processor.suspend()
        count = 0
        processor.queue(IncrementJob(processor, 1))
        sleep(200)
        Assert.assertEquals(0, count)
        processor.resume()
        val r = ResumeRunnable(processor)
        val resumer = Thread(r)
        resumer.start()
        processor.waitForJobs(10)
        Assert.assertEquals(1, count)
        sleep(200)
        if (r.state == ResumeRunnableState.INITIAL) {
            resumer.interrupt()
            sleep(200)
            val state = r.state
            if (state != ResumeRunnableState.RESUMED) {
                Assert.fail("Can't resume in other thread: $state")
            }
        }
    }

    @Test
    fun testSuspend3() {
        createProcessor()
        processor.start()
        processor.suspend()
        val r = SuspendRunnable(processor)
        val suspender = Thread(r)
        suspender.start()
        sleep(200)
        if (r.state == SuspendRunnableState.INITIAL) {
            suspender.interrupt()
            sleep(200)
            val state = r.state
            if (state != SuspendRunnableState.SUSPENDED) {
                Assert.fail("Can't suspend in other thread: $state")
            }
        }
    }

    @Test
    fun testSuspend4() {
        createProcessor()
        processor.start()
        for (i in 0..99) {
            processor.suspend()
            processor.resume()
        }
    }

    @Test
    fun testSuspend5() {
        createProcessor()
        processor.start()
        for (i in 0..99) {
            processor.suspend()
            processor.suspend()
            processor.resume()
            processor.suspend()
        }
    }

    @Test
    fun equalJobs() {
        createProcessor()
        AlwaysEqualJob(processor)
        for (i in 0..9) {
            Assert.assertFalse(AlwaysEqualJob(processor).wasQueued())
        }
        processor.start()
        sleep(200)
        Assert.assertTrue(AlwaysEqualJob(processor).wasQueued())
        processor.finish()
        // do not queue after processor has finished
        Assert.assertFalse(AlwaysEqualJob(processor).wasQueued())
    }

    @Test
    fun priorityOrder() {
        createProcessor()
        count = 0
        processor.start()
        val job = AcquiringLatchJob(processor)
        IncrementJob(processor, 1)
        SleepJob(processor, 300)
        IncrementJob(processor, 2, Priority.above_normal)
        SleepJob(processor, 300, Priority.above_normal)
        IncrementJob(processor, 3, Priority.highest)
        SleepJob(processor, 300, Priority.highest)
        job.release()
        sleep(200)
        Assert.assertEquals(3, count)
        sleep(300)
        Assert.assertEquals(5, count)
        sleep(300)
        Assert.assertEquals(6, count)
    }

    @Test
    fun pendingJobs() {
        createProcessor()
        count = 0
        processor.start()
        val job = AcquiringLatchJob(processor)
        IncrementJob(processor, 1)
        SleepJob(processor, 1000)
        IncrementJob(processor, 2, Priority.above_normal)
        SleepJob(processor, 1000, Priority.above_normal)
        IncrementJob(processor, 3, Priority.highest)
        SleepJob(processor, 1000, Priority.highest)
        // the following sleep() is necessary to make sure that AcquiringLatchJob is been executed
        // otherwise a race condition is possible resulting in processor.pendingJobs() returns 8 instead of 7
        sleep(1000)
        Assert.assertEquals(7, processor.pendingJobs())
        job.release()
        sleep(700)
        Assert.assertEquals(5, processor.pendingJobs())
        sleep(1000)
        Assert.assertEquals(3, processor.pendingJobs())
        sleep(1000)
        Assert.assertEquals(1, processor.pendingJobs())
        sleep(700)
        Assert.assertEquals(0, processor.pendingJobs())
        Assert.assertEquals(6, count)
    }

    @Test
    fun queueAtFuture() {
        createProcessor()
        processor.start()
        count = 0
        processor.queueAt(IncrementJob(1), System.currentTimeMillis() + 200)
        processor.queueAt(IncrementJob(2), System.currentTimeMillis() + 400)
        processor.queueAt(IncrementJob(3), System.currentTimeMillis() + 600)
        Assert.assertEquals(0, count)
        sleep(300)
        Assert.assertEquals(1, count)
        sleep(200)
        Assert.assertEquals(3, count)
        sleep(200)
        Assert.assertEquals(6, count)
    }

    @Test
    fun queueAtMerge() {
        createProcessor()
        processor.start()
        count = 0
        val job = IncrementJob(1)
        processor.queueAt(job, System.currentTimeMillis() + 200)
        processor.queueAt(job, System.currentTimeMillis() + 400)
        processor.queueAt(job, System.currentTimeMillis() + 600)
        sleep(500)
        Assert.assertEquals(0, count)
        sleep(200)
        Assert.assertEquals(1, count)
        processor.queueAt(job, System.currentTimeMillis() + 600)
        processor.queueAt(job, System.currentTimeMillis() + 100)
        sleep(200)
        Assert.assertEquals(2, count)
    }

    @Test
    fun queueIn() {
        createProcessor()
        processor.start()
        count = 0
        val job = IncrementJob(1)
        processor.queueIn(job, 200)
        sleep(100)
        Assert.assertEquals(0, count)
        sleep(200)
        Assert.assertEquals(1, count)
    }

    private class AcquiringLatchJob internal constructor(processor: JobProcessor) : LatchJob() {

        init {
            try {
                acquire()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }

            Assert.assertTrue(processor.queue(this, Priority.highest))
        }

        override fun execute() {
            acquire()
        }

    }

    private class SleepJob(processor: JobProcessor,
                           private var ticks: Long,
                           priority: Priority = Priority.normal) : Job(processor, priority) {

        init {
            Assert.assertTrue(wasQueued())
        }

        override fun execute() {
            println("SleepJob")
            sleep(ticks)
        }
    }

    private class IncrementJob(private val increment: Int) : Job() {

        internal constructor(processor: JobProcessor, increment: Int, priority: Priority = Priority.normal) : this(increment) {
            Assert.assertTrue(processor.queue(this, priority))
        }

        override fun execute() {
            val before = System.nanoTime()
            count += increment
            println("Increment executed in: " + (System.nanoTime() - before) + "ns.")
        }
    }

    /**
     * All jobs of this type are always equal
     */
    @Suppress("EqualsOrHashCode")
    private class AlwaysEqualJob(processor: JobProcessor) : Job(processor) {

        override fun isEqualTo(job: Job): Boolean {
            return true
        }

        override fun hashCode(): Int {
            return 0
        }

        override fun execute() {}
    }

    private class ResumeRunnable(private val processor: JobProcessor) : Runnable {

        @Volatile
        var state = ResumeRunnableState.INITIAL

        override fun run() {
            try {
                processor.resume()
                state = ResumeRunnableState.RESUMED
            } catch (e: InterruptedException) {
                state = ResumeRunnableState.INTERRUPTED
                Thread.currentThread().interrupt()
            }

        }
    }

    private enum class ResumeRunnableState {
        INITIAL,
        INTERRUPTED,
        RESUMED
    }

    private class SuspendRunnable(private val processor: JobProcessor) : Runnable {

        @Volatile
        var state = SuspendRunnableState.INITIAL

        override fun run() {
            try {
                processor.suspend()
                state = SuspendRunnableState.SUSPENDED
            } catch (e: InterruptedException) {
                state = SuspendRunnableState.INTERRUPTED
                Thread.currentThread().interrupt()
            }

        }
    }

    private enum class SuspendRunnableState {
        INITIAL,
        INTERRUPTED,
        SUSPENDED
    }

    companion object {

        private var count = 0

        private fun sleep(ticks: Long) {
            val endTicks = System.currentTimeMillis() + ticks
            try {
                Thread.sleep(ticks + 30)
            } catch (e: InterruptedException) {
                while (System.currentTimeMillis() < endTicks) {
                    Thread.yield()
                }
                e.printStackTrace()
                Thread.currentThread().interrupt()
            }
            Thread.yield()
        }
    }
}
