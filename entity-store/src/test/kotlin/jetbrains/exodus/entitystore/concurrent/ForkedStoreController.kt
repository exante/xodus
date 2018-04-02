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
package jetbrains.exodus.entitystore.concurrent

import mu.KLogging
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import java.io.File

private fun client(port: Int): RemoteProtocol =
        Retrofit.Builder().baseUrl("http://localhost:$port/").addConverterFactory(JacksonConverterFactory.create(mapper)).build().create(RemoteProtocol::class.java)

class ForkedStoreController(private val port: Int) : RemoteProtocol by client(port) {

    companion object : KLogging()

    var process: Process? = null
    val processName = "process[:$port]"

    fun launch(): Process {
        logger.info { "preparing $processName" }
        val pr = process
        if (pr != null) {
            return pr
        }
        val javaHome = System.getProperty("java.home")
        val javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java"
        val classpath = System.getProperty("java.class.path")
        val className = ForkedStoreHttp::class.qualifiedName + "Kt"

        val builder = ProcessBuilder(javaBin, "-cp", classpath, className, port.toString()).inheritIO()
        logger.info { "starting $processName by command" }
        logger.info { "-----------------------------" }
        logger.info { builder.command().joinToString(" ") }
        logger.info { "-----------------------------" }
        return builder.start().let {
            logger.info { "$processName is started" }
            process = it
            it
        }
    }

    fun awaitStart(timeout: Int = 5) = apply {
        var started = false
        val start = System.currentTimeMillis()
        var time = start
        while (!started && time <= start + timeout * 1000) {
            logger.info { "awaiting start of $processName" }
            val result = try {
                healthCheck().execute()
            } catch (e: Exception) {
                null
            }
            started = result?.let {
                logger.info { "$processName result is ${it.code()}" }
                it.isSuccessful && (it.body()?.ok ?: false)
            } ?: false
            Thread.sleep(500)
            time = System.currentTimeMillis()
        }
    }

}