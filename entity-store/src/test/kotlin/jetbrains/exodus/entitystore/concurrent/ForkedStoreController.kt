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
            logger.info { "$processName started" }
            process = it
            it
        }
    }

    fun awaitStart(timeout: Int = 5) = apply {
        var started = false
        val start = System.currentTimeMillis()
        var time = start
        while (!started && time <= start + timeout * 1000) {
            logger.info { "awaiting start of process[:$port]" }
            val result = try {
                healthCheck().execute()
            } catch (e: Exception) {
                null
            }
            started = result?.let {
                logger.info { "processName result is ${it.code()}" }
                it.isSuccessful && (it.body()?.ok ?: false)
            } ?: false
            Thread.sleep(500)
            time = System.currentTimeMillis()
        }
    }

}