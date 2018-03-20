package jetbrains.exodus.entitystore.concurrent

import mu.KLogging
import spark.kotlin.ignite

object ForkedStoreHttp : KLogging() {

    fun setup(host: String = "localhost", port: Int) {
        ignite().port(port).ipAddress(host).apply {
            after {
                logger.info {
                    "'${request.requestMethod()} ${request.pathInfo()}' - ${response.status()} ${response.type() ?: ""}"
                }
            }
            RemoteStoreProtocolImpl.registerRouting(this)

            internalServerError {
                "Sorry, something went wrong. Check server logs"
            }
        }
    }

}

fun main(args: Array<String>) {
    ForkedStoreHttp.setup(port = args[0].toInt())
}