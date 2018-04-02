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