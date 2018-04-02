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

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.util.*

class ConcurrentAccessTest {

    //TODO check port before use
    private val storeController1 = ForkedStoreController(28080)

    private val sharedLocation = newLocation()

    @Test
    fun `sharing database between two processes`() {
        storeController1.also {
            it.launch()
            it.awaitStart()
        }
        val storeVO = RemoteStoreVO(sharedLocation)
        val bind1 = storeController1.bind(storeVO).execute()
        assertTrue(bind1.isSuccessful)
        with(storeController1.newType("Type").execute()) {
            assertTrue(isSuccessful)
            assertTrue(body()!!.ok)
        }
        val newEntity = storeController1.newEntity(
                EntityVO(
                        type = "Type",
                        properties = listOf(EntityPropertyVO("name", "john")),
                        links = emptyList()
                )
        ).execute()

        with(newEntity) {
            assertTrue(isSuccessful)
        }

        with(StoreService(sharedLocation, null, true)) {
            assertEquals(listOf("Type"), entityTypes)
        }
    }

    @After
    fun start() {
        storeController1.process?.destroy()
        println(sharedLocation)
//        File(sharedLocation).delete()
    }

    private fun newLocation(): String {
        return File(System.getProperty("java.io.tmpdir"), Random().nextLong().toString()).absolutePath
    }
}