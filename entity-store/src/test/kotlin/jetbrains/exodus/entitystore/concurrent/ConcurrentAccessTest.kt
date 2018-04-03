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

import jetbrains.exodus.env.SharedAccessException
import jetbrains.exodus.env.StoreLockType
import org.junit.After
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.util.*
import kotlin.reflect.KClass

class ConcurrentAccessTest {

    //TODO check port before use
    private val storeController = ForkedStoreController(28080)

    private val sharedLocation = newLocation()

    @Test
    fun `sharing database between two processes`() {
        storeController.also {
            it.launch()
            it.awaitStart()
        }
        val storeVO = RemoteStoreVO(sharedLocation, StoreLockType.ALLOW_READ_WRITE)
        val bind1 = storeController.bind(storeVO).execute()
        assertTrue(bind1.isSuccessful)
        with(storeController.newType("Type").execute()) {
            assertTrue(isSuccessful)
            assertTrue(body()!!.ok)
        }
        val newEntity = storeController.newEntity(
                EntityVO(
                        type = "Type",
                        properties = listOf(EntityPropertyVO("name", "john")),
                        links = emptyList()
                )
        ).execute()

        with(newEntity) {
            assertTrue(isSuccessful)
        }

        with(StoreService(sharedLocation, null, StoreLockType.ALLOW_READ_WRITE, true)) {
            assertEquals(listOf("Type"), entityTypes)
        }
    }

    @Test
    fun `EXCLUSIVE lock type should restrict any shared access`() {
        storeController.also {
            it.launch()
            it.awaitStart()
        }
        val storeVO = RemoteStoreVO(sharedLocation, StoreLockType.EXCLUSIVE)
        val bind = storeController.bind(storeVO).execute()
        assertTrue(bind.isSuccessful)

        assertFail(SharedAccessException::class) {
            StoreService(sharedLocation, null, StoreLockType.ALLOW_READ, readonly = true)
        }
        assertFail(SharedAccessException::class) {
            StoreService(sharedLocation, null, StoreLockType.ALLOW_READ, readonly = false)
        }
    }

    @Test
    fun `ALLOW_READ lock type should not restrict readonly access`() {
        storeController.also {
            it.launch()
            it.awaitStart()
        }
        val storeVO = RemoteStoreVO(sharedLocation, StoreLockType.ALLOW_READ)
        val bind = storeController.bind(storeVO).execute()
        assertTrue(bind.isSuccessful)

        assertFail(SharedAccessException::class) {
            StoreService(sharedLocation, null, StoreLockType.ALLOW_READ, readonly = false)
        }

        StoreService(sharedLocation, null, StoreLockType.ALLOW_READ, readonly = true)
    }

    private fun assertFail(kClass: KClass<out Exception>, function: () -> Unit) {
        try {
            function()
            Assert.fail("expected fails with ${kClass.java}")
        } catch (e: Exception) {
            assertEquals(kClass.java, e.javaClass)
        }
    }

    @After
    fun start() {
        storeController.process?.destroy()
        println(sharedLocation)
        File(sharedLocation).delete()
    }

    private fun newLocation(): String {
        return File(System.getProperty("java.io.tmpdir"), Random().nextLong().toString()).absolutePath
    }
}