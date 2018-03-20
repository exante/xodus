package jetbrains.exodus.entitystore.concurrent

import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.util.*

class ConcurrentAccessTest {

    //TODO check port before use
    private val storeController1 = ForkedStoreController(28080)
    private val storeController2 = ForkedStoreController(28081)


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
        with(storeController1.newEntity(
                EntityVO(
                        type = "Type",
                        properties = listOf(EntityPropertyVO("name", "john")),
                        links = emptyList()
                )
        ).execute()) {
            assertTrue(isSuccessful)
        }


        storeController2.also {
            it.launch()
            it.awaitStart()
        }
        val bind2 = storeController2.bind(storeVO).execute()
        assertTrue(bind2.isSuccessful)
    }

    @After
    fun start() {
        storeController1.process?.destroy()
        storeController2.process?.destroy()
        File(sharedLocation).delete()
    }

    private fun newLocation(): String {
        return System.getProperty("java.io.tmpdir") + File.separator + Random().nextLong()
    }
}