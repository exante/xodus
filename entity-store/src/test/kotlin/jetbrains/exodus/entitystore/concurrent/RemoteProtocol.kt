package jetbrains.exodus.entitystore.concurrent

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.Environments
import mu.KLogging
import retrofit2.Call
import retrofit2.http.*
import spark.ResponseTransformer
import spark.kotlin.Http
import spark.kotlin.RouteHandler
import java.lang.Exception

val mapper: ObjectMapper = ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(KotlinModule())

object JsonTransformer : ResponseTransformer {

    override fun render(model: Any): String {
        return mapper.writeValueAsString(model)
    }
}


const val json = "application/json"


interface RemoteProtocol {

    @POST("/bind")
    fun bind(@Body store: RemoteStoreVO): Call<RemoteStoreVO>

    @POST("/entities")
    fun newEntity(@Body entity: EntityVO): Call<EntityVO>

    @POST("/types")
    fun newType(@Query("name") name: String): Call<ResultVO>

    @GET("/entities/{id}")
    fun getEntity(@Path("id") id: String): Call<EntityVO>

    @GET("/")
    fun healthCheck(): Call<ResultVO>

}

data class RemoteStoreVO(
        val location: String,
        val key: String? = null
)

data class ResultVO(
        val ok: Boolean
)

data class EntityVO(
        val id: String? = null,
        val type: String? = null,
        val properties: List<EntityPropertyVO>,
        val links: List<EntityLinkVO>
)

data class EntityPropertyVO(
        val name: String,
        val value: String?
)

data class EntityLinkVO(
        val name: String,
        val linkedEntitiesIds: List<String>
)


object RemoteStoreProtocolImpl {

    lateinit var storeService: StoreService

    fun registerRouting(http: Http) {
        http.safePost("/bind") {
            val store = mapper.readValue(request.body(), RemoteStoreVO::class.java)
            storeService = StoreService(store.location, store.key)
            store
        }
        http.safePost("/types") {
            val entity = request.queryParams("name")
            storeService.newType(entity)
            ResultVO(true)
        }
        http.safePost("/entities") {
            val entity = mapper.readValue(request.body(), EntityVO::class.java)
            storeService.newEntity(entity)
            entity
        }
        http.safeGet("/entities/:id") {
            storeService.getEntity(request.params("id"))
        }
        http.safeGet("/") {
            ResultVO(true)
        }

    }

    private fun Http.safePost(path: String = "", executor: RouteHandler.() -> Any) {
        post(path) {
            response.type(json)
            JsonTransformer.render(executor())
        }
    }


    private fun Http.safeGet(path: String = "", executor: RouteHandler.() -> Any) {
        get(path, json) {
            response.type(json)
            JsonTransformer.render(executor())
        }
    }

}

class StoreService(location: String, key: String?) {

    companion object : KLogging()

    private val store: PersistentEntityStoreImpl

    init {
        try {
            val config = EnvironmentConfig()
            val environment = Environments.newInstance(location, config)
            store = key.let {
                if (it == null) {
                    PersistentEntityStores.newInstance(environment)
                } else {
                    PersistentEntityStores.newInstance(environment, it)
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "can't open store" }
            throw e
        }
    }

    fun newEntity(entityVO: EntityVO): EntityVO {
        val id = store.transactional {
            val entity = it.newEntity(entityVO.type!!)
            entityVO.properties.forEach { prop ->
                prop.value?.let {
                    entity.setProperty(prop.name, it)
                }
            }
            entity.toIdString()
        }
        return getEntity(id)
    }

    fun getEntity(id: String): EntityVO {
        return store.transactional {
            it.getEntity(id.asId()).let {
                EntityVO(
                        id = it.id.toString(),
                        properties = it.propertyNames.map { name ->
                            EntityPropertyVO(
                                    name = name,
                                    value = it.getProperty(name)?.toString()
                            )
                        },
                        links = it.linkNames.map { name ->
                            EntityLinkVO(
                                    name = name,
                                    linkedEntitiesIds = it.getLinks(name).map { it.toIdString() }
                            )
                        }
                )
            }
        }
    }

    private fun String.asId(): EntityId {
        return PersistentEntityId.toEntityId(this)
    }

    private fun <T> PersistentEntityStore.transactional(call: (PersistentStoreTransaction) -> T): T {
        return computeInTransaction { call(it as PersistentStoreTransaction) }
    }

    fun newType(name: String) {
        store.transactional { store.getEntityTypeId(it, name, true) }
    }
}