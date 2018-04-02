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
package jetbrains.exodus.env

interface ProcessCoordinator : AutoCloseable {
    var highestRoot: Long?
    val lockType: StoreLockType

    var highestMetaTreeRoot: Long?
    val lowestUsedRoot: Long?
    var localLowestUsedRoot: Long?

    fun tryAcquireWriterLock(): Boolean

    fun <T> withHighestRootLock(action: () -> T): T

    fun withExclusiveLock(action: () -> Unit): Boolean

    override fun close()

    fun assertAccess(readonly: Boolean) {
        if (lockType == StoreLockType.EXCLUSIVE) {
            throw SharedAccessException("database is locked exclusively")
        } else if (lockType == StoreLockType.ALLOW_READ && !readonly) {
            throw SharedAccessException("database can be accessed only for read-only environments")
        }
    }
}


enum class StoreLockType(val code: Int) {

    EXCLUSIVE(0),
    ALLOW_READ(1),
    ALLOW_READ_WRITE(2)

}

fun Int.asLockType() = StoreLockType.values().firstOrNull{this == it.code} ?: StoreLockType.EXCLUSIVE

