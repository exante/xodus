/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.hash.PairProcedure;
import jetbrains.exodus.core.dataStructures.persistent.EvictListener;
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable;
import jetbrains.exodus.entitystore.iterate.UpdatableCachedInstanceIterable;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

final class EntityIterableCacheAdapterMutable extends EntityIterableCacheAdapter {
    private final State state;

    private EntityIterableCacheAdapterMutable(@NotNull final PersistentEntityStoreConfig config, @NotNull final State state) {
        super(config, state.cache);

        this.state = state;
    }

    @NotNull
    EntityIterableCacheAdapter endWrite() {
        return new EntityIterableCacheAdapter(config, cache.endWrite());
    }

    void update(@NotNull final PersistentStoreTransaction.HandleChecker checker,
                @NotNull final List<UpdatableCachedInstanceIterable> mutatedInTxn) {
        forEachKey(new ObjectProcedure<EntityIterableHandle>() {
            @Override
            public boolean execute(EntityIterableHandle object) {
                check(object, checker, mutatedInTxn);
                return true;
            }
        });
    }

    @Override
    void cacheObject(@NotNull EntityIterableHandle key, @NotNull CachedInstanceIterable it) {
        super.cacheObject(key, it);

        state.addHandle(key);
    }

    @Override
    void remove(@NotNull EntityIterableHandle key) {
        super.remove(key);

        state.removeHandle(key);
    }

    @Override
    void clear() {
        super.clear();

        state.clear();
    }

    private void check(@NotNull final EntityIterableHandle handle,
                       @NotNull final PersistentStoreTransaction.HandleChecker checker,
                       @NotNull List<UpdatableCachedInstanceIterable> mutatedInTxn) {
        switch (checker.checkHandle(handle, this)) {
            case KEEP:
                break; // do nothing, keep handle
            case REMOVE:
                remove(handle);
                break;
            case UPDATE:
                UpdatableCachedInstanceIterable it = (UpdatableCachedInstanceIterable) getObject(handle);
                if (it != null) {
                    if (!it.isMutated()) {
                        it = it.beginUpdate();
                        // cache new mutated iterable instance
                        cacheObject(handle, it);
                        mutatedInTxn.add(it);
                    }
                    checker.update(handle, it);
                }
        }
    }

    static EntityIterableCacheAdapterMutable create(@NotNull final EntityIterableCacheAdapter source) {
        State state = new State(source.cache);
        return new EntityIterableCacheAdapterMutable(source.config, state);
    }

    private static class State implements EvictListener<EntityIterableHandle, CacheItem> {
        private final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache;

        private final HashMap<Integer, Set<EntityIterableHandle>> byLink;
        // private final HashMap<Integer, ArrayList<EntityIterableHandle>> byType;

        State(@NotNull final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache) {
            this.cache = cache.getClone(this);
            byLink = new HashMap<>(cache.count());
            // byType = new HashMap<>(cache.count());

            cache.forEachEntry(new PairProcedure<EntityIterableHandle, CacheItem>() {
                @Override
                public boolean execute(EntityIterableHandle handle, CacheItem value) {
                    CachedInstanceIterable iterable = getCachedValue(value);
                    if (iterable != null) {
                        addHandle(handle);
                        // handle.getType(); handle.getType().getKind(); iterable.getEntityTypeId();
                    }
                    return true;
                }
            });
        }

        @Override
        public void onEvict(EntityIterableHandle key, CacheItem value) {
            removeHandle(key);
        }

        private void removeHandle(@NotNull EntityIterableHandle key) {
            for (int linkId : key.getLinkIds()) {
                Set<EntityIterableHandle> handles = byLink.get(linkId);
                if (handles != null) {
                    handles.remove(key);
                }
            }
        }

        private void addHandle(@NotNull EntityIterableHandle handle) {
            for (int linkId : handle.getLinkIds()) {
                Set<EntityIterableHandle> handles = byLink.get(linkId);
                if (handles == null) {
                    handles = new HashSet<>();
                    byLink.put(linkId, handles);
                }
                handles.add(handle);
            }
        }

        private void clear() {
            byLink.clear();
        }
    }
}