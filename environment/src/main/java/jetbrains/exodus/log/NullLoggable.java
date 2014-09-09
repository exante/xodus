/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

public final class NullLoggable extends RandomAccessLoggable {

    public static final int TYPE = 0;
    public static final int LENGTH = 1;
    @SuppressWarnings({"StaticVariableOfConcreteClass"})
    private static final NullLoggable PROTOTYPE = new NullLoggable(TYPE);

    NullLoggable(final long address) {
        super(address, TYPE, LENGTH, RandomAccessByteIterable.EMPTY, 0, NO_STRUCTURE_ID);
    }

    @NotNull
    @Override
    public RandomAccessByteIterable getData() {
        throw new IllegalStateException("Can't access NullLoggable.getData()");
    }

    @Override
    public int getDataLength() {
        throw new IllegalStateException("Can't access NullLoggable.getDataLength()");
    }

    public static NullLoggable create() {
        return PROTOTYPE;
    }

    public static boolean isNullLoggable(final int type) {
        return type == TYPE;
    }

    public static boolean isNullLoggable(@NotNull final Loggable loggable) {
        return isNullLoggable(loggable.getType());
    }
}