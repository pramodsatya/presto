/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrcRecordReaderOptions
{
    private final DataSize maxMergeDistance;
    private final DataSize tinyStripeThreshold;
    private final DataSize maxBlockSize;
    private final boolean mapNullKeysEnabled;
    private final boolean appendRowNumber;
    private final long maxSliceSize;
    private final boolean resetAllReaders;

    public OrcRecordReaderOptions(OrcReaderOptions options)
    {
        this(options.getMaxMergeDistance(),
                options.getTinyStripeThreshold(),
                options.getMaxBlockSize(),
                options.mapNullKeysEnabled(),
                options.appendRowNumber(),
                options.getMaxSliceSize(),
                options.isResetAllReaders());
    }

    public OrcRecordReaderOptions(
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            boolean mapNullKeysEnabled,
            boolean appendRowNumber,
            DataSize maxSliceSize,
            boolean resetAllReaders)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.mapNullKeysEnabled = mapNullKeysEnabled;
        this.appendRowNumber = appendRowNumber;
        checkArgument(maxSliceSize.toBytes() < Integer.MAX_VALUE, "maxSliceSize cannot be larger than Integer.MAX_VALUE");
        checkArgument(maxSliceSize.toBytes() > 0, "maxSliceSize must be positive");
        this.maxSliceSize = maxSliceSize.toBytes();
        this.resetAllReaders = resetAllReaders;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public DataSize getMaxBlockSize()
    {
        return maxBlockSize;
    }

    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    public boolean mapNullKeysEnabled()
    {
        return mapNullKeysEnabled;
    }

    public boolean appendRowNumber()
    {
        return appendRowNumber;
    }

    public long getMaxSliceSize()
    {
        return maxSliceSize;
    }

    public boolean isResetAllReaders()
    {
        return resetAllReaders;
    }
}
