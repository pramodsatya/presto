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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.tdigest.Centroid;
import com.facebook.presto.tdigest.TDigest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public final class TDigestFunctions
{
    public static final double DEFAULT_COMPRESSION = 100;

    @VisibleForTesting
    static final RowType TDIGEST_CENTROIDS_ROW_TYPE = RowType.from(
            ImmutableList.of(
                    RowType.field("centroid_means", new ArrayType(DOUBLE)),
                    RowType.field("centroid_weights", new ArrayType(INTEGER)),
                    RowType.field("compression", DOUBLE),
                    RowType.field("min", DOUBLE),
                    RowType.field("max", DOUBLE),
                    RowType.field("sum", DOUBLE),
                    RowType.field("count", BIGINT)));

    private TDigestFunctions() {}

    @ScalarFunction(value = "value_at_quantile", visibility = EXPERIMENTAL)
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantileDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return createTDigest(input).getQuantile(quantile);
    }

    @ScalarFunction(value = "values_at_quantiles", visibility = EXPERIMENTAL)
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType("array(double)")
    public static Block valuesAtQuantilesDouble(@SqlType("tdigest(double)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        TDigest tDigest = createTDigest(input);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "All quantiles should be non-null.");
            DOUBLE.writeDouble(output, tDigest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)));
        }
        return output.build();
    }

    @ScalarFunction(value = "quantile_at_value", visibility = EXPERIMENTAL)
    @Description("Given an input x between min/max values of t-digest, find which quantile is represented by that value")
    @SqlType(StandardTypes.DOUBLE)
    public static double quantileAtValueDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return createTDigest(input).getCdf(value);
    }

    @ScalarFunction(value = "quantiles_at_values", visibility = EXPERIMENTAL)
    @Description("For each input x between min/max values of t-digest, find which quantile is represented by that value")
    @SqlType("array(double)")
    public static Block quantilesAtValuesDouble(@SqlType("tdigest(double)") Slice input, @SqlType("array(double)") Block valuesArrayBlock)
    {
        TDigest tDigest = createTDigest(input);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, valuesArrayBlock.getPositionCount());
        for (int i = 0; i < valuesArrayBlock.getPositionCount(); i++) {
            checkCondition(!valuesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "All values should be non-null.");
            DOUBLE.writeDouble(output, tDigest.getCdf(DOUBLE.getDouble(valuesArrayBlock, i)));
        }
        return output.build();
    }

    @ScalarFunction(value = "scale_tdigest", visibility = EXPERIMENTAL)
    @Description("Scale a t-digest according to a new weight")
    @SqlType("tdigest(double)")
    public static Slice scaleTDigestDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double scale)
    {
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "Scale factor should be positive.");
        TDigest digest = createTDigest(input);
        digest.scale(scale);
        return digest.serialize();
    }

    @ScalarFunction(value = "destructure_tdigest", visibility = EXPERIMENTAL)
    @Description("Return the raw TDigest, including arrays of centroid means and weights, as well as min, max, sum, count, and compression factor.")
    @SqlType("row(centroid_means array(double), centroid_weights array(integer), compression double, min double, max double, sum double, count bigint)")
    public static Block destructureTDigest(@SqlType("tdigest(double)") Slice input)
    {
        TDigest tDigest = createTDigest(input);

        BlockBuilder blockBuilder = TDIGEST_CENTROIDS_ROW_TYPE.createBlockBuilder(null, 1);
        BlockBuilder rowBuilder = blockBuilder.beginBlockEntry();

        // Centroid means / weights
        BlockBuilder meansBuilder = DOUBLE.createBlockBuilder(null, tDigest.centroidCount());
        BlockBuilder weightsBuilder = INTEGER.createBlockBuilder(null, tDigest.centroidCount());
        for (Centroid centroid : tDigest.centroids()) {
            int weight = (int) centroid.getWeight();
            DOUBLE.writeDouble(meansBuilder, centroid.getMean());
            INTEGER.writeLong(weightsBuilder, weight);
        }
        rowBuilder.appendStructure(meansBuilder);
        rowBuilder.appendStructure(weightsBuilder);

        // Compression, min, max, sum, count
        DOUBLE.writeDouble(rowBuilder, tDigest.getCompressionFactor());
        DOUBLE.writeDouble(rowBuilder, tDigest.getMin());
        DOUBLE.writeDouble(rowBuilder, tDigest.getMax());
        DOUBLE.writeDouble(rowBuilder, tDigest.getSum());
        BIGINT.writeLong(rowBuilder, (long) tDigest.getSize());

        blockBuilder.closeEntry();
        return TDIGEST_CENTROIDS_ROW_TYPE.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    @ScalarFunction(value = "trimmed_mean", visibility = EXPERIMENTAL)
    @Description("Returns an estimate of the mean, excluding portions of the distribution outside the provided quantile bounds.")
    @SqlType("double")
    public static double trimmedMeanTDigestDouble(@SqlType("tdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double lowerQuantileBound, @SqlType(StandardTypes.DOUBLE) double upperQuantileBound)
    {
        checkCondition(lowerQuantileBound >= 0 && lowerQuantileBound <= 1, INVALID_FUNCTION_ARGUMENT, "Lower quantile bound should be in [0,1].");
        checkCondition(upperQuantileBound >= 0 && upperQuantileBound <= 1, INVALID_FUNCTION_ARGUMENT, "Upper quantile bound should be in [0,1].");
        TDigest digest = createTDigest(input);
        return digest.trimmedMean(lowerQuantileBound, upperQuantileBound);
    }

    @ScalarFunction(value = "construct_tdigest", visibility = EXPERIMENTAL)
    @Description("Create a TDigest by passing in its internal state.")
    @SqlType("tdigest(double)")
    public static Slice constructTDigest(
            @SqlType("array(double)") Block centroidMeansBlock,
            @SqlType("array(double)") Block centroidWeightsBlock,
            @SqlType(StandardTypes.DOUBLE) double compression,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max,
            @SqlType(StandardTypes.DOUBLE) double sum,
            @SqlType(StandardTypes.BIGINT) long count)
    {
        double[] centroidMeans = new double[centroidMeansBlock.getPositionCount()];
        for (int i = 0; i < centroidMeansBlock.getPositionCount(); i++) {
            centroidMeans[i] = DOUBLE.getDouble(centroidMeansBlock, i);
        }
        double[] centroidWeights = new double[centroidWeightsBlock.getPositionCount()];
        for (int i = 0; i < centroidWeightsBlock.getPositionCount(); i++) {
            centroidWeights[i] = DOUBLE.getDouble(centroidWeightsBlock, i);
        }

        TDigest tDigest = createTDigest(
                centroidMeans,
                centroidWeights,
                compression,
                min,
                max,
                sum,
                toIntExact(count));

        return tDigest.serialize();
    }

    @ScalarFunction(value = "merge_tdigest", visibility = EXPERIMENTAL)
    @Description("Merge an array of TDigests into a single TDigest")
    @SqlType("tdigest(double)")
    @SqlNullable
    public static Slice merge_tdigest(@SqlType("array(tdigest(double))") Block input)
    {
        if (input.getPositionCount() == 0) {
            return null;
        }
        TDigest output = null;
        for (int i = 0; i < input.getPositionCount(); i++) {
            if (input.isNull(i)) {
                continue;
            }
            TDigest tdigest = createTDigest(input.getSlice(i, 0, input.getSliceLength(i)));
            if (output == null) {
                output = tdigest;
            }
            else {
                output.merge(tdigest);
            }
        }
        return output == null ? null : output.serialize();
    }
}
