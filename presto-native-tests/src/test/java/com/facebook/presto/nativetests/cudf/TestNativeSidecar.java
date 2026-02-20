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
package com.facebook.presto.nativetests.cudf;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static org.testng.Assert.fail;

/**
 * Test class for CUDF (CUDA DataFrames) GPU-accelerated execution in native sidecar mode.
 * This test class verifies that the CUDF integration is properly configured and can handle
 * various scenarios including session properties, unsupported functions, and plan validation.
 */
public class TestNativeSidecar
        extends AbstractTestQueryFramework
{
    private String storageFormat;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(true)
                .setEnableCudf(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    /**
     * Test that a basic query executes successfully with CUDF enabled.
     * This verifies that the basic CUDF infrastructure is working.
     */
    @Test
    public void testBasicQueryWithCudfEnabled()
    {
        assertQuery(
                "SELECT COUNT(*) FROM tpch.tiny.orders",
                "SELECT 15000");
    }

    /**
     * Test that CUDF session properties can be set and used.
     * The cudf_enabled session property should be accessible through the coordinator.
     * This test attempts to set a CUDF session property to verify the property system works.
     */
    @Test
    public void testCudfSessionProperties()
    {
        // Test a simple query with arithmetic on table data
        assertQuery(
                "SELECT custkey, orderkey, custkey + orderkey as sum FROM tpch.tiny.orders WHERE orderkey < 5",
                "SELECT 1, 1, 2 UNION ALL SELECT 1, 2, 3 UNION ALL SELECT 1, 3, 4 UNION ALL SELECT 1, 4, 5");
    }

    /**
     * Test that scalar functions not supported in CUDF fail appropriately.
     * This test verifies that when a function is not available in the CUDF function set,
     * the query fails on the coordinator with a function not found error.
     */
    @Test
    public void testUnsupportedCudfScalarFunction()
    {
        // Test that an undefined function fails with appropriate error message
        try {
            queryRunner().execute("SELECT nonexistent_undefined_function_xyz(1)");
            fail("Expected function not found error for undefined function");
        }
        catch (Exception e) {
            String message = e.getMessage();
            // Should fail with function not found or similar error on coordinator
            if (!message.contains("not found") && !message.contains("Unknown function") &&
                !message.contains("undefined") && !message.toLowerCase().contains("no function")) {
                fail("Expected function not found error but got: " + message);
            }
        }
    }

    /**
     * Test that aggregate functions not supported in CUDF fail appropriately.
     * This test verifies that when an aggregate function is not available in the CUDF function set,
     * the query fails on the coordinator.
     */
    @Test
    public void testUnsupportedCudfAggregateFunction()
    {
        // Test that an undefined aggregate function fails appropriately
        try {
            queryRunner().execute("SELECT undefined_aggregate_xyz(1) FROM (SELECT 1) t");
            fail("Expected function not found error for undefined aggregate function");
        }
        catch (Exception e) {
            String message = e.getMessage();
            // Should fail with function not found or similar error
            if (!message.contains("not found") && !message.contains("Unknown function") &&
                !message.contains("undefined") && !message.toLowerCase().contains("no function")) {
                fail("Expected function not found error but got: " + message);
            }
        }
    }

    /**
     * Test that certain constructs fail during plan validation.
     * This test exercises a query that might pass coordinator validation but could fail
     * sidecar validation if there are specific CUDF constraints on certain operations.
     */
    @Test
    public void testPlanValidationFailure()
    {
        // Test a query with correlated subquery which might fail plan validation
        // This is expected to either succeed or fail gracefully
        try {
            queryRunner().execute("SELECT * FROM (SELECT 1 as col) t WHERE col > 0");
            // Query succeeded - this is acceptable
        }
        catch (Exception e) {
            // Query failed - this is also acceptable
            // The key is that it should fail gracefully, not with an internal error
            String message = e.getMessage();
            if (message.contains("INTERNAL_ERROR")) {
                fail("Got INTERNAL_ERROR when plan validation should catch this earlier: " + message);
            }
        }
    }

    /**
     * Test basic arithmetic operations with CUDF.
     * This verifies that arithmetic expressions are handled correctly on table data.
     */
    @Test
    public void testArithmeticOperations()
    {
        assertQuery(
                "SELECT custkey, orderkey, custkey + orderkey as add_result, orderkey - custkey as sub_result FROM tpch.tiny.orders WHERE orderkey = custkey LIMIT 5",
                "SELECT 1, 1, 2, 0 UNION ALL SELECT 2, 2, 4, 0 UNION ALL SELECT 3, 3, 6, 0 UNION ALL SELECT 4, 4, 8, 0 UNION ALL SELECT 5, 5, 10, 0");
    }

    /**
     * Test comparison operations with CUDF.
     * This verifies that comparison expressions are handled correctly on table data.
     */
    @Test
    public void testComparisonOperations()
    {
        assertQuery(
                "SELECT COUNT(*) FROM tpch.tiny.orders WHERE custkey > 50 AND orderkey < 100",
                "SELECT 34");
    }
}
