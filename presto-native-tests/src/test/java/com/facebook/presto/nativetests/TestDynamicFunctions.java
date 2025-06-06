package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestCustomFunctions;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.*;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

public class TestDynamicFunctions extends AbstractTestCustomFunctions {
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Path pluginDir = workingDir.resolve("presto-native-tests")
                .resolve("src")
                .resolve("test")
                .resolve("java")
                .resolve("resources");

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .setPluginDirectory(Optional.of(pluginDir))
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    @Test
    public void testCustomSum()
    {
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQuery(sql, sql.replace("custom_sum", "sum"));
    }
}
