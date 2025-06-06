package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;

public class TestCustomFunctions extends AbstractTestCustomFunctions {
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }
}
