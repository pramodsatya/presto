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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.presto.tests.operator.scalar.AbstractTestIpPrefix;
import org.testng.annotations.Test;

public class TestIpPrefixFunctions
        extends AbstractTestNativeFunctions
        implements AbstractTestIpPrefix
{
    // Velox sidecar cannot constant-fold IP_PREFIX() calls with IPADDRESS literal casts.
    @Override
    @Test(enabled = false)
    public void testIpAddressIpPrefix() {}

    @Override
    @Test(enabled = false)
    public void testStringIpPrefix() {}

    // Velox returns GENERIC_USER_ERROR (code 0) instead of INVALID_FUNCTION_ARGUMENT (code 7).
    @Override
    @Test(enabled = false)
    public void testIpPrefixCollapseNoNullPrefixesError() {}

    @Override
    @Test(enabled = false)
    public void testIpPrefixCollapseMixedIpVersionError() {}

    @Override
    @Test(enabled = false)
    public void testIpPrefixSubnetsInvalidPrefixLengths() {}
}
