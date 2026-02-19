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
#include <gtest/gtest.h>

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/sidecar/function/CudfFunctionMetadata.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox;
using namespace facebook::presto::cudf;
using namespace facebook::velox::cudf_velox;
using namespace facebook::presto::test::utils;

using json = nlohmann::json;

static const std::string kPrestoDefaultPrefix = "presto.default.";

class FunctionMetadataTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    aggregate::prestosql::registerAllAggregateFunctions(kPrestoDefaultPrefix);
    window::prestosql::registerAllWindowFunctions(kPrestoDefaultPrefix);
    functions::prestosql::registerAllScalarFunctions(kPrestoDefaultPrefix);
    // Register CUDF builtin functions with a prefix
    registerBuiltinFunctions("cudf.");
    // Register CUDF builtin aggregation functions
    registerStepAwareBuiltinAggregationFunctions("cudf.");
  }

  void SetUp() override {
    functionMetadata_ = getFunctionsMetadata();
  }

  void sortMetadataList(json::array_t& list) {
    for (auto& metadata : list) {
      // Sort constraint arrays for deterministic test comparisons.
      for (auto const& [key, val] : metadata.items()) {
        if (key.ends_with("Constraints") && metadata[key].is_array()) {
          std::sort(
              metadata[key].begin(),
              metadata[key].end(),
              [](const json& a, const json& b) { return a.dump() < b.dump(); });
        }
      }
    }
    std::sort(list.begin(), list.end(), [](const json& a, const json& b) {
      return folly::hasher<std::string>()(
                 a["functionKind"].dump() + a["paramTypes"].dump()) <
          folly::hasher<std::string>()(
                 b["functionKind"].dump() + b["paramTypes"].dump());
    });
  }

  void testFunction(
      const std::string& name,
      const std::string& expectedFile,
      size_t expectedSize) {
    json::array_t metadataList = functionMetadata_.at(name);
    EXPECT_EQ(metadataList.size(), expectedSize);
    std::string expectedStr = slurp(
        facebook::presto::test::utils::getDataPath(
            "/github/presto-trunk/presto-native-execution/presto_cpp/main/sidecar/function/tests/data/",
            expectedFile));
    auto expected = json::parse(expectedStr);

    json::array_t expectedList = expected[name];
    sortMetadataList(expectedList);
    sortMetadataList(metadataList);
    for (auto i = 0; i < expectedSize; i++) {
      EXPECT_EQ(expectedList[i], metadataList[i]) << "Position: " << i;
    }
  }

  json functionMetadata_;
};

// Tests for CUDF scalar functions
TEST_F(FunctionMetadataTest, cudfCardinality) {
  // Test CUDF cardinality function
  testFunction("cudf.cardinality", "CudfCardinality.json", 1);
}

// Tests for CUDF aggregate functions
TEST_F(FunctionMetadataTest, cudfSum) {
  // Test CUDF sum aggregate function
  testFunction("cudf.sum", "CudfSum.json", 6);
}

// Test that metadata is returned as a JSON object
TEST_F(FunctionMetadataTest, cudfMetadataStructure) {
  // The result should be a JSON object with function names as keys
  ASSERT_TRUE(functionMetadata_.is_object());
  ASSERT_FALSE(functionMetadata_.empty());

  // Verify that CUDF functions are present
  ASSERT_TRUE(functionMetadata_.contains("cudf.cardinality"));
  ASSERT_TRUE(functionMetadata_.contains("cudf.sum"));

  // Each function should have an array of signatures
  for (auto it = functionMetadata_.begin(); it != functionMetadata_.end();
       ++it) {
    ASSERT_TRUE(it.value().is_array()) << "Function: " << it.key();
    ASSERT_FALSE(it.value().empty()) << "Function: " << it.key();

    // Each signature should have the required fields
    for (const auto& signature : it.value()) {
      ASSERT_TRUE(signature.contains("outputType")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("paramTypes")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("schema")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("functionKind"))
          << "Function: " << it.key();

      // Schema should be "cudf"
      EXPECT_EQ(signature["schema"], "cudf") << "Function: " << it.key();
    }
  }
}
