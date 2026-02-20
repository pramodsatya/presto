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
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/sidecar/function/FunctionMetadata.h"
#include "presto_cpp/main/sidecar/function/FunctionUtils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::cudf_velox;

namespace facebook::presto::cudf {
namespace {

json buildAggregateMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<FunctionSignaturePtr>& signatures) {
  const std::vector<protocol::FunctionKind> kinds = {
      protocol::FunctionKind::AGGREGATE};
  json j = json::array();
  json tj;
  for (const auto& kind : kinds) {
    for (const auto& signature : signatures) {
      if (auto functionMetadata =
              facebook::presto::buildFunctionMetadata(name, schema, kind, *signature, std::nullopt)) {
        protocol::to_json(tj, functionMetadata.value());
        j.push_back(tj);
      }
    }
  }
  return j;
}

} // namespace

json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  json j;
  
  // Get metadata for all registered CUDF scalar functions
  const auto& cudfSignatures = getCudfFunctionSignatureMap();
  for (const auto& [name, signatures] : cudfSignatures) {
    // Use "cudf" as the schema and the function name as is
    if (!signatures.empty()) {
      j[name] = buildScalarMetadata(name, "cudf", signatures);
    }
  }

  // Get metadata for all registered CUDF aggregate functions
  const auto& cudfAggSignatures = getCudfAggregationFunctionSignatureMap();
  for (const auto& [name, signatures] : cudfAggSignatures) {
    // Use "cudf" as the schema and the function name as is
    if (!signatures.empty()) {
      j[name] = buildAggregateMetadata(name, "cudf", signatures);
    }
  }

  return j;
}

} // namespace facebook::presto::cudf
