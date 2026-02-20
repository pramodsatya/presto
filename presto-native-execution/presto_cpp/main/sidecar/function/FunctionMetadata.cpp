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
#include "presto_cpp/main/sidecar/function/FunctionMetadata.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/sidecar/function/FunctionUtils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto {
namespace {

const exec::VectorFunctionMetadata getScalarMetadata(
    const std::string& name) {
  auto simpleFunctionMetadata =
      exec::simpleFunctions().getFunctionSignaturesAndMetadata(name);
  if (simpleFunctionMetadata.size()) {
    // Functions like abs are registered as simple functions for primitive
    // types, and as a vector function for complex types like DECIMAL. So do not
    // throw an error if function metadata is not found in simple function
    // signature map.
    return simpleFunctionMetadata.back().first;
  }

  auto vectorFunctionMetadata = exec::getVectorFunctionMetadata(name);
  if (vectorFunctionMetadata.has_value()) {
    return vectorFunctionMetadata.value();
  }

  VELOX_UNREACHABLE("Metadata for function {} not found", name);
}

json buildAggregateMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  // All aggregate functions can be used as window functions.
  VELOX_USER_CHECK(
      getWindowFunctionSignatures(name).has_value(),
      "Aggregate function {} not registered as a window function",
      name);

  // The functions returned by this endpoint are stored as SqlInvokedFunction
  // objects, with SqlFunctionId serving as the primary key. SqlFunctionId is
  // derived from both the functionName and argumentTypes parameters. Returning
  // the same function twice—once as an aggregate function and once as a window
  // function introduces ambiguity, as functionKind is not a component of
  // SqlFunctionId. For any aggregate function utilized as a window function,
  // the function’s metadata can be obtained from the associated aggregate
  // function implementation for further processing. For additional information,
  // refer to the following: 	•
  // https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/function/SqlFunctionId.java
  //  •
  //  https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/function/SqlInvokedFunction.java

  const std::vector<protocol::FunctionKind> kinds = {
      protocol::FunctionKind::AGGREGATE};
  json j = json::array();
  json tj;
  for (const auto& kind : kinds) {
    for (const auto& signature : signatures) {
      if (auto functionMetadata = buildFunctionMetadata(
              name, schema, kind, *signature, std::nullopt, signature)) {
        protocol::to_json(tj, functionMetadata.value());
        j.push_back(tj);
      }
    }
  }
  return j;
}

json buildWindowMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<FunctionSignaturePtr>& signatures) {
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::WINDOW, *signature, std::nullopt)) {
      protocol::to_json(tj, functionMetadata.value());
      j.push_back(tj);
    }
  }
  return j;
}

} // namespace

json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  json j;

  // Lambda to check if a function should be skipped based on catalog filter
  auto skipCatalog = [&catalog](const std::string& functionCatalog) {
    return catalog.has_value() && functionCatalog != catalog.value();
  };

  // Get metadata for all registered scalar functions in velox.
  const auto signatures = getFunctionSignatures();
  static const std::unordered_set<std::string> kBlockList = {
      "row_constructor", "in", "is_null"};
  // Exclude aggregate companion functions (extract aggregate companion
  // functions are registered as vector functions).
  const auto aggregateFunctions = exec::aggregateFunctions().copy();
  for (const auto& entry : signatures) {
    const auto name = entry.first;
    // Skip internal functions. They don't have any prefix.
    if (kBlockList.count(name) != 0 ||
        name.find("$internal$") != std::string::npos ||
        getScalarMetadata(name).companionFunction) {
      continue;
    }

    const auto parts = util::getFunctionNameParts(name);
    if (skipCatalog(parts[0])) {
      continue;
    }
    const auto schema = parts[1];
    const auto function = parts[2];
    j[function] = buildScalarMetadata(name, schema, entry.second, getScalarMetadata());
  }

  // Get metadata for all registered aggregate functions in velox.
  for (const auto& entry : aggregateFunctions) {
    if (!aggregateFunctions.at(entry.first).metadata.companionFunction) {
      const auto name = entry.first;
      const auto parts = util::getFunctionNameParts(name);
      if (skipCatalog(parts[0])) {
        continue;
      }
      const auto schema = parts[1];
      const auto function = parts[2];
      j[function] =
          buildAggregateMetadata(name, schema, entry.second.signatures);
    }
  }

  // Get metadata for all registered window functions in velox. Skip aggregates
  // as they have been processed.
  const auto& functions = exec::windowFunctions();
  for (const auto& entry : functions) {
    if (aggregateFunctions.count(entry.first) == 0) {
      const auto name = entry.first;
      const auto parts = util::getFunctionNameParts(entry.first);
      if (skipCatalog(parts[0])) {
        continue;
      }
      const auto schema = parts[1];
      const auto function = parts[2];
      j[function] = buildWindowMetadata(name, schema, entry.second.signatures);
    }
  }

  return j;
}

} // namespace facebook::presto
