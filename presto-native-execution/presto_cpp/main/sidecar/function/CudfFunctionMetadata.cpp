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
#include "presto_cpp/main/sidecar/function/FunctionMetadataProvider.h"
#include "presto_cpp/main/sidecar/function/FunctionUtils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "folly/String.h"
#include <boost/algorithm/string.hpp>
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto::cudf {

namespace {

util::NameParts parseFunctionName(const std::string& registeredName) {
  std::vector<std::string> parts;
  folly::split('.', registeredName, parts, true);
  VELOX_USER_CHECK(
      parts.size() >= 2,
      "Invalid function name format for CUDF function {}",
      registeredName);
  if (parts.size() == 3) {
    return util::NameParts{parts[0], parts[1], parts[2], true};
  }
  return util::NameParts{"", parts[0], parts[1], false};
}

class CudfFunctionMetadataProvider : public facebook::presto::FunctionMetadataProvider {
 public:
  CudfFunctionMetadataProvider() {
    const auto signaturesWithMetadata =
        facebook::velox::cudf_velox::getCudfFunctionSignaturesAndMetadata();
    for (const auto& entry : signaturesWithMetadata) {
      std::vector<ScalarFunctionEntry> entries;
      entries.reserve(entry.second.size());
      for (const auto& [metadata, sig] : entry.second) {
        entries.push_back(ScalarFunctionEntry{metadata, sig});
      }
      scalarFunctions_[entry.first] = std::move(entries);
    }

    const auto& aggSignatures =
        facebook::velox::cudf_velox::getCudfAggregationFunctionSignatureMap();
    for (const auto& entry : aggSignatures) {
      aggregateFunctions_[entry.first] = entry.second;
    }
  }

  const ScalarFunctionMap& scalarFunctions() const override {
    return scalarFunctions_;
  }

  const AggregateFunctionMap& aggregateFunctions() const override {
    return aggregateFunctions_;
  }

  const WindowFunctionMap& windowFunctions() const override {
    static const WindowFunctionMap kEmpty;
    return kEmpty;
  }

  bool requireWindowRegistrationForAggregates() const override {
    return false;
  }

  util::NameParts parseName(const std::string& name) const override {
    return parseFunctionName(name);
  }

 private:
  ScalarFunctionMap scalarFunctions_;
  AggregateFunctionMap aggregateFunctions_;
};

} // namespace

json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  CudfFunctionMetadataProvider provider;
  return provider.getFunctionsMetadata(catalog);
}

} // namespace facebook::presto::cudf
