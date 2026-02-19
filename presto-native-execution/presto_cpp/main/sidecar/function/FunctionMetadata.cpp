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
#include "presto_cpp/main/sidecar/function/FunctionMetadataProvider.h"
#include "presto_cpp/main/sidecar/function/FunctionUtils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {

namespace {

class CpuFunctionMetadataProvider : public FunctionMetadataProvider {
 public:
  CpuFunctionMetadataProvider() {
    const auto signatures = facebook::velox::exec::getFunctionSignatures();
    for (const auto& entry : signatures) {
      const auto metadata = facebook::presto::util::getScalarMetadata(entry.first);
      std::vector<ScalarFunctionEntry> entries;
      entries.reserve(entry.second.size());
      for (const auto* sig : entry.second) {
        entries.push_back(ScalarFunctionEntry{metadata, sig});
      }
      scalarFunctions_[entry.first] = std::move(entries);
    }

    const auto aggregateFunctions = facebook::velox::exec::aggregateFunctions().copy();
    for (const auto& entry : aggregateFunctions) {
      if (entry.second.metadata.companionFunction) {
        continue;
      }
      aggregateFunctions_[entry.first] = entry.second.signatures;
    }

    windowFunctions_ = facebook::velox::exec::windowFunctions();
  }

  const ScalarFunctionMap& scalarFunctions() const override {
    return scalarFunctions_;
  }

  bool isScalarBlocked(const std::string& name) const override {
    static const std::unordered_set<std::string> kBlockList = {
        "row_constructor", "in", "is_null"};
    if (kBlockList.count(name) != 0 ||
        name.find("$internal$") != std::string::npos) {
      return true;
    }
    const auto it = scalarFunctions_.find(name);
    return it != scalarFunctions_.end() &&
        !it->second.empty() &&
        it->second.front().metadata.companionFunction;
  }

  const AggregateFunctionMap& aggregateFunctions() const override {
    return aggregateFunctions_;
  }

  const WindowFunctionMap& windowFunctions() const override {
    return windowFunctions_;
  }

  util::NameParts parseName(const std::string& name) const override {
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    return util::NameParts{parts[0], parts[1], parts[2], true};
  }

 private:
  ScalarFunctionMap scalarFunctions_;
  AggregateFunctionMap aggregateFunctions_;
  WindowFunctionMap windowFunctions_;
};

} // namespace

json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  CpuFunctionMetadataProvider provider;
  return provider.getFunctionsMetadata(catalog);
}

} // namespace facebook::presto
