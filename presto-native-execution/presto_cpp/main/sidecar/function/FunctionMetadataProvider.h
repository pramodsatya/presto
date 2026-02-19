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

#pragma once

#include <optional>
#include <unordered_map>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::presto::util {
struct NameParts {
  std::string catalog;
  std::string schema;
  std::string function;
  bool hasCatalog{false};
};
}

namespace facebook::presto {

struct ScalarFunctionEntry {
  facebook::velox::exec::VectorFunctionMetadata metadata;
  const facebook::velox::exec::FunctionSignature* signature;
};

using ScalarFunctionMap = std::unordered_map<
    std::string,
    std::vector<ScalarFunctionEntry>>;

using AggregateFunctionMap = std::unordered_map<
    std::string,
    std::vector<facebook::velox::exec::AggregateFunctionSignaturePtr>>;

using WindowFunctionMap = std::unordered_map<
    std::string,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>>;

class FunctionMetadataProvider {
 public:
  virtual ~FunctionMetadataProvider() = default;

  nlohmann::json getFunctionsMetadata(
      const std::optional<std::string>& catalog) const;

 protected:
  virtual util::NameParts parseName(const std::string& name) const = 0;

  virtual const ScalarFunctionMap& scalarFunctions() const = 0;
  virtual bool isScalarBlocked(const std::string& name) const {
    return false;
  }

  virtual const AggregateFunctionMap& aggregateFunctions() const = 0;
  virtual bool requireWindowRegistrationForAggregates() const {
    return true;
  }

  virtual const WindowFunctionMap& windowFunctions() const = 0;

  virtual bool shouldSkipCatalog(
      const std::optional<std::string>& catalog,
      const util::NameParts& parts) const {
    return catalog.has_value() && parts.hasCatalog &&
        parts.catalog != catalog.value();
  }

  nlohmann::json buildScalarMetadata(
      const std::string& name,
      const std::string& schema,
      const std::vector<ScalarFunctionEntry>& entries) const;
};

} // namespace facebook::presto
