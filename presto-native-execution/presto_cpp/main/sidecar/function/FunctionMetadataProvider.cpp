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

#include "presto_cpp/main/sidecar/function/FunctionMetadataProvider.h"

#include <boost/algorithm/string.hpp>

#include "presto_cpp/main/sidecar/function/FunctionUtils.h"

namespace facebook::presto {

using facebook::presto::util::getLongVariableConstraints;
using facebook::presto::util::getTypeVariableConstraints;
using facebook::presto::util::isValidPrestoType;

nlohmann::json FunctionMetadataProvider::buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<ScalarFunctionEntry>& entries) const {
  nlohmann::json j = nlohmann::json::array();
  nlohmann::json tj;
  for (const auto& entry : entries) {
    const auto* signature = entry.signature;
    if (!isValidPrestoType(signature->returnType())) {
      continue;
    }

    const auto& argumentTypes = signature->argumentTypes();
    std::vector<std::string> paramTypes(argumentTypes.size());
    bool validSignature = true;
    for (size_t i = 0; i < argumentTypes.size(); ++i) {
      if (!isValidPrestoType(argumentTypes.at(i))) {
        validSignature = false;
        break;
      }
      paramTypes[i] =
          boost::algorithm::to_lower_copy(argumentTypes.at(i).toString());
    }
    if (!validSignature) {
      continue;
    }

    protocol::JsonBasedUdfFunctionMetadata metadata;
    metadata.docString = name;
    metadata.functionKind = protocol::FunctionKind::SCALAR;
    metadata.outputType =
        boost::algorithm::to_lower_copy(signature->returnType().toString());
    metadata.paramTypes = paramTypes;
    metadata.schema = schema;
    metadata.variableArity = signature->variableArity();

    protocol::RoutineCharacteristics routineCharacteristics;
    protocol::Determinism determinism = entry.metadata.deterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    protocol::NullCallClause nullCallClause =
        entry.metadata.defaultNullBehavior
        ? protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT
        : protocol::NullCallClause::CALLED_ON_NULL_INPUT;
    routineCharacteristics.language =
        std::make_shared<protocol::Language>(protocol::Language({"CPP"}));
    routineCharacteristics.determinism =
        std::make_shared<protocol::Determinism>(determinism);
    routineCharacteristics.nullCallClause =
        std::make_shared<protocol::NullCallClause>(nullCallClause);
    metadata.routineCharacteristics = routineCharacteristics;

    metadata.typeVariableConstraints =
        std::make_shared<std::vector<protocol::TypeVariableConstraint>>(
            getTypeVariableConstraints(*signature));
    metadata.longVariableConstraints =
        std::make_shared<std::vector<protocol::LongVariableConstraint>>(
            getLongVariableConstraints(*signature));

    protocol::to_json(tj, metadata);
    j.push_back(tj);
  }
  return j;
}

nlohmann::json FunctionMetadataProvider::getFunctionsMetadata(
    const std::optional<std::string>& catalog) const {
  nlohmann::json j;

  // Scalars
  for (const auto& entry : scalarFunctions()) {
    const auto& name = entry.first;
    if (isScalarBlocked(name)) {
      continue;
    }
    const auto parts = parseName(name);
    if (shouldSkipCatalog(catalog, parts)) {
      continue;
    }
    j[parts.function] = buildScalarMetadata(name, parts.schema, entry.second);
  }

  // Aggregates
  for (const auto& entry : aggregateFunctions()) {
    const auto& name = entry.first;
    const auto parts = parseName(name);
    if (shouldSkipCatalog(catalog, parts)) {
      continue;
    }
    j[parts.function] = facebook::presto::util::buildAggregateMetadata(
        name,
        parts.schema,
        entry.second,
        requireWindowRegistrationForAggregates());
  }

  // Windows
  for (const auto& entry : windowFunctions()) {
    const auto& name = entry.first;
    const auto parts = parseName(name);
    if (shouldSkipCatalog(catalog, parts)) {
      continue;
    }
    j[parts.function] = facebook::presto::util::buildWindowMetadata(
        name, parts.schema, entry.second);
  }

  return j;
}

} // namespace facebook::presto
