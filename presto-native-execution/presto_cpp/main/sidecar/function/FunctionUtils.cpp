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

#include "presto_cpp/main/sidecar/function/FunctionUtils.h"
#include <boost/algorithm/string.hpp>
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {

bool isValidPrestoType(const facebook::velox::exec::TypeSignature& typeSignature) {
  if (typeSignature.parameters().empty()) {
    // Hugeint type is not supported in Presto.
    auto kindName = boost::algorithm::to_upper_copy(typeSignature.baseName());
    if (auto typeKind = facebook::velox::TypeKindName::tryToTypeKind(kindName)) {
      return typeKind.value() != facebook::velox::TypeKind::HUGEINT;
    }
  } else {
    for (const auto& paramType : typeSignature.parameters()) {
      if (!isValidPrestoType(paramType)) {
        return false;
      }
    }
  }
  return true;
}

const protocol::AggregationFunctionMetadata getAggregationFunctionMetadata(
  const std::string& name,
  const facebook::velox::exec::AggregateFunctionSignature& signature) {
  protocol::AggregationFunctionMetadata metadata;
  metadata.intermediateType =
      boost::algorithm::to_lower_copy(signature.intermediateType().toString());
  metadata.isOrderSensitive =
    facebook::velox::exec::getAggregateFunctionEntry(name)->metadata.orderSensitive;
  return metadata;
}

const std::vector<protocol::TypeVariableConstraint> getTypeVariableConstraints(
  const facebook::velox::exec::FunctionSignature& functionSignature) {
  std::vector<protocol::TypeVariableConstraint> typeVariableConstraints;
  const auto& functionVariables = functionSignature.variables();
  for (const auto& [name, signature] : functionVariables) {
    if (signature.isTypeParameter()) {
      protocol::TypeVariableConstraint typeVariableConstraint;
      typeVariableConstraint.name =
          boost::algorithm::to_lower_copy(signature.name());
      typeVariableConstraint.orderableRequired = signature.orderableTypesOnly();
      typeVariableConstraint.comparableRequired =
          signature.comparableTypesOnly();
      typeVariableConstraints.emplace_back(typeVariableConstraint);
    }
  }
  return typeVariableConstraints;
}

const std::vector<protocol::LongVariableConstraint> getLongVariableConstraints(
  const facebook::velox::exec::FunctionSignature& functionSignature) {
  std::vector<protocol::LongVariableConstraint> longVariableConstraints;
  const auto& functionVariables = functionSignature.variables();
  for (const auto& [name, signature] : functionVariables) {
    if (signature.isIntegerParameter() && !signature.constraint().empty()) {
      protocol::LongVariableConstraint longVariableConstraint;
      longVariableConstraint.name =
          boost::algorithm::to_lower_copy(signature.name());
      longVariableConstraint.expression =
          boost::algorithm::to_lower_copy(signature.constraint());
      longVariableConstraints.emplace_back(longVariableConstraint);
    }
  }
  return longVariableConstraints;
}

const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const protocol::FunctionKind& kind,
    const std::optional<facebook::velox::exec::VectorFunctionMetadata>& metadata) {
  protocol::Determinism determinism;
  protocol::NullCallClause nullCallClause;
  if (kind == protocol::FunctionKind::SCALAR && metadata.has_value()) {
    determinism = metadata.value().deterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    nullCallClause = metadata.value().defaultNullBehavior
        ? protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT
        : protocol::NullCallClause::CALLED_ON_NULL_INPUT;
  } else {
    // Default metadata values of DETERMINISTIC and CALLED_ON_NULL_INPUT for
    // non-scalar functions or when metadata is not available.
    determinism = protocol::Determinism::DETERMINISTIC;
    nullCallClause = protocol::NullCallClause::CALLED_ON_NULL_INPUT;
  }

  protocol::RoutineCharacteristics routineCharacteristics;
  routineCharacteristics.language =
      std::make_shared<protocol::Language>(protocol::Language({"CPP"}));
  routineCharacteristics.determinism =
      std::make_shared<protocol::Determinism>(determinism);
  routineCharacteristics.nullCallClause =
      std::make_shared<protocol::NullCallClause>(nullCallClause);
  return routineCharacteristics;
}

std::optional<protocol::JsonBasedUdfFunctionMetadata> buildFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const protocol::FunctionKind& kind,
    const facebook::velox::exec::FunctionSignature& signature,
    const std::optional<facebook::velox::exec::VectorFunctionMetadata>& scalarMetadata,
    const facebook::velox::exec::AggregateFunctionSignaturePtr& aggregateSignature) {
  protocol::JsonBasedUdfFunctionMetadata metadata;
  metadata.docString = name;
  metadata.functionKind = kind;
  if (!isValidPrestoType(signature.returnType())) {
    return std::nullopt;
  }
  metadata.outputType =
      boost::algorithm::to_lower_copy(signature.returnType().toString());

  const auto& argumentTypes = signature.argumentTypes();
  std::vector<std::string> paramTypes(argumentTypes.size());
  for (auto i = 0; i < argumentTypes.size(); i++) {
    if (!isValidPrestoType(argumentTypes.at(i))) {
      return std::nullopt;
    }
    paramTypes[i] =
        boost::algorithm::to_lower_copy(argumentTypes.at(i).toString());
  }
  metadata.paramTypes = paramTypes;
  metadata.schema = schema;
  metadata.variableArity = signature.variableArity();
  metadata.routineCharacteristics = getRoutineCharacteristics(kind, scalarMetadata);
  metadata.typeVariableConstraints =
      std::make_shared<std::vector<protocol::TypeVariableConstraint>>(
          getTypeVariableConstraints(signature));
  metadata.longVariableConstraints =
      std::make_shared<std::vector<protocol::LongVariableConstraint>>(
          getLongVariableConstraints(signature));

  if (aggregateSignature) {
    metadata.aggregateMetadata =
        std::make_shared<protocol::AggregationFunctionMetadata>(
            getAggregationFunctionMetadata(name, *aggregateSignature));
  }
  return metadata;
}

json buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<const FunctionSignature*>& signatures,
  const std::optional<VectorFunctionMetadata> metadata) {
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = facebook::presto::buildFunctionMetadata(
            name, schema, protocol::FunctionKind::SCALAR, *signature, std::nullopt)) {
      protocol::to_json(tj, functionMetadata.value());
      j.push_back(tj);
    }
  }
  return j;
}

} // namespace facebook::presto
