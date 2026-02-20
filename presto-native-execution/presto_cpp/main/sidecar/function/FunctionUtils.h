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

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SimpleFunctionRegistry.h"

using json = nlohmann::json;

namespace facebook::presto {

/// Check if the Velox type is supported in Presto.
/// HUGEINT type is not supported in Presto.
bool isValidPrestoType(
    const facebook::velox::exec::TypeSignature& typeSignature);

/// Get aggregation function metadata for a given function and signature.
const protocol::AggregationFunctionMetadata getAggregationFunctionMetadata(
    const std::string& name,
    const facebook::velox::exec::AggregateFunctionSignature& signature);

/// Get type variable constraints from a function signature.
const std::vector<protocol::TypeVariableConstraint> getTypeVariableConstraints(
    const facebook::velox::exec::FunctionSignature& functionSignature);

/// Get long variable constraints from a function signature.
const std::vector<protocol::LongVariableConstraint> getLongVariableConstraints(
    const facebook::velox::exec::FunctionSignature& functionSignature);

/// Get routine characteristics for a function given its optional metadata.
const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const protocol::FunctionKind& kind,
    const std::optional<facebook::velox::exec::VectorFunctionMetadata>& metadata);

/// Build function metadata from signature and optional aggregate signature.
std::optional<protocol::JsonBasedUdfFunctionMetadata> buildFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const protocol::FunctionKind& kind,
    const facebook::velox::exec::FunctionSignature& signature,
    const std::optional<facebook::velox::exec::VectorFunctionMetadata>& scalarMetadata,
    const facebook::velox::exec::AggregateFunctionSignaturePtr& aggregateSignature = nullptr);

} // namespace facebook::presto::function_utils
