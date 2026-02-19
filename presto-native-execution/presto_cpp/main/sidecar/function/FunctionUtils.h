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

namespace facebook::presto::function_utils {

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

/// Get scalar function metadata, optionally providing default for CUDF
/// functions.
const facebook::velox::exec::VectorFunctionMetadata getScalarMetadata(
    const std::string& name,
    bool provideCudfDefaults = false);

/// Get routine characteristics for a function.
const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const std::string& name,
    const protocol::FunctionKind& kind,
    bool provideCudfDefaults = false);

} // namespace facebook::presto::function_utils
