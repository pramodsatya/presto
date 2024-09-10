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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;

namespace facebook::presto::expression {

/// When 'isSimplified' is true, the cases (or arguments) in switch expression
/// have been simplified when the expression was constant folded in Velox. In
/// this case, the expression corresponding to the first switch case that always
/// evaluates to true is returned in 'caseExpression'.
/// When 'isSimplified' is false, the cases in switch expression have not been
/// simplified, and the switch expression arguments required by Presto are
/// returned in 'arguments'.
struct SwitchFormArguments {
  bool isSimplified = false;
  json caseExpression;
  json::array_t arguments;
};

/// Helper class to convert Velox expression of type exec::Expr to its
/// corresponding Presto expression of type protocol::RowExpression. The
/// function veloxToPrestoRowExpression is used in RowExpressionOptimizer to
/// convert the constant folded velox expression of type exec::Expr to a Presto
/// expression of type protocol::RowExpression:
/// 1. A constant velox expression of type exec::ConstantExpr without any inputs
///    is converted to Presto expression of type protocol::ConstantExpression.
///    If the velox constant expression is of ROW type, it is converted to a
///    Presto expression of type protocol::SpecialFormExpression (with Form as
///    ROW_CONSTRUCTOR).
/// 2. Velox expression representing a variable is of type exec::FieldReference,
///    it is converted to a Presto expression of type
///    protocol::VariableReferenceExpression.
/// 3. Special form expressions and expressions with a vector function in Velox
///    can map either to a Presto protocol::SpecialFormExpression or to a Presto
///    protocol::CallExpression. Based on the expression name, we decide whether
///    the velox expression is converted to a Presto expression of type
///    protocol::SpecialFormExpression or of type protocol::CallExpression.
///
/// The function getConstantRowExpression is used in RowExpressionOptimizer to
/// convert a velox constant expression of type exec::ConstantExpr to a Presto
/// constant expression of type protocol::ConstantExpression.
class RowExpressionConverter {
 public:
  explicit RowExpressionConverter(memory::MemoryPool* pool) : pool_(pool) {}

  /// Converts a velox constant expression of type exec::ConstantExpr to a
  /// Presto constant expression of type protocol::ConstantExpression.
  std::shared_ptr<protocol::ConstantExpression> getConstantRowExpression(
      const std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  /// Converts the velox expression of type exec::Expr to a Presto expression of
  /// type protocol::RowExpression serialized as json. Called from the function
  /// optimize in RowExpressionOptimizer with two arguments:
  /// 1. expr: The constant folded velox expression.
  /// 2. inputRowExpr: The input Presto expression to RowExpressionOptimizer
  ///    of type protocol::RowExpression (serialized as json), before it is
  ///    optimized and constant folded.
  json veloxToPrestoRowExpression(
      const exec::ExprPtr& expr,
      const json& inputRowExpr);

 private:
  /// ValueBlock in Presto expression of type protocol::ConstantExpression
  /// requires only the column from the serialized PrestoPage without the page
  /// header. This function is used to serialize a velox Vector to ValueBlock.
  std::string getValueBlock(const VectorPtr& vector);

  /// Helper function to get arguments for switch special form expression when
  /// the CASE expression is of 'simple' form (please refer to the documentation
  /// at: https://prestodb.io/docs/current/functions/conditional.html#case).
  SwitchFormArguments getSimpleSwitchFormArgs(
      const exec::SwitchExpr* switchExpr,
      const json::array_t& inputArgs);

  /// Helper function to get arguments for Presto special form expression of
  /// type protocol::SpecialFormExpression with SWITCH form from a velox switch
  /// expression of type exec::SwitchExpr.
  SwitchFormArguments getSwitchSpecialFormArgs(
      const exec::SwitchExpr* switchExpr,
      const json& input);

  /// Helper function to construct a Presto special form expression of type
  /// protocol::SpecialFormExpression (serialized as json) from a velox
  /// expression of type exec::Expr.
  json getSpecialForm(const exec::ExprPtr& expr, const json& input);

  /// Helper function to construct a Presto special form expression of type
  /// protocol::SpecialFormExpression with ROW_CONSTRUCTOR form (serialized as
  /// json) from a velox constant expression of type exec::ConstantExpr.
  json getRowConstructorSpecialForm(
      std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  /// Helper function to construct a Presto constant expression of type
  /// protocol::ConstantExpression (serialized as json) from a velox expression
  /// of type exec::Expr. When the velox expression is of ROW type, the helper
  /// function getRowConstructorSpecialForm is called to return a Presto
  /// protocol::SpecialFormExpression.
  json toConstantRowExpression(const exec::ExprPtr& expr);

  /// Helper function to construct a Presto call expression of type
  /// protocol::CallExpression (serialized as json) from a velox expression
  /// of type exec::Expr.
  json toCallRowExpression(const exec::ExprPtr& expr, const json& input);

  memory::MemoryPool* pool_;
  const std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
};
} // namespace facebook::presto::expression
