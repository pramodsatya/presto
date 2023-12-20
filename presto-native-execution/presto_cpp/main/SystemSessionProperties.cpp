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
#include "velox/core/QueryConfig.h"

#include "presto_cpp/main/SystemSessionProperties.h"
#include "presto_cpp/main/common/Configs.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto {

void SystemSessionProperties::addSessionProperty(
    const std::string& name,
    const std::string& description,
    const TypePtr& type,
    const bool isHidden,
    const std::string& veloxConfigName,
    const std::optional<std::string> veloxDefault) {
  std::string defaultVal;
  if (veloxDefault.has_value()) {
    defaultVal = veloxDefault.value();
  } else {
    defaultVal = "none";
  }
  sessionProperties_[name] = std::make_unique<SessionProperty>(
      name,
      description,
      type->toString(),
      isHidden,
      veloxConfigName,
      defaultVal);
}

SystemSessionProperties::SystemSessionProperties() {
  auto baseVeloxQueryConfig = BaseVeloxQueryConfig::instance();
  // Use empty instance to get default property values.
  velox::core::QueryConfig queryConfig{{}};
  using velox::core::QueryConfig;
  // List of native session properties is kept as the source of truth in
  // presto-native.
  addSessionProperty(
      kExprEvalSimplified,
      "Native Execution only. Enable simplified path in expression evaluation",
      BOOLEAN(),
      false,
      QueryConfig::kExprEvalSimplified,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kExprEvalSimplified));

  addSessionProperty(
      kMaxSpillLevel,
      "Native Execution only. The maximum allowed spilling level for hash join "
      "build. 0 is the initial spilling level, -1 means unlimited.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillLevel,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kMaxSpillLevel));

  addSessionProperty(
      kMaxSpillFileSize,
      "The max allowed spill file size. If it is zero, then there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillFileSize,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kMaxSpillFileSize));

  addSessionProperty(
      kSpillCompressionKind,
      "Native Execution only. The compression algorithm type to compress the "
      "spilled data. Supported compression codecs are: ZLIB, SNAPPY, LZO, "
      "ZSTD, LZ4 and GZIP. NONE means no compression.",
      VARCHAR(),
      false,
      QueryConfig::kSpillCompressionKind,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kSpillCompressionKind));

  addSessionProperty(
      kSpillWriteBufferSize,
      "Native Execution only. The maximum size in bytes to buffer the serialized "
      "spill data before writing to disk for IO efficiency. If set to zero, "
      "buffering is disabled.",
      BIGINT(),
      false,
      QueryConfig::kSpillWriteBufferSize,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kSpillWriteBufferSize));

  addSessionProperty(
      kSpillFileCreateConfig,
      "Native Execution only. Config used to create spill files. This config is "
      "provided to underlying file system and the config is free form. The form should be "
      "defined by the underlying file system.",
      VARCHAR(),
      false,
      QueryConfig::kSpillFileCreateConfig,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kSpillFileCreateConfig));

  addSessionProperty(
      kJoinSpillEnabled,
      "Native Execution only. Enable join spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kJoinSpillEnabled,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kJoinSpillEnabled));

  addSessionProperty(
      kWindowSpillEnabled,
      "Native Execution only. Enable window spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWindowSpillEnabled,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kWindowSpillEnabled));

  addSessionProperty(
      kWriterSpillEnabled,
      "Native Execution only. Enable writer spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWriterSpillEnabled,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kWriterSpillEnabled));

  addSessionProperty(
      kRowNumberSpillEnabled,
      "Native Execution only. Enable row number spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kRowNumberSpillEnabled,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kRowNumberSpillEnabled));

  addSessionProperty(
      kJoinSpillPartitionBits,
      "Native Execution only. The number of bits (N) used to calculate the "
      "spilling partition number for hash join and RowNumber: 2 ^ N",
      INTEGER(),
      false,
      QueryConfig::kJoinSpillPartitionBits,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kRowNumberSpillEnabled));

  addSessionProperty(
      kTopNRowNumberSpillEnabled,
      "Native Execution only. Enable topN row number spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kTopNRowNumberSpillEnabled,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kTopNRowNumberSpillEnabled));

  addSessionProperty(
      kValidateOutputFromOperators,
      "If set to true, then during execution of tasks, the output vectors of "
      "every operator are validated for consistency. This is an expensive check "
      "so should only be used for debugging. It can help debug issues where "
      "malformed vector cause failures or crashes by helping identify which "
      "operator is generating them.",
      BOOLEAN(),
      false,
      QueryConfig::kValidateOutputFromOperators,
      baseVeloxQueryConfig->getDefaultValue(QueryConfig::kValidateOutputFromOperators));

  addSessionProperty(
      QueryConfig::kAdjustTimestampToTimezone,
      "Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: "
      "this will be removed",
      BOOLEAN(),
      false,
      QueryConfig::kAdjustTimestampToTimezone,
      std::to_string(true)); // Overrides velox default value.

  addSessionProperty(
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      "Native Execution only. The cpu time slice limit in ms that a driver thread. "
      "If not zero, can continuously run without yielding. If it is zero, then "
      "there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      std::to_string(1000)); // Overrides velox default value.
}

const std::unordered_map<std::string, std::shared_ptr<SessionProperty>>&
SystemSessionProperties::getSessionProperties() const {
  return sessionProperties_;
}

std::shared_ptr<SessionProperty> SystemSessionProperties::getSessionProperty(
    const std::string& key) const {
  VELOX_USER_CHECK(
      (sessionProperties_.find(key) != sessionProperties_.end()),
      "Session property {} cannot be found",
      key);
  auto it = sessionProperties_.find(key);
  return it->second;
}

void SystemSessionProperties::updateDefaultValues(
    std::unordered_map<std::string, std::string>& configStrings) const {
  for (auto& property : sessionProperties_) {
    if (!property.second->getDefaultValue().empty()) {
      configStrings[property.second->getVeloxConfigName()] =
          property.second->getDefaultValue();
    }
  }
}

void SystemSessionProperties::updateSessionValues(
    std::unordered_map<std::string, std::string>& configStrings,
    const std::map<std::string, std::string>& systemProperties) const {
  for (const auto& it : systemProperties) {
    configStrings[toVeloxConfig(it.first)] = it.second;
  }
}

std::string SystemSessionProperties::toVeloxConfig(
    const std::string& name) const {
  auto it = sessionProperties_.find(name);
  return it == sessionProperties_.end() ? name
                                        : it->second->getVeloxConfigName();
}

json SessionPropertyReporter::getJsonMetaDataSessionProperty(
    const std::string& name) {
  const auto sessionProperty =
      systemSessionProperties_.getSessionProperty(name);
  json sessionPropertyJson;
  sessionPropertyJson["name"] = sessionProperty->getName();
  sessionPropertyJson["description"] = sessionProperty->getDescription();
  sessionPropertyJson["typeSignature"] = sessionProperty->getType();
  sessionPropertyJson["defaultValue"] = sessionProperty->getDefaultValue();
  sessionPropertyJson["hidden"] = sessionProperty->isHidden();
  return sessionPropertyJson;
}

json SessionPropertyReporter::getJsonMetaDataSessionProperties() {
  json j = json::array();
  std::unordered_map<std::string, std::shared_ptr<SessionProperty>>
      sessionProperties = systemSessionProperties_.getSessionProperties();
  for (const auto& sessionProperty : sessionProperties) {
    json sessionPropertyJson;
    sessionPropertyJson["name"] = sessionProperty.second->getName();
    sessionPropertyJson["description"] =
        sessionProperty.second->getDescription();
    sessionPropertyJson["typeSignature"] = sessionProperty.second->getType();
    sessionPropertyJson["defaultValue"] =
        sessionProperty.second->getDefaultValue();
    sessionPropertyJson["hidden"] = sessionProperty.second->isHidden();
    j.push_back(sessionPropertyJson);
  }
  return j;
}

} // namespace facebook::presto
