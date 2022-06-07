#include "pch.h"
#include "structs.h"

namespace xivres {
	void to_json(nlohmann::json& j, const game_language& value) {
		switch (value) {
			case game_language::Japanese:
				j = "Japanese";
				break;
			case game_language::English:
				j = "English";
				break;
			case game_language::German:
				j = "German";
				break;
			case game_language::French:
				j = "French";
				break;
			case game_language::ChineseSimplified:
				j = "ChineseSimplified";
				break;
			case game_language::Korean:
				j = "Korean";
				break;
			case game_language::Unspecified:
			default:
				j = "Unspecified"; // fallback
		}
	}

	void from_json(const nlohmann::json& j, game_language& newValue) {
		std::string newValueString = j.get<std::string>();
		for (auto& c : newValueString)
			if (c < 128)
				c = std::tolower(c);

		newValue = game_language::Unspecified;
		if (newValueString.empty())
			return;

		if (newValueString.substr(0, std::min<size_t>(8, newValueString.size())) == "japanese")
			newValue = game_language::Japanese;
		else if (newValueString.substr(0, std::min<size_t>(7, newValueString.size())) == "english")
			newValue = game_language::English;
		else if (newValueString.substr(0, std::min<size_t>(6, newValueString.size())) == "german")
			newValue = game_language::German;
		else if (newValueString.substr(0, std::min<size_t>(8, newValueString.size())) == "deutsche")
			newValue = game_language::German;
		else if (newValueString.substr(0, std::min<size_t>(6, newValueString.size())) == "french")
			newValue = game_language::French;
		else if (newValueString.substr(0, std::min<size_t>(17, newValueString.size())) == "chinesesimplified")
			newValue = game_language::ChineseSimplified;
		else if (newValueString.substr(0, std::min<size_t>(6, newValueString.size())) == "korean")
			newValue = game_language::Korean;
	}
}

void structs::to_json(nlohmann::json& j, const PluralColumns& o) {
	j = nlohmann::json::array({
		o.singularColumnIndex,
		o.pluralColumnIndex,
		o.capitalizedColumnIndex,
		o.languageSpecificColumnIndex,
	});
	for (auto& i : j) {
		if (i.get<uint32_t>() == PluralColumns::Index_NoColumn)
			i = nullptr;
	}
	while (!j.empty() && j.back().is_null())
		j.erase(j.size() - 1);
}

void structs::from_json(const nlohmann::json& j, PluralColumns& o) {
	if (!j.is_array())
		throw std::invalid_argument("PluralColumns must be an array");
	if (j.empty())
		return;

	o.singularColumnIndex = j.at(0).is_null() ? PluralColumns::Index_NoColumn : j.at(0).get<uint32_t>();
	if (j.size() >= 2)
		o.pluralColumnIndex = j.at(1).is_null() ? PluralColumns::Index_NoColumn : j.at(1).get<uint32_t>();
	if (j.size() >= 3)
		o.capitalizedColumnIndex = j.at(2).is_null() ? PluralColumns::Index_NoColumn : j.at(2).get<uint32_t>();
	if (j.size() >= 4)
		o.languageSpecificColumnIndex = j.at(3).is_null() ? PluralColumns::Index_NoColumn : j.at(3).get<uint32_t>();
}

void structs::to_json(nlohmann::json& j, const TargetGroup& o) {
	j = o.columnIndices;
}

void structs::from_json(const nlohmann::json& j, TargetGroup& o) {
	o.columnIndices = j.get<decltype(o.columnIndices)>();
}

void structs::to_json(nlohmann::json& j, const ReplacementTemplate& o) {
	j = nlohmann::json::object({
		{"from", o.from},
		{"to", o.to},
		{"icase", o.icase},
	});
}

void structs::from_json(const nlohmann::json& j, ReplacementTemplate& o) {
	o.from = j.at("from").get<std::string>();
	o.to = j.at("to").get<std::string>();
	o.icase = j.value("icase", true);
}

void structs::to_json(nlohmann::json& j, const IgnoredCell& o) {
	j = nlohmann::json::object({
		{"name", o.name},
		{"id", o.id},
		{"column", o.column},
	});
	if (o.forceLanguage.has_value())
		j["forceLanguage"] = o.forceLanguage.value();
	if (o.forceString.has_value())
		j["forceString"] = o.forceString.value();
}

void structs::from_json(const nlohmann::json& j, IgnoredCell& o) {
	o.name = j.at("name").get<std::string>();
	o.id = j.at("id").get<int>();
	o.column = j.at("column").get<int>();

	if (const auto it = j.find("forceLanguage"); it == j.end())
		o.forceLanguage = std::nullopt;
	else
		o.forceLanguage = it->get<xivres::game_language>();

	if (const auto it = j.find("forceString"); it == j.end())
		o.forceString = std::nullopt;
	else
		o.forceString = it->get<std::string>();
}

void structs::to_json(nlohmann::json& j, const Rule& o) {
	j = nlohmann::json::object({
		{"targetGroups", o.targetGroups},
		{"stringPattern", o.stringPattern},
		{"replaceTo", o.replaceTo},
		{"skipIfAllSame", o.skipIfAllSame},
		{"preprocessReplacements", o.preprocessReplacements},
		{"postprocessReplacements", o.postprocessReplacements},
	});
}

void structs::from_json(const nlohmann::json& j, Rule& o) {
	o.targetGroups = j.at("targetGroups").get<decltype(o.targetGroups)>();
	o.stringPattern = j.value("stringPattern", decltype(o.stringPattern)());
	o.replaceTo = j.at("replaceTo").get<decltype(o.replaceTo)>();
	o.skipIfAllSame = j.value("skipIfAllSame", true);
	if (const auto it = j.find("preprocessReplacements"); it != j.end()) {
		for (const auto& entry : it->items()) {
			auto lang = nlohmann::json(entry.key()).get<xivres::game_language>();
			o.preprocessReplacements.emplace(lang, entry.value().get<std::vector<std::string>>());
		}
	}
	o.postprocessReplacements = j.value("postprocessReplacements", decltype(o.postprocessReplacements)());
}

void structs::to_json(nlohmann::json& j, const Config& o) {
	j = nlohmann::json::object({
		{"name", o.name},
		{"description", o.description},
		{"targetLanguage", o.targetLanguage},
		{"sourceLanguages", o.sourceLanguages},
		{"columnMap", o.columnMap},
		{"pluralMap", o.pluralMap},
		{"targetGroups", o.targetGroups},
		{"replacementTemplates", o.replacementTemplates},
		{"ignoredCells", o.ignoredCells},
		{"rules", o.rules},
	});
	if (o.sourceLanguages.empty())
		throw std::invalid_argument("sourceLanguages cannot be empty");
	if (o.sourceLanguages.size() > 7)
		throw std::invalid_argument("Only up to 7 sourceLanguages are supported");
	if (std::ranges::find(o.sourceLanguages, xivres::game_language::Unspecified) != o.sourceLanguages.end())
		throw std::invalid_argument("Unspecified language in sourceLanguages is not supported");
}

void structs::from_json(const nlohmann::json& j, Config& o) {
	o.name = j.at("name").get<decltype(o.name)>();
	o.description = j.value("description", decltype(o.description)());
	o.targetLanguage = j.at("targetLanguage").get<decltype(o.targetLanguage)>();
	o.sourceLanguages = j.at("sourceLanguages").get<decltype(o.sourceLanguages)>();
	if (const auto it = j.find("columnMap"); it != j.end()) {
		o.columnMap.clear();
		for (const auto& pair : it->items()) {
			if (pair.key().starts_with("#"))
				continue;

			std::map<xivres::game_language, std::vector<size_t>> colDef;
			for (const auto& pair2 : pair.value().items()) {
				auto lang = nlohmann::json(pair2.key()).get<xivres::game_language>();
				colDef.emplace(lang, pair2.value().get<std::vector<size_t>>());
			}

			o.columnMap.emplace_back(pair.key(), colDef);
		}
	}
	if (const auto it = j.find("pluralMap"); it != j.end()) {
		o.pluralMap.clear();
		for (const auto& pair : it->items()) {
			if (pair.key().starts_with("#"))
				continue;

			PluralColumns newItem{};
			from_json(pair.value(), newItem);
			o.pluralMap.emplace_back(pair.key(), newItem);
		}
	}
	if (const auto it = j.find("targetGroups"); it != j.end()) {
		o.targetGroups.clear();
		for (const auto& pair : it->items()) {
			if (pair.key().starts_with("#"))
				continue;

			TargetGroup newItem{};
			from_json(pair.value(), newItem);
			o.targetGroups.emplace(pair.key(), std::move(newItem));
		}
	}
	if (const auto it = j.find("replacementTemplates"); it != j.end()) {
		o.replacementTemplates.clear();
		for (const auto& pair : it->items()) {
			if (pair.key().starts_with("#"))
				continue;

			ReplacementTemplate newItem{};
			from_json(pair.value(), newItem);
			o.replacementTemplates.emplace(pair.key(), std::move(newItem));
		}
	}
	o.ignoredCells = j.value("ignoredCells", decltype(o.ignoredCells)());
	o.rules = j.at("rules").get<decltype(o.rules)>();
}
