#include "pch.h"

#include <xivres/excel.h>
#include <xivres/excel.type2gen.h>
#include <xivres/installation.h>
#include <xivres/util.thread_pool.h>
#include <xivres/packed_stream.standard.h>

#include "structs.h"

class excel_transformer {
	xivres::installation m_source;
	std::map<std::string, int> m_sheets;

	std::vector<std::unique_ptr<xivres::installation>> m_additionalRoots;
	std::vector<xivres::game_language> m_fallbackLanguages;

	std::filesystem::path m_outputDir;
	int m_compressionLevel = Z_NO_COMPRESSION;  // Z_BEST_COMPRESSION

	struct ReplacementRule {
		srell::u8cregex SheetNameRegex;
		srell::u8cregex CellStrRegex;
		std::vector<xivres::game_language> SourceLang;
		std::string ReplaceTo;
		std::set<size_t> ColumnIndices;
		std::map<xivres::game_language, std::vector<std::string>> PreprocessActionNames;
		std::vector<std::string> PostprocessActionNames;
	};
	std::vector<std::pair<srell::u8cregex, std::map<xivres::game_language, std::vector<size_t>>>> columnMaps;
	std::vector<std::pair<srell::u8cregex, structs::PluralColumns>> m_pluralColumns;
	std::map<xivres::game_language, std::vector<ReplacementRule>> m_rowReplacementRules;
	std::map<xivres::game_language, std::set<structs::IgnoredCell>> m_ignoredCells;
	std::map<std::string, std::pair<srell::u8cregex, std::string>> m_actions;

public:
	excel_transformer(const std::filesystem::path& gamePath)
		: m_source(gamePath)
		, m_sheets(xivres::excel::exl::reader(m_source).name_to_id_map()) {

		m_fallbackLanguages.emplace_back(xivres::game_language::English);
		m_fallbackLanguages.emplace_back(xivres::game_language::Japanese);
		m_fallbackLanguages.emplace_back(xivres::game_language::French);
		m_fallbackLanguages.emplace_back(xivres::game_language::German);
		m_fallbackLanguages.emplace_back(xivres::game_language::ChineseSimplified);
		m_fallbackLanguages.emplace_back(xivres::game_language::Korean);
	}

	void add_additional_root(const std::filesystem::path& path) {
		m_additionalRoots.emplace_back(std::make_unique<xivres::installation>(path));
	}

	void add_transform_config(const std::filesystem::path& path) {
		const auto transformConfig = nlohmann::json::parse(std::ifstream(path)).get<structs::Config>();

		for (const auto& entry : transformConfig.columnMap)
			columnMaps.emplace_back(srell::u8cregex(entry.first, srell::regex_constants::ECMAScript | srell::regex_constants::icase), entry.second);

		for (const auto& entry : transformConfig.pluralMap)
			m_pluralColumns.emplace_back(srell::u8cregex(entry.first, srell::regex_constants::ECMAScript | srell::regex_constants::icase), entry.second);

		for (const auto& entry : transformConfig.replacementTemplates) {
			m_actions.emplace(entry.first, std::make_pair(
				srell::u8cregex(entry.second.from, srell::regex_constants::ECMAScript | (entry.second.icase ? srell::regex_constants::icase : srell::regex_constants::syntax_option_type())),
				entry.second.to));
		}

		m_ignoredCells[transformConfig.targetLanguage].insert(transformConfig.ignoredCells.begin(), transformConfig.ignoredCells.end());
		for (const auto& rule : transformConfig.rules) {
			for (const auto& targetGroupName : rule.targetGroups) {
				for (const auto& target : transformConfig.targetGroups.at(targetGroupName).columnIndices) {
					m_rowReplacementRules[transformConfig.targetLanguage].emplace_back(ReplacementRule{
						srell::u8cregex(target.first, srell::regex_constants::ECMAScript | srell::regex_constants::icase),
						srell::u8cregex(rule.stringPattern, srell::regex_constants::ECMAScript | srell::regex_constants::icase),
						transformConfig.sourceLanguages,
						rule.replaceTo,
						{target.second.begin(), target.second.end()},
						rule.preprocessReplacements,
						rule.postprocessReplacements,
						});
				}
			}
		}
	}

	void set_output_directory(std::filesystem::path path) {
		m_outputDir = std::move(path);
	}

	void set_compression_level(int level) {
		m_compressionLevel = level;
	}

	void work() {
		create_directories(m_outputDir);

		{
			std::ofstream ttmpl(m_outputDir / "TTMPL.mpl.tmp", std::ios::binary);
			std::ofstream ttmpd(m_outputDir / "TTMPD.mpd.tmp", std::ios::binary);
			std::mutex writeMtx;

			xivres::util::thread_pool pool;
			try {
				for (const auto& exhName : m_sheets | std::views::keys) {
					pool.Submit([this, exhName, &ttmpl, &ttmpd, &writeMtx]() {
						sheet_worker(*this, exhName, ttmpl, ttmpd, writeMtx).work();
					});
				}
				pool.SubmitDoneAndWait();

			} catch (const std::exception&) {
				pool.SubmitDoneAndWait();
			}
		}

		if (const auto p = m_outputDir / "TTMPL.mpl"; exists(p))
			std::filesystem::remove(p);
		if (const auto p = m_outputDir / "TTMPD.mpd"; exists(p))
			std::filesystem::remove(p);
		std::filesystem::rename(m_outputDir / "TTMPL.mpl.tmp", m_outputDir / "TTMPL.mpl");
		std::filesystem::rename(m_outputDir / "TTMPD.mpd.tmp", m_outputDir / "TTMPD.mpd");
	}

private:
	class sheet_worker {
		const excel_transformer& m_owner;
		const std::string m_exhName;
		std::ofstream& m_ttmpl;
		std::ofstream& m_ttmpd;
		std::mutex& m_writeMtx;

		const char* m_lastStep = "waiting";
		std::unique_ptr<xivres::excel::type2gen> m_sheet;

	public:
		sheet_worker(const excel_transformer& owner, std::string exhName, std::ofstream& ttmpl, std::ofstream& ttmpd, std::mutex& writeLock)
			: m_owner(owner)
			, m_exhName(std::move(exhName))
			, m_ttmpl(ttmpl)
			, m_ttmpd(ttmpd)
			, m_writeMtx(writeLock) {
		}

		void work() {
			m_lastStep = "Load source EXH/D files";
			if (!load_source())
				return;

			m_lastStep = "Setup";
			setup_from_source();

			m_lastStep = "Load external EXH/D files";
			if (m_exhName == "CustomTalk")
				merge_custom_talk();
			else if (m_exhName == "CompleteJournal")
				merge_complete_journal();
			else
				merge_default();

			for (auto& [id, rowSet] : m_sheet->Data) {
				m_lastStep = "Find which language to use while filling current row if missing in other languages";
				const auto referenceRowLanguage = get_reference_language(rowSet);
				if (referenceRowLanguage == xivres::game_language::Unspecified)
					continue;
				std::vector<xivres::excel::cell> referenceRow = rowSet.at(referenceRowLanguage);

				m_lastStep = "Fill missing rows for languages that aren't from source, and restore columns if unmodifiable";
				std::set<size_t> referenceRowUsedColumnIndices;
				ensure_cell_filled(rowSet, referenceRowUsedColumnIndices, referenceRow, referenceRowLanguage);

				m_lastStep = "Adjust language data per use config";
				apply_transformation_rules(id, rowSet, referenceRowUsedColumnIndices);
			}

			m_lastStep = "Compile";
			export_to_ttmps();
		}

	private:
		structs::PluralColumns pluralColumnIndices{};
		std::map<xivres::game_language, std::vector<ReplacementRule>> exhRowReplacementRules;
		std::map<xivres::game_language, std::vector<size_t>> columnMap;

		void setup_from_source() {
			pluralColumnIndices = {};
			for (const auto& [pattern, data] : m_owner.m_pluralColumns) {
				if (srell::regex_search(m_exhName, pattern)) {
					pluralColumnIndices = data;
					break;
				}
			}

			exhRowReplacementRules.clear();
			for (const auto language : m_owner.m_fallbackLanguages)
				exhRowReplacementRules.emplace(language, std::vector<ReplacementRule>{});

			for (auto& [language, rules] : m_owner.m_rowReplacementRules) {
				auto& exhRules = exhRowReplacementRules.at(language);
				for (auto& rule : rules)
					if (srell::regex_search(m_exhName, rule.SheetNameRegex))
						exhRules.emplace_back(rule);
			}

			columnMap.clear();
			for (const auto& [pattern, data] : m_owner.columnMaps) {
				if (!srell::regex_search(m_exhName, pattern))
					continue;
				for (const auto& [language, data2] : data)
					columnMap[language] = data2;
			}
		}

		bool load_source() {
			const auto exhReaderSource = xivres::excel::exh::reader(m_owner.m_source, m_exhName);
			if (exhReaderSource.header().Variant != xivres::excel::variant::Level2)
				return false;

			if (std::ranges::find(exhReaderSource.get_languages(), xivres::game_language::Unspecified) != exhReaderSource.get_languages().end())
				return false;

			m_sheet = std::make_unique<xivres::excel::type2gen>(m_exhName, exhReaderSource.get_columns(), exhReaderSource.header().ReadStrategy);
			m_sheet->FillMissingLanguageFrom = m_owner.m_fallbackLanguages;
			m_sheet->add_language(xivres::game_language::Japanese);
			m_sheet->add_language(xivres::game_language::English);
			m_sheet->add_language(xivres::game_language::German);
			m_sheet->add_language(xivres::game_language::French);
			m_sheet->add_language(xivres::game_language::ChineseSimplified);
			m_sheet->add_language(xivres::game_language::Korean);

			for (const auto language : exhReaderSource.get_languages()) {
				for (const auto& page : exhReaderSource.get_pages()) {
					const auto exdPathSpec = exhReaderSource.get_exd_path(page, language);
					try {
						const auto exdReader = xivres::excel::exd::reader(exhReaderSource, m_owner.m_source.get_file(exdPathSpec));
						m_sheet->add_language(language);
						for (const auto i : exdReader.get_row_ids())
							m_sheet->set_row(i, language, exdReader[i][0].get_cells_vector());
					} catch (const std::out_of_range&) {
						// pass
					} catch (const std::exception& e) {
						throw std::runtime_error(std::format("Error occurred while processing {}: {}", exdPathSpec, e.what()));
					}
				}
			}

			return true;
		}

		size_t translateColumnIndex(xivres::game_language fromLanguage, xivres::game_language toLanguage, size_t fromIndex) {
			const auto fromIter = columnMap.find(fromLanguage);
			const auto toIter = columnMap.find(toLanguage);
			if (fromIter == columnMap.end() || toIter == columnMap.end())
				return fromIndex;

			const auto it = std::ranges::lower_bound(fromIter->second, fromIndex);
			if (it == fromIter->second.end() || *it != fromIndex)
				return fromIndex;
			return toIter->second.at(it - fromIter->second.begin());
		}

		void merge_custom_talk() {
			// discard CustomTalk from external EXH/D files, or it will effectively disable the unending journey
		}

		void merge_complete_journal() {
			// Row ID does not persist across versions. Use first 4 columns as the alternate key.

			const auto ToMapKey = [](const std::vector<xivres::excel::cell>& columns) {
				// 20, 20, 16, 8
				return 0
					| (columns[0].uint64 << 44)
					| (columns[1].uint64 << 24)
					| (columns[2].uint64 << 8)
					| (columns[3].uint64 << 0);
			};

			do {
				auto cannotProceed = false;
				for (size_t i = 0; !cannotProceed && i < 4; i++) {
					if (!m_sheet->Columns[i].is_integer()) {
						std::cout << std::format("[{}] Skipping because column #{} of target file is not an integer, but is of type code {}.", m_exhName, i, static_cast<int>(*m_sheet->Columns[i].Type)) << std::endl;
						cannotProceed = true;
					}
				}
				if (cannotProceed)
					break;
				if (!m_sheet->Columns[5].is_string()) {
					std::cout << std::format("[{}] Skipping because column #5 of target file is not a string, but is of type code {}.", m_exhName, static_cast<int>(*m_sheet->Columns[5].Type)) << std::endl;
					break;
				}

				std::map<uint64_t, uint32_t> questTitleIdMap;
				for (const auto& [rowId, rowSet] : m_sheet->Data) {
					for (const auto& row : rowSet | std::views::values) {
						if (row[5].String.empty())
							continue;

						questTitleIdMap[ToMapKey(row)] = rowId;
						break; // intentional; first 4 columns should be same for all languages across regions.
					}
				}

				for (const auto& reader : m_owner.m_additionalRoots) {
					try {
						const auto exhReaderCurrent = xivres::excel::exh::reader(*reader, m_exhName);

						cannotProceed = false;
						for (size_t i = 0; !cannotProceed && i < 4; i++) {
							if (!exhReaderCurrent.get_column(i).is_integer()) {
								std::cout << std::format("[{}] Skipping because column #{} of source file is not an integer, but is of type code {}.", m_exhName, i, static_cast<int>(*m_sheet->Columns[i].Type)) << std::endl;
								cannotProceed = true;
							}
						}
						if (cannotProceed)
							continue;
						if (!exhReaderCurrent.get_column(5).is_string()) {
							std::cout << std::format("[{}] Skipping because column #5 of source file is not a string, but is of type code {}.", m_exhName, static_cast<int>(*m_sheet->Columns[5].Type)) << std::endl;
							break;
						}

						for (const auto language : exhReaderCurrent.get_languages()) {
							for (const auto& page : exhReaderCurrent.get_pages()) {
								const auto exdPathSpec = exhReaderCurrent.get_exd_path(page, language);
								try {
									std::cout << std::format("[{}] Adding {}", m_exhName, exdPathSpec) << std::endl;

									const auto exdReader = xivres::excel::exd::reader(exhReaderCurrent, reader->get_file(exdPathSpec));
									m_sheet->add_language(language);

									for (const auto i : exdReader.get_row_ids()) {
										auto addingRow = exdReader[i][0].get_cells_vector();
										if (addingRow[5].String.empty())
											continue;

										const auto targetRowIdIt = questTitleIdMap.find(ToMapKey(addingRow));
										if (targetRowIdIt == questTitleIdMap.end())
											continue;
										const auto targetRowId = targetRowIdIt->second;

										auto& rowSet = m_sheet->Data.at(targetRowId);
										std::vector<xivres::excel::cell>* pRow = nullptr;
										if (const auto it = rowSet.find(language); it != rowSet.end())
											pRow = &it->second;
										else {
											for (const auto& l : m_sheet->FillMissingLanguageFrom) {
												if (const auto it2 = rowSet.find(l); it2 != rowSet.end()) {
													pRow = &rowSet[language];
													*pRow = it2->second;
													break;
												}
											}
										}
										if (!pRow)
											continue;

										(*pRow)[5].String = std::move(addingRow[5].String);
									}
								} catch (const std::exception& e) {
									std::cout << std::format("[{}] Skipping {} because of error: {}", m_exhName, exdPathSpec, e.what()) << std::endl;
								}
							}
						}
					} catch (const std::out_of_range&) {
						// pass
					}
				}
			} while (false);
		}

		void merge_default() {
			for (const auto& reader : m_owner.m_additionalRoots) {
				try {
					const auto exhReaderCurrent = xivres::excel::exh::reader(*reader, m_exhName);
					for (const auto language : exhReaderCurrent.get_languages()) {
						for (const auto& page : exhReaderCurrent.get_pages()) {
							const auto exdPathSpec = exhReaderCurrent.get_exd_path(page, language);
							try {
								std::cout << std::format("[{}] Adding {}", m_exhName, exdPathSpec) << std::endl;

								const auto exdReader = xivres::excel::exd::reader(exhReaderCurrent, reader->get_file(exdPathSpec));
								m_sheet->add_language(language);
								for (const auto i : exdReader.get_row_ids()) {
									auto row = exdReader[i][0].get_cells_vector();
									const auto rowSetIt = m_sheet->Data.find(i);
									if (rowSetIt == m_sheet->Data.end())
										continue;

									const auto& rowSet = rowSetIt->second;
									auto referenceRowLanguage = xivres::game_language::Unspecified;
									const std::vector<xivres::excel::cell>* referenceRowPtr = nullptr;
									for (const auto& l : m_sheet->FillMissingLanguageFrom) {
										if (auto it = rowSet.find(l);
											it != rowSet.end()) {
											referenceRowLanguage = l;
											referenceRowPtr = &it->second;
											break;
										}
									}
									if (!referenceRowPtr)
										continue;
									const auto& referenceRow = *referenceRowPtr;

									auto prevRow{ std::move(row) };
									row = referenceRow;

									xivres::xivstring pluralBaseString;
									{
										constexpr auto N = structs::PluralColumns::Index_NoColumn;
										size_t cols[]{
											pluralColumnIndices.capitalizedColumnIndex == N ? N : translateColumnIndex(referenceRowLanguage, language, pluralColumnIndices.capitalizedColumnIndex),
											pluralColumnIndices.singularColumnIndex == N ? N : translateColumnIndex(referenceRowLanguage, language, pluralColumnIndices.singularColumnIndex),
											pluralColumnIndices.pluralColumnIndex == N ? N : translateColumnIndex(referenceRowLanguage, language, pluralColumnIndices.pluralColumnIndex),
											pluralColumnIndices.languageSpecificColumnIndex == N ? N : translateColumnIndex(referenceRowLanguage, language, pluralColumnIndices.languageSpecificColumnIndex),
										};
										for (auto& col : cols) {
											if (col == N || col >= prevRow.size() || prevRow[col].Type != xivres::excel::cell_type::String)
												col = N;
											else if (!prevRow[col].String.empty() && pluralBaseString.empty())
												pluralBaseString = prevRow[col].String;
										}
									}

									for (size_t j = 0; j < row.size(); ++j) {
										if (row[j].Type != xivres::excel::cell_type::String)
											continue;

										const auto otherColIndex = translateColumnIndex(referenceRowLanguage, language, j);
										if (otherColIndex >= prevRow.size()) {
											if (otherColIndex == j)
												continue;

											std::cout << std::format(
												"[{}] Skipping column: Column {} of language {} is was requested but there are {} columns",
												m_exhName, j, otherColIndex, static_cast<int>(language), prevRow.size()) << std::endl;
											continue;
										}

										if (prevRow[otherColIndex].Type != xivres::excel::cell_type::String) {
											std::cout << std::format(
												"[{}] Skipping column: Column {} of language {} is string but column {} of language {} is not a string",
												m_exhName, j, static_cast<int>(referenceRowLanguage), otherColIndex, static_cast<int>(language)) << std::endl;
											continue;
										}

										if (prevRow[otherColIndex].String.empty()) {
											if (pluralBaseString.empty())
												continue;

											if (j != pluralColumnIndices.singularColumnIndex
												&& j != pluralColumnIndices.pluralColumnIndex
												&& j != pluralColumnIndices.capitalizedColumnIndex
												&& j != pluralColumnIndices.languageSpecificColumnIndex) {
												continue;
											}

											row[j].String = pluralBaseString;
											continue;
										}

										if (prevRow[otherColIndex].String.escaped().starts_with("_rsv_"))
											continue;

										row[j].String = std::move(prevRow[otherColIndex].String);
									}
									m_sheet->set_row(i, language, std::move(row), false);
								}
							} catch (const std::exception& e) {
								std::cout << std::format(
									"[{}] Skipping {} because of error: {}", m_exhName, exdPathSpec, e.what()) << std::endl;
							}
						}
					}
				} catch (const std::out_of_range&) {
					// pass
				}
			}
		}

		xivres::game_language get_reference_language(const std::map<xivres::game_language, std::vector<xivres::excel::cell>>& rowSet) {
			for (const auto& l : m_sheet->FillMissingLanguageFrom) {
				if (auto it = rowSet.find(l); it != rowSet.end())
					return l;
			}
			return xivres::game_language::Unspecified;
		}

		void ensure_cell_filled(
			std::map<xivres::game_language, std::vector<xivres::excel::cell>>& rowSet,
			std::set<size_t>& refRowUsedCols,
			const std::vector<xivres::excel::cell>& refRow,
			xivres::game_language refRowLang
		) {
			for (const auto& language : m_sheet->Languages) {
				if (auto it = rowSet.find(language);
					it == rowSet.end())
					rowSet[language] = refRow;
				else {
					auto& row = it->second;

					// Pass 1. Fill missing columns if we have plural information
					{
						auto copyFromColumnIndex = pluralColumnIndices.capitalizedColumnIndex;
						if (copyFromColumnIndex == structs::PluralColumns::Index_NoColumn || copyFromColumnIndex >= row.size() || row[copyFromColumnIndex].Type != xivres::excel::cell_type::String || row[copyFromColumnIndex].String.empty())
							copyFromColumnIndex = pluralColumnIndices.singularColumnIndex;
						if (copyFromColumnIndex == structs::PluralColumns::Index_NoColumn || copyFromColumnIndex >= row.size() || row[copyFromColumnIndex].Type != xivres::excel::cell_type::String || row[copyFromColumnIndex].String.empty())
							copyFromColumnIndex = pluralColumnIndices.pluralColumnIndex;
						if (copyFromColumnIndex == structs::PluralColumns::Index_NoColumn || copyFromColumnIndex >= row.size() || row[copyFromColumnIndex].Type != xivres::excel::cell_type::String || row[copyFromColumnIndex].String.empty())
							copyFromColumnIndex = pluralColumnIndices.languageSpecificColumnIndex;

						if (copyFromColumnIndex != structs::PluralColumns::Index_NoColumn) {
							size_t targetColumnIndices[]{
								pluralColumnIndices.capitalizedColumnIndex,
								pluralColumnIndices.singularColumnIndex,
								pluralColumnIndices.pluralColumnIndex,
								pluralColumnIndices.languageSpecificColumnIndex,
							};
							for (const auto targetColumnIndex : targetColumnIndices) {
								if (targetColumnIndex != structs::PluralColumns::Index_NoColumn
									&& targetColumnIndex < row.size()
									&& row[targetColumnIndex].Type == xivres::excel::cell_type::String
									&& row[targetColumnIndex].String.empty()) {
									row[targetColumnIndex].String = row[copyFromColumnIndex].String;
								}
							}
						}
					}

					// Pass 2. Fill missing columns from columns of reference language
					for (size_t i = 0; i < row.size(); ++i) {
						if (refRow[i].Type != xivres::excel::cell_type::String) {
							row[i] = refRow[i];
							refRowUsedCols.insert(i);
						} else if (row[i].String.empty()) {
							// apply only if made of incompatible languages

							int sourceLanguageType, referenceLanguageType;
							switch (language) {
								case xivres::game_language::Japanese:
								case xivres::game_language::English:
								case xivres::game_language::German:
								case xivres::game_language::French:
									sourceLanguageType = 1;
									break;
								case xivres::game_language::ChineseSimplified:
									sourceLanguageType = 2;
									break;
								case xivres::game_language::Korean:
									sourceLanguageType = 3;
									break;
								default:
									sourceLanguageType = 0;
							}

							switch (refRowLang) {
								case xivres::game_language::Japanese:
								case xivres::game_language::English:
								case xivres::game_language::German:
								case xivres::game_language::French:
									referenceLanguageType = 1;
									break;
								case xivres::game_language::ChineseSimplified:
									referenceLanguageType = 2;
									break;
								case xivres::game_language::Korean:
									referenceLanguageType = 3;
									break;
								default:
									referenceLanguageType = 0;
							}

							if (sourceLanguageType != referenceLanguageType) {
								row[i] = refRow[i];
								refRowUsedCols.insert(i);
							}
						}
					}
				}
			}
		}

		void apply_transformation_rules(
			uint32_t id,
			std::map<xivres::game_language, std::vector<xivres::excel::cell>>& rowSet,
			std::set<size_t>& refRowUsedCols
		) {
			std::map<xivres::game_language, std::vector<xivres::excel::cell>> pendingReplacements;
			for (const auto& language : m_sheet->Languages) {
				const auto& rules = exhRowReplacementRules.at(language);

				const std::set<structs::IgnoredCell>* currentIgnoredCells = nullptr;
				if (const auto it = m_owner.m_ignoredCells.find(language); it != m_owner.m_ignoredCells.end())
					currentIgnoredCells = &it->second;

				if (rules.empty() && !currentIgnoredCells)
					continue;

				auto row{ rowSet.at(language) };

				for (size_t columnIndex = 0; columnIndex < row.size(); columnIndex++) {
					if (row[columnIndex].Type != xivres::excel::cell_type::String)
						continue;

					if (refRowUsedCols.contains(columnIndex))
						continue;

					if (currentIgnoredCells) {
						if (const auto it = currentIgnoredCells->find(structs::IgnoredCell{ m_exhName, static_cast<int>(id), static_cast<int>(columnIndex), std::nullopt, std::nullopt });
							it != currentIgnoredCells->end()) {
							if (it->forceString) {
								row[columnIndex].String = xivres::xivstring(*it->forceString);
								continue;
							} else if (it->forceLanguage) {
								if (const auto it2 = rowSet.find(*it->forceLanguage);
									it2 != rowSet.end()) {
									std::cout << std::format(
										R"(Using "{}" in place of "{}" per rules, at {}({}, {}))",
										it2->second[columnIndex].String.repr(),
										row[columnIndex].String.repr(),
										m_exhName, id, columnIndex) << std::endl;
									row[columnIndex].String = it2->second[columnIndex].String;
									continue;
								}
							}
						}
					}

					for (const auto& rule : rules) {
						if (!rule.ColumnIndices.contains(columnIndex))
							continue;

						if (!regex_search(row[columnIndex].String.escaped(), rule.CellStrRegex))
							continue;

						std::vector p = { std::format("{}:{}", m_exhName, id) };
						for (const auto ruleSourceLanguage : rule.SourceLang) {
							if (const auto it = rowSet.find(ruleSourceLanguage);
								it != rowSet.end()) {

								if (row[columnIndex].Type != xivres::excel::cell_type::String)
									throw std::invalid_argument(std::format("Column {} of sourceLanguage {} in {} is not a string column", columnIndex, static_cast<int>(ruleSourceLanguage), m_exhName));

								auto readColumnIndex = columnIndex;
								bool normalizeToCapital = false;
								switch (ruleSourceLanguage) {
									case xivres::game_language::English:
									case xivres::game_language::German:
									case xivres::game_language::French:
										switch (language) {
											case xivres::game_language::Japanese:
											case xivres::game_language::ChineseSimplified:
											case xivres::game_language::Korean:
												normalizeToCapital = true;
										}
										break;

									case xivres::game_language::Japanese:
									case xivres::game_language::ChineseSimplified:
									case xivres::game_language::Korean:
									{
										normalizeToCapital = true;
										break;
									}
								}
								if (normalizeToCapital) {
									if (pluralColumnIndices.capitalizedColumnIndex != structs::PluralColumns::Index_NoColumn) {
										if (readColumnIndex == pluralColumnIndices.pluralColumnIndex
											|| readColumnIndex == pluralColumnIndices.singularColumnIndex)
											readColumnIndex = pluralColumnIndices.capitalizedColumnIndex;
									} else {
										if (readColumnIndex == pluralColumnIndices.pluralColumnIndex)
											readColumnIndex = pluralColumnIndices.singularColumnIndex;
									}
								}
								if (const auto actions = rule.PreprocessActionNames.find(ruleSourceLanguage); actions != rule.PreprocessActionNames.end()) {
									xivres::xivstring escaped(it->second[readColumnIndex].String);
									escaped.use_newline_payload(false);
									std::string replacing(escaped.parsed());
									for (const auto& ruleName : actions->second) {
										const auto& [replaceFrom, replaceTo] = m_owner.m_actions.at(ruleName);
										replacing = regex_replace(replacing, replaceFrom, replaceTo);
									}
									p.emplace_back(escaped.parsed(replacing).escaped());
								} else
									p.emplace_back(it->second[readColumnIndex].String.escaped());
							} else
								p.emplace_back();
						}
						while (p.size() < 16)
							p.emplace_back();

						auto allSame = true;
						size_t nonEmptySize = 0;
						size_t lastNonEmptyIndex = 1;
						for (size_t i = 1; i < p.size(); ++i) {
							if (!p[i].empty()) {
								if (p[i] != p[1])
									allSame = false;
								nonEmptySize++;
								lastNonEmptyIndex = i;
							}
						}
						std::string out;
						if (allSame)
							out = p[1];
						else if (nonEmptySize <= 1)
							out = p[lastNonEmptyIndex];
						else
							out = std::format(rule.ReplaceTo, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);

						xivres::xivstring escaped(out);
						escaped.use_newline_payload(false);
						if (!rule.PostprocessActionNames.empty()) {
							std::string replacing(escaped.parsed());
							for (const auto& ruleName : rule.PostprocessActionNames) {
								const auto& [replaceFrom, replaceTo] = m_owner.m_actions.at(ruleName);
								replacing = regex_replace(replacing, replaceFrom, replaceTo);
							}
							escaped.parsed(replacing);
						}
						row[columnIndex].String = std::move(escaped);
						break;
					}
				}
				pendingReplacements.emplace(language, std::move(row));
			}
			for (auto& [language, row] : pendingReplacements)
				m_sheet->set_row(id, language, std::move(row));
		}

		void export_to_ttmps() {
			for (auto& kv : m_sheet->compile()) {
				const auto& entryPathSpec = kv.first;
				auto& data = kv.second;

				const auto targetPath = m_owner.m_outputDir / entryPathSpec.path();

				m_lastStep = "Compress";
				std::vector<char> dataBytes;
				{
					const auto provider = std::make_unique<xivres::compressing_packed_stream<xivres::standard_compressing_packer>>(
						entryPathSpec,
						std::make_shared<xivres::memory_stream>(std::span<uint8_t>(reinterpret_cast<uint8_t*>(data.data()), data.size())),
						m_owner.m_compressionLevel);
					dataBytes = provider->read_vector<char>();
					std::vector<char>().swap(data);
				}

				m_lastStep = "Write to filesystem";
				const auto lock = std::lock_guard(m_writeMtx);
				const auto entryLine = std::format("{}\n", nlohmann::json::object({
					{"FullPath", entryPathSpec.path()},
					{"ModOffset", static_cast<uint64_t>(m_ttmpd.tellp())},
					{"ModSize", dataBytes.size()},
					{"DatFile", "0a0000"},
					}).dump());
				m_ttmpl.write(entryLine.data(), entryLine.size());
				m_ttmpd.write(dataBytes.data(), static_cast<std::streamsize>(dataBytes.size()));
			}
		}
	};
};

int main() {
	const auto game1path = std::filesystem::path(LR"(C:\Program Files (x86)\SquareEnix\FINAL FANTASY XIV - A Realm Reborn\game)");
	const auto game2path = std::filesystem::path(LR"(C:\Program Files (x86)\FINAL FANTASY XIV - KOREA\game)");
	const auto game3path = std::filesystem::path(LR"(C:\Program Files (x86)\SNDA\FFXIV\game)");

	excel_transformer m(game1path);
	m.add_additional_root(game2path);
	m.add_additional_root(game3path);
	m.add_transform_config(LR"(Z:\GitWorks\Soreepeong\XivAlexander\StaticData\ExcelTransformConfig\CrossRegionRemapping.json)");
	m.add_transform_config(LR"(Z:\GitWorks\Soreepeong\XivAlexander\StaticData\ExcelTransformConfig\Korean_SayQuestEnglish.json)");
	m.set_output_directory(LR"(Z:\XIV\JP\game\ttmp\0a0000)");
	m.set_compression_level(Z_NO_COMPRESSION);
	m.work();
	return 0;
}