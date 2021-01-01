/*!
 *  Copyright (c) 2015 by Contributors
 * \file csv_parser.h
 * \brief iterator parser to parse csv format
 * \author Tianqi Chen
 */
#ifndef DMLC_DATA_CSV_PARSER_H_
#define DMLC_DATA_CSV_PARSER_H_

#include <dmlc/data.h>
#include <dmlc/strtonum.h>
#include <dmlc/parameter.h>
#include <cmath>
#include <cstring>
#include <map>
#include <string>
#include <limits>
#include "./row_block.h"
#include "./text_parser.h"

namespace dmlc {
namespace data {

struct CSVParserParam : public Parameter<CSVParserParam> {
  std::string format;
  std::string label_column;
  std::string delimiter;
  int weight_column;
  // declare parameters
  DMLC_DECLARE_PARAMETER(CSVParserParam) {
    DMLC_DECLARE_FIELD(format).set_default("csv")
        .describe("File format.");
    DMLC_DECLARE_FIELD(label_column).set_default("")
        .describe("List of column indices that represent labels.");
    DMLC_DECLARE_FIELD(delimiter).set_default(",")
        .describe("Delimiter used in the csv file.");
    DMLC_DECLARE_FIELD(weight_column).set_default(-1)
        .describe("Column index that will put into instance weights.");
  }

  static const char kLabelColumnListDelimiter = ',';
  std::map<int, int> label_column_indices;
  size_t label_count;
  void ExtractLabelColumnIndices() {
    if (label_column.length() > 0) {
      std::string element;
      std::stringstream list(label_column);
      int output_index = 0;
      while (std::getline(list, element, kLabelColumnListDelimiter)) {
        if (!element.empty()) {
          const char front = element.front();
          if (isdigit(front) || front == '-' || front == '+') {
            std::string::size_type next = 0;
            int input_index = std::stoi(element, &next);
            if (next == element.size()) {
              if (input_index >= 0) {
                const auto inserted = label_column_indices.insert({input_index, output_index});
                if (inserted.second) { // insert succeeded
                  output_index++;
                } else {
                  LOG(WARNING) << "Ignoring duplicate label_column index "
                               << input_index;
                }
              } else {
                LOG(WARNING) << "Ignoring negative label_column index "
                             << input_index;
              }
            } else {
              LOG(WARNING) << "Ignoring label_column list entry '"
                           << element << "' "
                           << "containing unexpected character '"
                           << element.at(next) << "'";
            }
          } else {
            LOG(WARNING) << "Ignoring non-numeric label_column list entry '"
                         << element << "'";
          }
        } else {
          LOG(WARNING) << "Ignoring missing label_column list entry";
        }
      }
    }
    label_count = (label_column_indices.size() > 1 ? label_column_indices.size() : 1);
    return;
  }
};

/*!
 * \brief CSVParser, parses a dense csv format.
 *  Currently is a dummy implementation, when label column is not specified.
 *  All columns are treated as real dense data.
 *  label will be assigned to 0.
 *
 *  This should be extended in future to accept arguments of column types.
 */
template <typename IndexType, typename DType = real_t>
class CSVParser : public TextParserBase<IndexType, DType> {
 public:
  explicit CSVParser(InputSplit *source,
                     const std::map<std::string, std::string>& args,
                     int nthread)
      : TextParserBase<IndexType, DType>(source, nthread) {
    param_.Init(args);
    param_.ExtractLabelColumnIndices();
    CHECK_EQ(param_.format, "csv");
    CHECK(param_.label_column_indices.find(param_.weight_column) ==
          param_.label_column_indices.end())
      << "Must have distinct columns for labels and instance weights";
  }

 protected:
  virtual void ParseBlock(const char *begin,
                          const char *end,
                          RowBlockContainer<IndexType, DType> *out);

 private:
  CSVParserParam param_;
};

template <typename IndexType, typename DType>
void CSVParser<IndexType, DType>::
ParseBlock(const char *begin,
           const char *end,
           RowBlockContainer<IndexType, DType> *out) {
  out->Clear();
  out->label_count = param_.label_count;
  std::vector<DType> label(param_.label_count);
  const char * lbegin = begin;
  const char * lend = lbegin;
  // advance lbegin if it points to newlines
  while ((lbegin != end) && (*lbegin == '\n' || *lbegin == '\r')) ++lbegin;
  while (lbegin != end) {
    // get line end
    this->IgnoreUTF8BOM(&lbegin, &end);
    lend = lbegin + 1;
    while (lend != end && *lend != '\n' && *lend != '\r') ++lend;

    const char* p = lbegin;
    int column_index = 0;
    IndexType idx = 0;
    std::fill(label.begin(), label.end(), DType(0.0f));
    real_t weight = std::numeric_limits<real_t>::quiet_NaN();

    while (p != lend) {
      char *endptr;
      DType v;
      // if DType is float32
      if (std::is_same<DType, real_t>::value) {
        v = strtof(p, &endptr);
      // If DType is int32
      } else if (std::is_same<DType, int32_t>::value) {
        v = static_cast<int32_t>(strtoll(p, &endptr, 0));
      // If DType is int64
      } else if (std::is_same<DType, int64_t>::value) {
        v = static_cast<int64_t>(strtoll(p, &endptr, 0));
      // If DType is all other types
      } else {
        LOG(FATAL) << "Only float32, int32, and int64 are supported for the time being";
      }

      const auto label_column_found = param_.label_column_indices.find(column_index);
      if (label_column_found != param_.label_column_indices.end()) {
        label[label_column_found->second] = v;
      } else if (std::is_same<DType, real_t>::value
                 && column_index == param_.weight_column) {
        weight = v;
      } else {
        if (std::distance(p, static_cast<char const*>(endptr)) != 0) {
          out->value.push_back(v);
          out->index.push_back(idx++);
        } else {
          idx++;
        }
      }
      p = (endptr >= lend) ? lend : endptr;
      ++column_index;
      while (*p != param_.delimiter[0] && p != lend) ++p;
      if (p == lend && idx == 0) {
        LOG(FATAL) << "Delimiter \'" << param_.delimiter << "\' is not found in the line. "
                   << "Expected \'" << param_.delimiter
                   << "\' as the delimiter to separate fields.";
      }
      if (p != lend) ++p;
    }
    // skip empty line
    while ((*lend == '\n' || *lend == '\r') && lend != end) ++lend;
    lbegin = lend;

    out->label.insert(out->label.cend(), label.cbegin(), label.cend());
    if (!std::isnan(weight)) {
      out->weight.push_back(weight);
    }
    out->offset.push_back(out->index.size());
  }
  CHECK_GT(out->label_count, 0);
  CHECK_EQ(out->label.size() % out->label_count, 0);
  CHECK_EQ((out->label.size() / out->label_count) + 1, out->offset.size());
  CHECK(out->weight.size() == 0 || out->weight.size() + 1 == out->offset.size());
}
}  // namespace data
}  // namespace dmlc
#endif  // DMLC_DATA_CSV_PARSER_H_
