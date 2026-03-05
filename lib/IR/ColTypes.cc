#include "IR/ColTypes.h"

namespace p2cllvm {
llvm::Type *createColumnType(llvm::LLVMContext &context, TypeEnum type) {
  switch (type) {
  case TypeEnum::Integer:
    return p2c_col_gen<int32_t>::createType(context);
  case TypeEnum::Double:
    return p2c_col_gen<double>::createType(context);
  case TypeEnum::Char:
    return p2c_col_gen<char>::createType(context);
  case TypeEnum::String:
    return p2c_col_gen<StringView>::createType(context);
  case TypeEnum::BigInt:
    return p2c_col_gen<int64_t>::createType(context);
  case TypeEnum::Bool:
    return p2c_col_gen<bool>::createType(context);
  case TypeEnum::Date:
    return p2c_col_gen<Date>::createType(context);
  default:
    throw std::runtime_error("Unsupported type");
  }
}
} // namespace p2cllvm
