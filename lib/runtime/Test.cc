#include "runtime/Test.h"

namespace p2cllvm {
bool operator==(const StringView &lhs, const std::string &rhs) {
  return std::string_view(lhs.data, lhs.length) == std::string_view(rhs);
}

bool skipTooLong(AssertLengthContext *ctx) { return ctx->len != maxLineItemAssertLen; }

} // namespace p2cllvm
