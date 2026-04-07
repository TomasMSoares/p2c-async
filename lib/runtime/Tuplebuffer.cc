
#include "IR/Defs.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include "runtime/tuplebuffer.h"

#include <llvm/ADT/Twine.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>

using namespace p2cllvm;

namespace p2cllvm {
char *Buffer::insertUnchecked(size_t elemSize) {
  char *elem = mem + ptr;
  ptr += elemSize;
  return elem;
}
TypeRef<llvm::StructType> Buffer::createType(llvm::LLVMContext &context) {
  return getOrCreateType(context, "Buffer", [&]() {
    return llvm::StructType::get(context, {BigIntTy::createType(context),
                                           BigIntTy::createType(context),
                                           PointerTy::createType(context)});
  });
}
} // namespace p2cllvm
