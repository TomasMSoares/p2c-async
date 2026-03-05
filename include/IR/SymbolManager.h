#pragma once

#include <bit>
#include <cstdint>
#include <llvm/ADT/StringRef.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/Support/raw_ostream.h>
#include <string_view>
class SymbolManager {
    public:
        template<typename T>
        llvm::FunctionCallee getOrInsertFunction(llvm::Module* module, std::string_view name, T* cfn, llvm::FunctionType* type){
            auto fn = module->getOrInsertFunction(name, type);
            module->getFunction(name)->setLinkage(llvm::GlobalValue::ExternalLinkage);
            symbols[name] = std::bit_cast<uint64_t>(cfn);
            return fn;
        }

        template<typename T>
        llvm::FunctionCallee getOrInsertFunction(llvm::Module* module, std::string_view name, T* cfn, llvm::Type* ret, auto&& ...types){
            auto fn = module->getOrInsertFunction(name, ret, types...);
            module->getFunction(name)->setLinkage(llvm::GlobalValue::ExternalLinkage);
            // Don't do this at home
            symbols[name] = std::bit_cast<uint64_t>(cfn);
            return fn;
        }
        auto begin() { return symbols.begin(); }
        auto end() { return symbols.end(); }
    private:
        llvm::StringMap<uint64_t> symbols;

};
