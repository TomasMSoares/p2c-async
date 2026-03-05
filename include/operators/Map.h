#pragma once
#include "IR/Builder.h"
#include "IR/Expression.h"
#include "internal/basetypes.h"
#include "operators/Iu.h"
#include "operators/Operator.h"
#include <memory>
#include <string_view>

namespace p2cllvm {
class Map : public Operator {
public:
  Map(std::unique_ptr<Operator> &&parent, std::unique_ptr<Exp> &&map,
      std::string_view name, TypeEnum type)
      : iu(name, type), parent(std::move(parent)), map(std::move(map)){}
  void produce(IUSet &required, Builder &builder, ConsumerFn consumer, InitFn fn) override {
      IUSet iuset = (required | map->getIUs()) - IUSet{{&iu}};

    parent->produce(iuset, builder, [&](Builder &builder) {
      ValueRef<> val = builder.createExpEval(map);
      val = iu.type.createCast(val, iu.type.typeEnum, iu.type.createType(builder.getContext()), builder);
      builder.getCurrentScope().updateValue(&iu, val);
      if(iu.type.typeEnum == TypeEnum::String)
        builder.getCurrentScope().updatePtr(&iu, val);
      consumer(builder);
    }, fn);
  }

  IUSet availableIUs() override {
    IUSet iuset;
    iuset.add(&iu);
    return iuset | parent->availableIUs();
  }

  IU *getIU(std::string_view name) {
    if (iu.name == name) {
      return &iu;
    }
    return nullptr;
  }

  ~Map() override = default;

private:
  IU iu;
  std::unique_ptr<Operator> parent;
  std::unique_ptr<Exp> map;
};
} // namespace p2cllvm
