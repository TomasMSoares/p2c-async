#include <gtest/gtest.h>
#include "internal/basetypes.h"
#include "runtime/Runtime.h"
using namespace p2cllvm;

TEST(STRING, EQUAL){
    std::string s1 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";
    std::string s2 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";

    StringView sv1{s1.data(), s1.length()};
    StringView sv2{s2.data(), s2.length()};
    EXPECT_TRUE(string_eq(&sv1, &sv2));
}

TEST(STRING, NOT_EQUAL1){
    std::string s1 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";
    std::string s2 = "lar deposits. final packages cajole. regular waters are final requests. regular accounts are according to";

    StringView sv1{s1.data(), s1.length()};
    StringView sv2{s2.data(), s2.length()};
    EXPECT_FALSE(string_eq(&sv1, &sv2));
}

TEST(STRING, LIKE_TEST){
    std::string s1 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";
    std::string pattern = "lar deposits.%";

    StringView sv1{s1.data(), s1.length()};
    EXPECT_TRUE(like_prefix(&sv1, pattern.data(), pattern.length() - 1));
}

TEST(STRING, LIKE_TEST_MIDDLE){
    std::string s1 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";
    std::string pattern = "%final%";

    StringView sv1{s1.data(), s1.length()};
    EXPECT_TRUE(like(&sv1, pattern.data() + 1, pattern.length() - 2));
}

TEST(STRING, LIKE_TEST_BEGIN){
    std::string s1 = "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to";
    std::string pattern = "%to";

    StringView sv1{s1.data(), s1.length()};
    EXPECT_TRUE(like(&sv1, pattern.data() + 1, pattern.length() - 1));
}
