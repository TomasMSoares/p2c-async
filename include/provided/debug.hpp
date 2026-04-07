#pragma once
#include <string>
#include <cassert>
#include <stdexcept>

#ifdef NDEBUG
#define ensure(expr, msg...) if (!getenv("notest") && !(expr)) throw std::logic_error("Error: " #expr " was not fulfilled (" __FILE__ ":" + std::to_string(__LINE__) + ")" + std::string(msg));
#else
#define ensure(expr, msg...) assert(getenv("notest") || (expr))
#endif

#define ensure_errno(expr, msg...) \
    do { \
        if (!getenv("notest") && !(expr)) { \
            auto err = errno; \
            throw std::runtime_error(std::string("Error: ") + strerror(err) + " (" __FILE__ ":" + std::to_string(__LINE__) + "): " + std::string(msg)); \
        } \
    } while (0)
