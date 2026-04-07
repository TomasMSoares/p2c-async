#include "runtime/Runtime.h"
#include "runtime/ThreadLocalContext.h"
#include "runtime/tuplebuffer.h"
#include <algorithm>
#include <functional>
#include <gtest/gtest.h>
#include <thread>
#include <unistd.h>

struct Elem {
  char c1, c2;
  bool operator<(const Elem &elem) const {
    if (c1 == elem.c1)
      return c2 < elem.c2;
    return c1 < elem.c1;
  }

  bool operator>(const Elem &elem) const {
    if (c1 == elem.c1)
      return c2 > elem.c2;
    return c1 > elem.c1;
  }
};
static int cmp(const void *v1, const void *v2) {
  auto *elem1 = reinterpret_cast<const Elem *>(v1);
  auto *elem2 = reinterpret_cast<const Elem *>(v2);
  return (*elem1 > *elem2) - (*elem1 < *elem2);
}

TEST(SORT_TEST, SORT) {

  ThreadLocalStorage<ThreadSortContext> tls;
  auto &lctx = *tls.getOrInsert(std::this_thread::get_id());
  p2cllvm::Buffer buffer(sysconf(_SC_PAGE_SIZE));
  auto *elem = reinterpret_cast<Elem *>(insertSortEntry(&lctx, sizeof(Elem)));
  elem->c1 = 'N';
  elem->c2 = 'F';
  elem = reinterpret_cast<Elem *>(insertSortEntry(&lctx, sizeof(Elem)));
  elem->c1 = 'A';
  elem->c2 = 'F';
  elem = reinterpret_cast<Elem *>(insertSortEntry(&lctx, sizeof(Elem)));
  elem->c1 = 'R';
  elem->c2 = 'F';
  elem = reinterpret_cast<Elem *>(insertSortEntry(&lctx, sizeof(Elem)));
  elem->c1 = 'N';
  elem->c2 = 'O';
  insertSortBuffer(&tls, &buffer, sizeof(Elem));
  sortBuffer(&buffer, sizeof(Elem), &cmp);
  auto *begin = reinterpret_cast<Elem *>(buffer.mem);
  auto *end = reinterpret_cast<Elem*>(buffer.mem + buffer.ptr);
  EXPECT_EQ(lctx.elems, 4);
  EXPECT_TRUE(std::is_sorted(begin, end, std::less<Elem>()));
}
