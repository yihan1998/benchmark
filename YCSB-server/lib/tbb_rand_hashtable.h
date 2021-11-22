//
// tbb_rand_hashtable.h
//
// Created by Jinglei Ren on 12/22/14.
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_LIB_TBB_RAND_HASHTABLE_H_
#define YCSB_C_LIB_TBB_RAND_HASHTABLE_H_

#include "lib/string_hashtable.h"

#include <iostream>
#include <vector>
#include "tbb/concurrent_hash_map.h"
#include "tbb/queuing_rw_mutex.h"
#include "lib/string.h"

#ifndef __cplusplus
# include <stdatomic.h>
#else
# include <atomic>
# define _Atomic(X) std::atomic< X >
#endif

extern "C"
{
#include "cetus.h"
#include "cetus_api.h"

#include "mthread.h"
#include "mthread_mutex.h"
}

namespace vmp {

template<class V>
class TbbRandHashtable : public StringHashtable<V> {
 public:
  typedef typename StringHashtable<V>::KVPair KVPair;

  TbbRandHashtable() {
    mthread_mutex_init((mthread_mutex_t *)&mutex_, NULL);
  };

  V Get(const char *key) const; ///< Returns NULL if the key is not found
  bool Insert(const char *key, V value);
  V Update(const char *key, V value);
  V Remove(const char *key);
  std::vector<KVPair> Entries(const char *key = NULL, std::size_t n = -1) const;
  std::size_t Size() const { return table_.size(); }

 private:
  struct HashEqual {
    uint64_t hash(const String &hstr) const { return hstr.hash(); }
    bool equal(const String &a, const String &b) const { return a == b; }
  };

  typedef tbb::concurrent_hash_map<String, V, HashEqual> Hashtable;

  Hashtable table_;
  // mutable tbb::queuing_rw_mutex mutex_;
  mthread_mutex_t mutex_;
};

template<class V>
V TbbRandHashtable<V>::Get(const char *key) const {
  typename Hashtable::accessor result;
  // tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
  mthread_mutex_lock((mthread_mutex_t *)&mutex_);
  if (!table_.find(result, String::Wrap(key))) {
    mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
    return NULL;
  }
  mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
  return result->second;
}

template<class V>
bool TbbRandHashtable<V>::Insert(const char *key, V value) {
  if (!key) return false;
  String skey = String::Copy<MemAlloc>(key);
  // tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
  mthread_mutex_lock((mthread_mutex_t *)&mutex_);
  bool ret = table_.insert(std::make_pair(skey, value));
  mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
  return ret;
}

template<class V>
V TbbRandHashtable<V>::Update(const char *key, V value) {
  typename Hashtable::accessor result;
  V old(NULL);
  // tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
  mthread_mutex_lock((mthread_mutex_t *)&mutex_);
  if (table_.find(result, String::Wrap(key))) {
    old = result->second;
    result->second = value;
  }
  mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
  return old;
}

template<class V>
V TbbRandHashtable<V>::Remove(const char *key) {
  typename Hashtable::accessor result;
  V old(NULL);
  // tbb::queuing_rw_mutex::scoped_lock lock(mutex_, false);
  mthread_mutex_lock((mthread_mutex_t *)&mutex_);
  if (table_.find(result, String::Wrap(key))) {
    String::Free<MemAlloc>(result->first);
    old = result->second;
    table_.erase(result);
  }
  mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
  return old;
}

template<class V>
std::vector<typename TbbRandHashtable<V>::KVPair> TbbRandHashtable<V>::Entries(
    const char *key, std::size_t n) const {
  std::vector<KVPair> pairs;
  typename Hashtable::const_iterator pos;
  // tbb::queuing_rw_mutex::scoped_lock lock(mutex_);
  mthread_mutex_lock((mthread_mutex_t *)&mutex_);
  pos = key ? table_.equal_range(String::Wrap(key)).first : table_.begin();
  for (std::size_t i = 0; pos != table_.end() && i < n; ++pos, ++i) {
    pairs.push_back(std::make_pair(pos->first.value(), pos->second));
  }
  mthread_mutex_unlock((mthread_mutex_t *)&mutex_);
  return pairs;
}

} // vmp

#endif // YCSB_C_LIB_TBB_RAND_HASHTABLE_H_
