//
//  discrete_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DISCRETE_GENERATOR_H_
#define YCSB_C_DISCRETE_GENERATOR_H_

#include "generator.h"

#ifndef __cplusplus
# include <stdatomic.h>
#else
# include <atomic>
# define _Atomic(X) std::atomic< X >
#endif

#include <cassert>
#include <mutex>
#include <vector>
#include "utils.h"

extern "C"
{
#include "cetus.h"
#include "cetus_api.h"

#include "mthread.h"
#include "mthread_mutex.h"
}

namespace ycsbc {

template <typename Value>
class DiscreteGenerator : public Generator<Value> {
 public:
  DiscreteGenerator() : sum_(0) {
    mthread_mutex_init(&mutex_, NULL);
  }
  void AddValue(Value value, double weight);

  Value Next();
  Value Last() { return last_; }

 private:
  std::vector<std::pair<Value, double>> values_;
  double sum_;
  std::atomic<Value> last_;
  // std::mutex mutex_;
  mthread_mutex_t mutex_;
};

template <typename Value>
inline void DiscreteGenerator<Value>::AddValue(Value value, double weight) {
  if (values_.empty()) {
    last_.store(value);
  }
  values_.push_back(std::make_pair(value, weight));
  sum_ += weight;
}

template <typename Value>
inline Value DiscreteGenerator<Value>::Next() {
  // mutex_.lock();
  mthread_mutex_lock(&mutex_);
  double chooser = utils::RandomDouble();
  // mutex_.unlock();
  mthread_mutex_unlock(&mutex_);
  
  for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
    if (chooser < p->second / sum_) {
      last_.store(p->first);
      return last_.load();
    }
    chooser -= p->second / sum_;
  }
  
  assert(false);
  return last_.load();
}

} // ycsbc

#endif // YCSB_C_DISCRETE_GENERATOR_H_
