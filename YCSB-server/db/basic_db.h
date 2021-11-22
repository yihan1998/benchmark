//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

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
using std::cout;
using std::endl;

namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init(int keylength, int valuelength) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_init(&mutex_, NULL);
    cout << "A new thread begins working." << endl;
  }

  int Read(const std::string &table, const std::string &key, std::string &value) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_lock(&mutex_);
    cout << "READ " << table << ' ' << key << endl;
    // if (fields) {
    //   cout << " [ ";
    //   for (auto f : *fields) {
    //     cout << f << ' ';
    //   }
    //   cout << ']' << endl;
    // } else {
    //   cout  << " < all fields >" << endl;
    // }
    mthread_mutex_unlock(&mutex_);
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int record_count, std::vector<std::vector<KVPair>> &records) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_lock(&mutex_);
    cout << "SCAN " << table << ' ' << key << " " << record_count << endl;
    // if (fields) {
    //   cout << " [ ";
    //   for (auto f : *fields) {
    //     cout << f << ' ';
    //   }
    //   cout << ']' << endl;
    // } else {
    //   cout  << " < all fields >" << endl;
    // }
    mthread_mutex_unlock(&mutex_);
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             const std::string &value) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_lock(&mutex_);
    cout << "UPDATE " << table << ' ' << key << " [ " << value << ']' << endl;
    // for (auto v : values) {
    //   cout << v.first << '=' << v.second << ' ';
    // }
    // cout << ']' << endl;
    mthread_mutex_unlock(&mutex_);
    return 0;
  }

  int Insert(const std::string &table, const std::string &key,
             const std::string &value) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_lock(&mutex_);
    cout << "INSERT " << table << ' ' << key << " [ " << value << ']' << endl;
    // for (auto v : values) {
    //   cout << v.first << '=' << v.second << ' ';
    // }
    // cout << ']' << endl;
    mthread_mutex_unlock(&mutex_);
    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    // std::lock_guard<std::mutex> lock(mutex_);
    mthread_mutex_lock(&mutex_);
    cout << "DELETE " << table << ' ' << key << endl;
    mthread_mutex_unlock(&mutex_);
    return 0; 
  }

 private:
  // std::mutex mutex_;
  mthread_mutex_t mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

