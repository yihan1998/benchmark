//
//  redis_db.h
//  YCSB-C
//

#ifndef YCSB_C_REDIS_DB_H_
#define YCSB_C_REDIS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"
#include "redis/redis_client.h"
#include "redis/hiredis/hiredis.h"

using std::cout;
using std::endl;

namespace ycsbc {

class RedisDB : public DB {
 public:
  RedisDB(const char *host, int port, int slaves) :
      redis_(host, port, slaves) {
  }

  int Read(const std::string &table, const std::string &key, std::string &value);

  int Scan(const std::string &table, const std::string &key,
           int record_count, std::vector<std::vector<KVPair>> &records);

  int Update(const std::string &table, const std::string &key,
             const std::string &value);

  int Insert(const std::string &table, const std::string &key,
             const std::string &value) {
    return Update(table, key, value);
  }

  int Delete(const std::string &table, const std::string &key) {
    std::string cmd("DEL " + key);
    redis_.Command(cmd);
    return DB::kOK;
  }

 private:
  RedisClient redis_;
};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_

