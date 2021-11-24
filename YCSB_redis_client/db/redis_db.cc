//
//  redis_db.cc
//  YCSB-C
//

#include "redis_db.h"

#include <cstring>

using namespace std;

namespace ycsbc {

int RedisDB::Read(const std::string &table, const std::string &key, std::string &value) {
  redisReply *reply = (redisReply *)redisCommand(redis_.context(), "GET %s", key.c_str());
  if (!reply) return DB::kErrorNoData;
  value = string(reply->str);
  
  return DB::kOK;
}

int RedisDB::Update(const std::string &table, const std::string &key, const std::string &value) {
  redisReply *reply = (redisReply *)redisCommand(redis_.context(), "SET %s %s", key.c_str(), value.c_str());
  if (!reply) return DB::kOK;
  cout << reply->str << endl;
  assert(strcmp(reply->str,"OK") == 0);

  return DB::kOK;;
}

int RedisDB::Scan(const std::string &table, const std::string &key, int record_count, std::vector<std::vector<KVPair>> &records) {
  redisReply *reply = (redisReply *)redisCommand(redis_.context(), "SCAN %d COUNT %d", 0, record_count);
  if (!reply) return DB::kOK;
  int index = atoi(reply->element[0]->str);
  printf("index: %d",index);
  if(reply->elements == 1){
    printf("no data");
    return DB::kErrorNoData;
  }
  if (reply->element[1]->type != REDIS_REPLY_ARRAY) {
    printf("redis scan keys reply not array");
    freeReplyObject(reply);
    return DB::kErrorNoData;
  }
  for (int i = 0; i < reply->element[1]->elements; i++) {
    string key = reply->element[1]->element[i]->str;
    printf("i: %d,key: %d\n",i,key);
  }

  return DB::kOK;
}

} // namespace ycsbc
