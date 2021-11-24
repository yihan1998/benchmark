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
  cout << "scan for " << record_count << " records" << endl;
  int index;
  int read = 0;
  do {
    redisReply *reply = (redisReply *)redisCommand(redis_.context(), "SCAN %d COUNT %d", index, record_count - read);
    // redisReply *reply = (redisReply *)redisCommand(redis_.context(), "SCAN %d", index);
    if (!reply) return DB::kOK;
    index = stoi(reply->element[0]->str);
    printf("index: %d\n",index);
    if(reply->elements == 1){
      printf("no data");
      return DB::kErrorNoData;
    }
    if (reply->element[1]->type != REDIS_REPLY_ARRAY) {
      printf("redis scan keys reply not array");
      freeReplyObject(reply);
      return DB::kErrorNoData;
    }
    read += reply->element[1]->elements;
    printf("elements: %d\n",reply->element[1]->elements);
    for (int i = 0; i < reply->element[1]->elements; i++) {
      printf("key: %s\n", reply->element[1]->element[i]->str);
    }
  } while (read < record_count);

  return DB::kOK;
}

} // namespace ycsbc
