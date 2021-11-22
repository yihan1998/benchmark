//
//  redis_db.cc
//  YCSB-C
//

#include "redis_db.h"

#include <cstring>

using namespace std;

namespace ycsbc {

int RedisDB::Read(const std::string &table, const std::string &key, std::string &value) {
    redisReply * reply = (redisReply *)redisCommand(redis_.context(), "GET %s", key.c_str());
    if (!reply) {
		return DB::kOK;
	}
    // assert(reply->elements == 1);
	std::cout << " reply elements: " << reply->elements << std::endl;
    value = string(reply->element[0]->str);
    freeReplyObject(reply);
  	return DB::kOK;
}

int RedisDB::Update(const std::string &table, const std::string &key,
                     const std::string &value) {
	string cmd("SET");
	size_t len = cmd.length() + 1 + key.length() + 1 + value.length();
	cmd.reserve(len);

  	cmd.append(" ").append(key);
  	cmd.append(" ").append(value);
  	assert(cmd.length() == len);
  	redis_.Command(cmd);
  	return DB::kOK;
}

} // namespace ycsbc
