#include <iostream>
#include <string>
#include <vector>
#include "redis/redis_client.h"
#include "db/redis_db.h"

using namespace std;
using namespace ycsbc;

int main(int argc, const char *argv[]) {
  const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
  int port = (argc > 2) ? atoi(argv[2]) : 6379;

  RedisClient client(host, port, 0);

  client.Command("HMSET Ren field1 jinglei@ren.systems field2 Jinglei");

  RedisDB db(host, port, false);
  db.Init();
  string key = "Ren";
  vector<DB::KVPair> result;

  std::string value;
  int ret;
  ret = db.Read(key, key, value);
  cout << value << endl;

  string new_value = "HelloWorld!";
  db.Update(key, key, new_value);

  result.clear();
  ret = db.Read(key, key, value);
  cout << value << endl;

  db.Delete(key, key);
  result.clear();
  ret = db.Read(key, key, value);
  cout << "After delete: " << result.size() << endl;
  return 0;
}
