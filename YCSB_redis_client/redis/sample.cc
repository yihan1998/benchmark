#include <iostream>
#include <string>
#include <vector>
#include <random>
#include "redis/redis_client.h"
#include "db/redis_db.h"

using namespace std;
using namespace ycsbc;

typedef std::pair<std::string, std::string> KVPair;

int main(int argc, const char *argv[]) {
  const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
  int port = (argc > 2) ? atoi(argv[2]) : 6379;

  RedisClient client(host, port, 0);

  cout << " ***** Generating Keys *****" << endl;
  int start = 0;
  std::string keys[20];
  for (int i = 0; i < 20; i++) {
    std::string key("key");
    key.append(std::to_string(i));
    keys[i].assign(key);
    cout << keys[i] << endl;
  }

  RedisDB db(host, port, false);
  db.Init();
  vector<DB::KVPair> result;

  cout << " ***** Inserting Keys *****" << endl;
  for (int i = 0; i < 20; i++) {
    string value;
    value.append(16, 'a' + rand() % 26);
    cout << value << endl;
    db.Update(keys[i], keys[i], value);
  }

  cout << " ***** Reading Keys *****" << endl;
  for (int i = 0; i < 20; i++) {
    string value;
    db.Read(keys[i], keys[i], value);
    cout << value << endl;
  }

  result.clear();
  std::vector<std::vector<KVPair>> records;
  db.Scan(keys[0], keys[0], 5, records);

  return 0;
}
