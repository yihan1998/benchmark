//
//  memcached_db.h
//  YCSB-C
//

#ifndef YCSB_C_MEMCACHED_DB_H_
#define YCSB_C_MEMCACHED_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

#include <libmemcached/memcached.h>

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

class MemcachedDB : public DB {
public:
    MemcachedDB() {
        mthread_mutex_init(&mutex_, NULL);

        memcached_return rc;        
        memserver_ = NULL;

        memc_ = memcached_create(NULL);
        memserver_ = memcached_server_list_append(memserver_, "localhost", 11211, &rc);
        
        rc = memcached_server_push(memc_, memserver_);
        if (rc == MEMCACHED_SUCCESS)
            fprintf(stderr, "Added server successfully\n");
        else
            fprintf(stderr, "Couldn't add server: %s\n", memcached_strerror(memc_, rc));
    }

    ~MemcachedDB() {
        memcached_free(memc_);
    }

    void Init(int keylength, int valuelength) {
        // std::lock_guard<std::mutex> lock(mutex_);
        mthread_mutex_lock(&mutex_);
        keylength_ = keylength;
        valuelength_ = valuelength;
        mthread_mutex_unlock(&mutex_);
    }

    int Read(const std::string &table, const std::string &key, std::string &value) {
        // std::lock_guard<std::mutex> lock(mutex_);
        mthread_mutex_lock(&mutex_);

        memcached_return rc;
        uint32_t flags;
        
        const char * read_key = CopyString(key);
        
        size_t value_length;

        char * ret = memcached_get(memc_, read_key, strlen(read_key), &value_length, &flags, &rc);
        // fprintf(stdout, "  read value len: %d, %s\n", value_length, ret);

        if (rc == MEMCACHED_SUCCESS) {
            value = ret;
            // std::cout << "READ " << table << ' ' << key << " [ " << value << ']' << std::endl;
            /*Be sure to free the data returned by memcached*/
            free(ret);
        }

        // DeleteString(read_key);

        mthread_mutex_unlock(&mutex_);
        return (rc == MEMCACHED_SUCCESS)? DB::kOK : DB::kErrorNoData;
    }

    int Scan(const std::string &table, const std::string &key,
            int record_count, std::vector<std::vector<KVPair>> &records) {
        // std::lock_guard<std::mutex> lock(mutex_);
        // cout << "SCAN " << table << ' ' << key << " " << record_count << endl;
        return 0;
    }

    int Update(const std::string &table, const std::string &key,
                const std::string &value) {
        // std::lock_guard<std::mutex> lock(mutex_);
        mthread_mutex_lock(&mutex_);
        
        const char * update_key = CopyString(key);
        const char * update_value = CopyString(value);

        memcached_return_t rc = memcached_set(memc_, update_key, strlen(update_key), update_value, strlen(update_value), (time_t)0, (uint32_t)0);
        // if (rc == MEMCACHED_SUCCESS) {
        //     std::cout << "Update " << table << ' ' << key << " [ " << value << ']' << std::endl;
        // } else {
        //     fprintf(stderr,"Couldn't update key: %s\n", memcached_strerror(memc_, rc));
        // }
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr,"Couldn't update key: %s\n", memcached_strerror(memc_, rc));
        }
        
        // DeleteString(update_key);
        // DeleteString(update_value);

        mthread_mutex_unlock(&mutex_);
        return DB::kOK;
    }

    int Insert(const std::string &table, const std::string &key,
                const std::string &value) {
        // std::lock_guard<std::mutex> lock(mutex_);
        mthread_mutex_lock(&mutex_);

        const char * insert_key = CopyString(key);
        const char * insert_value = CopyString(value);

        memcached_return_t rc = memcached_set(memc_, insert_key, strlen(insert_key), insert_value, strlen(insert_value), (time_t)0, (uint32_t)0);
        // if (rc == MEMCACHED_SUCCESS) {
        //     std::cout << "INSERT " << table << ' ' << key << " [ " << value << ']' << std::endl;
        // } else {
        //     fprintf(stderr,"Couldn't insert key: %s\n", memcached_strerror(memc_, rc));
        // }
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr,"Couldn't insert key: %s\n", memcached_strerror(memc_, rc));
        }

        // DeleteString(insert_key);
        // DeleteString(insert_value);
            
        mthread_mutex_unlock(&mutex_);
        return DB::kOK;
    }

    int Delete(const std::string &table, const std::string &key) {
        const char * delete_key = CopyString(key);

        memcached_return_t rc = memcached_delete(memc_, delete_key, strlen(delete_key), (time_t)0);
        // if (rc == MEMCACHED_SUCCESS) {
        //     std::cout << "DELETE " << table << ' ' << key << std::endl;
        // } else {
        //     fprintf(stderr,"Couldn't delete key: %s\n", memcached_strerror(memc_, rc));            
        // }
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr,"Couldn't delete key: %s\n", memcached_strerror(memc_, rc));
        }

        // DeleteString(delete_key);

        return (rc == MEMCACHED_SUCCESS)? DB::kOK : DB::kErrorNoData;
    }

private:
    // std::mutex mutex_;
    mthread_mutex_t mutex_;
    memcached_st * memc_;
    memcached_server_st * memserver_;
    int keylength_;
    int valuelength_;

protected:
    const char *CopyString(const std::string &str) {
        char *value = new char[str.length() + 1];
        strcpy(value, str.c_str());
        return value;
    }

    void DeleteString(const char *str) {
        delete[] str;
    }
};

} // ycsbc

#endif // YCSB_C_MEMCACHED_DB_H_