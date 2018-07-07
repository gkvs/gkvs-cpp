/*
 *
 * Copyright 2018-present GKVS authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "driver.h"
#include "rocks_driver.h"


#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace gkvs {


    class RocksDriver final : public Driver {

    public:

        explicit RocksDriver(const std::string& name, const json &conf, const std::string& db_dir) : Driver(name) {

            Options options;
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            options.create_if_missing = true;
            options.create_missing_column_families= true;

            options.compression = kBZip2Compression;

            auto db_paths_it = conf.find("db_paths");
            if (db_paths_it != conf.end()) {

                json db_paths = *db_paths_it;

                auto path_it = db_paths.find("path");
                auto target_size_it = db_paths.find("target_size");

                if (path_it != db_paths.end() && target_size_it != db_paths.end()) {

                    std::string path = path_it->get<std::string>();
                    uint64_t target_size = target_size_it->get<uint64_t>();

                    options.db_paths.push_back(DbPath(path, target_size));
                }
            }

            auto db_log_dir_it = conf.find("db_log_dir");
            if (db_log_dir_it != conf.end()) {
                options.db_log_dir = db_log_dir_it->get<std::string>();
            }

            auto wal_dir_it = conf.find("wal_dir");
            if (wal_dir_it != conf.end()) {
                options.wal_dir = wal_dir_it->get<std::string>();
            }

            std::string testDb = "test";

            rocksdb::Status s = DB::Open(options, testDb.c_str(), &db_);

            if (!s.ok()) {
                throw std::runtime_error("fail to open db: test, " + s.ToString());
            }



        }

        ~RocksDriver() override {

            delete db_;

            std::cout << "Graceful shutdown rocks driver" << std::endl;

        }


        void get(const KeyOperation *request, ValueResult *response) override {

            do_get(request, response);

        }

        void multiGet(const BatchKeyOperation *request, BatchValueResult *response) override {

            do_multi_get(request, response);

        }

        void scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, writer);

        }

        void put(const PutOperation *request, StatusResult *response) override {

            do_put(request, response);

        }

        void remove(const KeyOperation *request, StatusResult *response) override {

            do_remove(request, response);

        }


    private:

        DB* db_;

    protected:

        void do_multi_get(const BatchKeyOperation *request, BatchValueResult *response);

        void do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer);

        void send_result(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer, Slice& key, Slice& value);

        void do_get(const KeyOperation *request, ValueResult *response);

        void do_put(const PutOperation *request, StatusResult *response);

        void do_remove(const KeyOperation *request, StatusResult *response);

        void metadata_result(const uint64_t* verOrNull, int ttl, Metadata *metadata) {

            if (verOrNull != nullptr) {
                set_version(verOrNull, metadata);
            }

            metadata->set_ttl(ttl);

        }

        void set_version(const uint64_t* ver, Metadata *meta) {

            meta->add_version(ver[0] & 0xFFFFFFFF);
            meta->add_version(ver[0] >> 32);

            meta->add_version(ver[1] & 0xFFFFFFFF);
            meta->add_version(ver[1] >> 32);

        }

        bool get_version(const PutOperation* request, uint64_t* rver) {

            if (request->version_size() == 4) {
                rver[0] = (((uint64_t)request->version(1)) << 32) | request->version(0);
                rver[1] = (((uint64_t)request->version(3)) << 32) | request->version(2);
                return true;
            }

            return false;
        }

        void key_result(const Slice& sliceKey, gkvs::ValueResult* result, const OutputOptions &out) {

            bool includeKey = include_key(out);

            if (includeKey) {
                result->mutable_key()->set_raw(sliceKey.data(), sliceKey.size());
            }

        }

        void key_result(const Key& operationKey, gkvs::ValueResult* result, const OutputOptions &out) {

            bool includeKey = include_key(out);

            if (includeKey) {
                result->mutable_key()->CopyFrom(operationKey);
            }

        }

        void value_result(const Slice& sliceValue, gkvs::Value *value, const OutputOptions &out) {

            if (include_value(out)) {
                value->set_raw(sliceValue.data(), sliceValue.size());
            }

        }

    };


    Driver* create_rocks_driver(const std::string &name, const json& conf, const std::string &db_dir) {
        return new RocksDriver(name, conf, db_dir);
    }

}


void gkvs::RocksDriver::do_scan(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer) {

    const std::string &tableName = request->tablename();

    if (tableName.empty()) {
        ValueResult result;
        result.mutable_header()->set_tag(request->header().tag());
        bad_request("empty table name", result.mutable_status());
        writer->WriteLast(result, grpc::WriteOptions());
        return;
    }

    Iterator* iterator = db_->NewIterator(ReadOptions());

    iterator->SeekToFirst();

    while(iterator->Valid()) {

        Slice key = iterator->key();
        Slice value = iterator->value();

        send_result(request, writer, key, value);

        iterator->Next();
    }

    delete iterator;

}

void gkvs::RocksDriver::send_result(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer, Slice& key, Slice& value) {

    ValueResult response;
    response.mutable_header()->set_tag(request->header().tag());

    response.mutable_metadata()->set_ttl(100);
    response.mutable_metadata()->add_version(1);

    metadata_result(nullptr, -1, response.mutable_metadata());
    key_result(key, &response, request->output());
    value_result(value, response.mutable_value(), request->output());
    success(response.mutable_status());

    writer->Write(response, grpc::WriteOptions());

}


void gkvs::RocksDriver::do_multi_get(const BatchKeyOperation *request, BatchValueResult *response) {

    std::vector<Slice> keys;
    int size = request->operation_size();

    for (int i = 0; i < size; ++i) {
        keys.push_back(request->operation(i).key().raw());
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses = db_->MultiGet(ReadOptions(), keys, &values);

    for (int i = 0; i < size; ++i) {

        ValueResult* result = response->add_result();

        rocksdb::Status& status = statuses[i];
        const KeyOperation& keyOperation = request->operation(i);
        std::string& value = values[i];

        result->mutable_header()->set_tag(keyOperation.header().tag());
        key_result(keyOperation.key(), result, keyOperation.output());

        if (status.ok()) {
            metadata_result(nullptr, -1, result->mutable_metadata());
            value_result(value, result->mutable_value(), keyOperation.output());
            success(result->mutable_status());
        }
        else if (status.IsNotFound()) {
            success(result->mutable_status());
        }
        else {
            driver_error(status.ToString().c_str(), result->mutable_status());
        }

    }

}

void gkvs::RocksDriver::do_get(const KeyOperation *request, ValueResult *response) {

    response->mutable_header()->set_tag(request->header().tag());
    key_result(request->key(), response, request->output());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();

    std::string value;

    rocksdb::Status status = db_->Get(ReadOptions(), key, &value);

    if (status.ok()) {
        metadata_result(nullptr, -1, response->mutable_metadata());
        value_result(value, response->mutable_value(), request->output());
        success(response->mutable_status());
    }
    else if (status.IsNotFound()) {
        success(response->mutable_status());
    }
    else {
        driver_error(status.ToString().c_str(), response->mutable_status());
    }

}

void gkvs::RocksDriver::do_put(const PutOperation *request, StatusResult *response) {

    response->mutable_header()->set_tag(request->header().tag());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();
    const std::string& value = request->value().raw();

    rocksdb::Status status = db_->Put(WriteOptions(), key, value);

    if (status.ok()) {

        success(response->mutable_status());

    }
    else {
        driver_error(status.ToString().c_str(), response->mutable_status());
    }

}

void gkvs::RocksDriver::do_remove(const KeyOperation *request, StatusResult *response) {

    response->mutable_header()->set_tag(request->header().tag());

    if (!request->has_key()) {
        bad_request("no key", response->mutable_status());
        return;
    }

    if (request->key().recordKey_case() != Key::RecordKeyCase::kRaw) {
        bad_request("key must be raw", response->mutable_status());
        return;
    }

    const std::string& tableName = request->key().tablename();

    if (tableName.empty()) {
        bad_request("empty table name", response->mutable_status());
        return;
    }

    const std::string& key = request->key().raw();

    rocksdb::Status status = db_->Delete(WriteOptions(), key);

    if (status.ok()) {

        success(response->mutable_status());

    }
    else {
        driver_error(status.ToString().c_str(), response->mutable_status());
    }

}