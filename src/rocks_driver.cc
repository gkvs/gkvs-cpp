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

#include <glog/logging.h>

#include <nlohmann/json.hpp>

using json = nlohmann::json;


namespace gkvs {

    static const int DEF_ROCKS_BUCKETS = 1023;

    class RocksTable final : public Table {

    public:

        explicit RocksTable(const std::string& table) :
                Table(table),
                ttl_(0)
        {}

        bool configure(const json& conf, std::string& error) {

            auto i = conf.find("ttl");

            if (i != conf.end()) {
                ttl_ = i->get<int>();
            }

            return true;

        }

        int get_ttl() const {
            return ttl_;
        }

    private:

        int ttl_;

    };

    class RocksDriver final : public Driver {

    public:

        explicit RocksDriver(const std::string& name, const std::string& db_dir)
                : Driver(name),
                  db_dir_(db_dir)
        {}

        bool configure(const json &conf, std::string& error) override {

            LOG(INFO) << "rocksdb[" << get_name() << "] configure=" << conf << std::endl;

            auto i = conf.find("db_name");
            if (i == conf.end()) {
                error = "property 'db_name' is not defined";
                return false;
            }

            db_name_ = i->get<std::string>();

            options_.IncreaseParallelism();
            options_.OptimizeLevelStyleCompaction();
            options_.create_if_missing = true;
            options_.create_missing_column_families= true;

            i = conf.find("buckets");
            if (i != conf.end()) {
                buckets_ = i->get<int>();
            }
            else {
                buckets_ = DEF_ROCKS_BUCKETS;
            }

            i = conf.find("compression");
            if (i != conf.end()) {
                std::string compr = i->get<std::string>();
                options_.compression = parse_compression(compr);
            }

            /**
            i = conf.find("db_paths");
            if (i != conf.end()) {

                json db_paths = *i;

                auto path_it = db_paths.find("path");
                auto target_size_it = db_paths.find("target_size");

                if (path_it != db_paths.end() && target_size_it != db_paths.end()) {

                    std::string path = path_it->get<std::string>();
                    uint64_t target_size = target_size_it->get<uint64_t>();

                    options_.db_paths.push_back(DbPath(path, target_size));
                }
            }
             **/

            /**
            i = conf.find("db_log_dir");
            if (i != conf.end()) {
                options_.db_log_dir = i->get<std::string>();
            }

            i = conf.find("wal_dir");
            if (i != conf.end()) {
                options_.wal_dir = i->get<std::string>();
            }
             **/

            return true;

        }

        bool connect(std::string& error) override {

            column_families_.push_back(ColumnFamilyDescriptor(
                    kDefaultColumnFamilyName, ColumnFamilyOptions()));

            std::vector<std::string> existing_names;

            rocksdb::Status s = DB::Open(options_, db_name_.c_str(), &db_);

            if (!s.ok()) {
                error = s.ToString();
                return false;
            }

            /**
            LOG(INFO) << "existing_names: [";
            for (const auto &n : existing_names) {
                LOG(INFO) << n << ", ";
            }
            LOG(INFO) << "]"  << std::endl;
             **/

            return true;
        }

        ~RocksDriver() override {

            delete db_;

            std::cout << "Graceful shutdown rocks driver" << std::endl;

        }

        bool add_table(const std::string& table, const json& conf, std::string& error) override {

            auto i = map_.find(table);

            if (i != map_.end()) {
                error = "table '" + table +"' already exists in " + get_name();
                return false;
            }

            std::shared_ptr<RocksTable> tbl(new RocksTable(table));

            if (!tbl->configure(conf, error)) {
                return false;
            }

            map_[table] = tbl;

            return true;
        }

        void list_tables(std::vector<std::string>& list) override {

            for (auto &i : map_) {
                list.push_back(i.first);
            }

        }

        std::shared_ptr<Table> find_table(const std::string& table) override {

            auto i = map_.find(table);

            if (i != map_.end()) {

                return std::dynamic_pointer_cast<Table, RocksTable>(i->second);
            }

            return std::shared_ptr<Table>();

        }

        void get(const KeyOperation *request, const std::string& table, ValueResult *response) override {

            do_get(request, table, response);

        }

        void multiGet(const std::vector<MultiGetEntry>& entries) override {

            do_multi_get(entries);

        }

        void scan(const ScanOperation *request, const std::string& table, ::grpc::ServerWriter<ValueResult> *writer) override {

            do_scan(request, table, writer);

        }

        void put(const PutOperation *request, const std::string& table, StatusResult *response) override {

            do_put(request, table, response);

        }

        void remove(const KeyOperation *request, const std::string& table, StatusResult *response) override {

            do_remove(request, table, response);

        }


    private:

        std::string db_name_;

        std::string db_dir_;
        DB* db_;
        Options options_;
        int buckets_;

        std::vector<ColumnFamilyDescriptor> column_families_;
        std::vector<ColumnFamilyHandle*> handles_;

        std::unordered_map<std::string, std::shared_ptr<RocksTable>> map_;

    protected:

        void do_multi_get(const std::vector<MultiGetEntry>& entries);

        void do_scan(const ScanOperation *request, const std::string& table, ::grpc::ServerWriter<ValueResult> *writer);

        void send_result(const ScanOperation *request, ::grpc::ServerWriter<ValueResult> *writer, Slice& key, Slice& value);

        void do_get(const KeyOperation *request, const std::string& table, ValueResult *response);

        void do_put(const PutOperation *request, const std::string& table, StatusResult *response);

        void do_remove(const KeyOperation *request, const std::string& table, StatusResult *response);

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

        void key_result(const std::string& view, const Slice& sliceKey, gkvs::ValueResult* result, const OutputOptions &out) {

            bool includeKey = check::include_key(out);

            if (includeKey) {
                result->mutable_key()->set_recordkey(sliceKey.data(), sliceKey.size());
            }

        }

        void value_result(const Slice& sliceValue, gkvs::Value *value, const OutputOptions &out) {

            if (check::include_value(out)) {
                value->set_raw(sliceValue.data(), sliceValue.size());
            }

        }

        inline CompressionType parse_compression(const std::string& compression) {

            if (compression == "snappy") {
                return kSnappyCompression;
            }

            if (compression == "lz4") {
                return kLZ4Compression;
            }

            if (compression == "lz4h") {
                return kLZ4HCCompression;
            }

            if (compression == "zstd") {
                return kZSTDNotFinalCompression;
            }

            if (compression == "bz2") {
                return kBZip2Compression;
            }

            if (compression == "zlib") {
                return kZlibCompression;
            }

            if (compression == "xpress") {
                return kXpressCompression;
            }

            return kNoCompression;
        }


    };


    std::shared_ptr<Driver> create_rocks_driver(const std::string &name, const std::string &db_dir) {
        return std::shared_ptr<Driver>(new RocksDriver(name, db_dir));
    }


}


void gkvs::RocksDriver::do_scan(const ScanOperation *request, const std::string& table, ::grpc::ServerWriter<ValueResult> *writer) {

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
    result::header(request->header(), response.mutable_header());

    response.mutable_metadata()->set_ttl(100);
    response.mutable_metadata()->add_version(1);

    metadata_result(nullptr, -1, response.mutable_metadata());
    key_result(request->viewname(), key, &response, request->output());
    value_result(value, response.mutable_value(), request->output());
    status::success(response.mutable_status());

    writer->Write(response, grpc::WriteOptions());

}


void gkvs::RocksDriver::do_multi_get(const std::vector<MultiGetEntry>& entries) {

    std::vector<Slice> keys;
    int size = entries.size();

    for (int i = 0; i < size; ++i) {
        keys.push_back(entries[i].get_request().key().recordkey());
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses = db_->MultiGet(ReadOptions(), keys, &values);

    for (int i = 0; i < size; ++i) {

        const KeyOperation& keyOperation = entries[i].get_request();
        ValueResult* result = entries[i].get_response();

        rocksdb::Status& status = statuses[i];
        std::string& value = values[i];

        if (status.ok()) {
            metadata_result(nullptr, -1, result->mutable_metadata());
            value_result(value, result->mutable_value(), keyOperation.output());
            status::success(result->mutable_status());
        }
        else if (status.IsNotFound()) {
            status::success(result->mutable_status());
        }
        else {
            status::driver_error(status.ToString().c_str(), result->mutable_status());
        }

    }

}

void gkvs::RocksDriver::do_get(const KeyOperation *request, const std::string& table, ValueResult *response) {

    const std::string& key = request->key().recordkey();

    std::string value;

    rocksdb::Status status = db_->Get(ReadOptions(), key, &value);

    if (status.ok()) {
        metadata_result(nullptr, -1, response->mutable_metadata());
        value_result(value, response->mutable_value(), request->output());
        status::success(response->mutable_status());
    }
    else if (status.IsNotFound()) {
        status::success(response->mutable_status());
    }
    else {
        status::driver_error(status.ToString().c_str(), response->mutable_status());
    }

}

void gkvs::RocksDriver::do_put(const PutOperation *request, const std::string& table, StatusResult *response) {

    const std::string& key = request->key().recordkey();
    const std::string& value = request->value().raw();

    if (request->compareandput()) {
        status::unsupported("RocksDB driver does not support compare and put", response->mutable_status());
        return;
    }

    rocksdb::Status status = db_->Put(WriteOptions(), key, value);


    if (status.ok()) {

        status::success(response->mutable_status());

    }
    else {
        status::driver_error(status.ToString().c_str(), response->mutable_status());
    }

}

void gkvs::RocksDriver::do_remove(const KeyOperation *request, const std::string& table, StatusResult *response) {

    const std::string& key = request->key().recordkey();

    rocksdb::Status status = db_->Delete(WriteOptions(), key);

    if (status.ok()) {

        status::success(response->mutable_status());

    }
    else {
        status::driver_error(status.ToString().c_str(), response->mutable_status());
    }

}