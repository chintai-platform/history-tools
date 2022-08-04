// copyright defined in LICENSE.txt

#include "fill_pg_plugin.hpp"
#include "state_history_connection.hpp"
#include "state_history_pg.hpp"

#include <boost/algorithm/string/join.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "abieos_sql_converter.hpp"
#include <pqxx/tablewriter>

#include <nlohmann/json.hpp>

#include <abieos.h>
#include <abieos.hpp>

using namespace appbase;
using namespace eosio::ship_protocol;
using namespace state_history;
using namespace state_history::pg;
using namespace std::literals;

namespace asio = boost::asio;
namespace bpo  = boost::program_options;

using asio::ip::tcp;
using boost::beast::flat_buffer;
using boost::system::error_code;

inline std::string to_string(const eosio::checksum256& v) { return sql_str(v); }

inline std::string quote(std::string s) { return "'" + s + "'"; }

/// global variables for recording incremental numbers
struct global_t {
    int64_t transaction_number = 0;
    int64_t action_number = 0;
    int64_t action_data_number = 0;
    int64_t table_row_number = 0;
    int64_t table_row_data_number = 0;
} global_indexes;

/// define abieos context, storing abis for table row delta decoding
struct abieos_context_s {
    const char* last_error = "";
    std::string last_error_buffer{};
    std::string result_str{};
    std::vector<char> result_bin{};

    std::map<abieos::name, abieos::abi> contracts{};
};

/// a wrapper class for pqxx::work to log the SQL command sent to database
struct work_t {
    pqxx::work w;

    work_t(pqxx::connection& conn)
        : w(conn) {}

    auto exec(std::string stmt) {
        dlog(stmt.c_str());
        return w.exec(stmt);
    }

    void commit() { w.commit(); }

    auto quote_name(const std::string& str) { return w.quote_name(str); }
};

/// connection struct to initalize and access the SQL connection from everywhere
// struct connection {
//   work_t t;
//   work_t* get_connection() 
//   {
//     if (t == NULL)
//     {
//       work_t link(*sql_connection);
//       t = link;
//     }
//     return t;
//   }
// }

/// a wrapper class for pqxx::pipeline to log the SQL command sent to database
struct pipeline_t {
    pqxx::pipeline p;

    pipeline_t(work_t& w)
        : p(w.w) {}

    auto insert(std::string stmt) -> decltype(p.insert(stmt)) {
        dlog(stmt.c_str());
        return p.insert(stmt);
    }

    template <typename T>
        auto retrieve(T&& t) {
            return p.retrieve(std::move(t));
        }

    auto retrieve() { return p.retrieve(); }

    bool empty() const { return p.empty(); }

    void complete() { p.complete(); }
};

/// a wrapper class for pqxx::tablewriter to log the write_raw_line()
struct tablewriter {
    pqxx::tablewriter wr;
    tablewriter(work_t& t, const std::string& name)
        : wr(t.w, name) {}
    template<typename ITER>
        tablewriter(
                work_t &t,
                const std::string &name,
                ITER begincolumns,
                ITER endcolumns) :
            wr(t.w, name, begincolumns, endcolumns) {}

    void write_raw_line(std::string v) { wr.write_raw_line(v); }
    void complete() { wr.complete(); }
};

struct table_stream {
    pqxx::connection c;
    work_t           t;
    tablewriter      writer;

    table_stream(const std::string& name)
        : t(c)
          , writer(t, name) {}
    table_stream(const std::string& name, const std::vector<std::string>& columns)
        : t(c),
        writer(t, name, columns.begin(), columns.end()) {}
};

template <typename T>
std::size_t num_bytes(const eosio::opaque<T>& obj) { return obj.num_bytes();}
std::size_t num_bytes(std::optional<eosio::input_stream> strm) { return strm.has_value() ? strm->end - strm->pos : 0; }

struct fpg_session;

struct fill_postgresql_config : connection_config {
    std::string             schema;
    uint32_t                skip_to       = 0;
    uint32_t                stop_before   = 0;
    std::vector<trx_filter> trx_filters   = {};
    bool                    drop_schema   = false;
    bool                    create_schema = false;
    bool                    enable_trim   = false;
};

struct fill_postgresql_plugin_impl : std::enable_shared_from_this<fill_postgresql_plugin_impl> {
    std::shared_ptr<fill_postgresql_config> config = std::make_shared<fill_postgresql_config>();
    std::shared_ptr<fpg_session>            session;
    boost::asio::deadline_timer             timer;

    fill_postgresql_plugin_impl()
        : timer(app().get_io_service()) {}

    ~fill_postgresql_plugin_impl();

    void schedule_retry() {
        timer.expires_from_now(boost::posix_time::seconds(1));
        timer.async_wait([this](auto&) {
                ilog("retry...");
                start();
                });
    }

    void start();
};

eosio::abi_type& get_type(std::map<std::string, eosio::abi_type>& abi_types, std::string type_name) {
    auto itr = abi_types.find(type_name);
    if (itr != abi_types.end()) {
        return itr->second;
    }
    throw std::runtime_error("Unable to find " + type_name + " in the received abi");
}

struct fpg_session : connection_callbacks, std::enable_shared_from_this<fpg_session> {
    fill_postgresql_plugin_impl*                         my = nullptr;
    std::shared_ptr<fill_postgresql_config>              config;
    std::optional<pqxx::connection>                      sql_connection;
    std::shared_ptr<state_history::connection>           connection;
    bool                                                 created_trim    = false;
    uint32_t                                             head            = 0;
    std::string                                          head_id         = "";
    uint32_t                                             irreversible    = 0;
    std::string                                          irreversible_id = "";
    uint32_t                                             first           = 0;
    uint32_t                                             first_bulk      = 0;
    std::map<std::string, std::unique_ptr<table_stream>> table_streams;
    abieos_sql_converter                                 converter;
    std::map<std::string, eosio::abi_type>               abi_types;

    fpg_session(fill_postgresql_plugin_impl* my)
        : my(my)
          , config(my->config) {

              ilog("connect to postgresql");
              sql_connection.emplace();

              using basic_types = std::tuple<
                  bool, uint8_t, int8_t, uint16_t, int16_t, uint32_t, int32_t, uint64_t, int64_t, double, std::string, unsigned __int128,
                  __int128, eosio::float128, eosio::varuint32, eosio::varint32, eosio::name, eosio::checksum256, eosio::time_point,
                  eosio::time_point_sec, eosio::block_timestamp, eosio::public_key, eosio::signature, eosio::bytes, eosio::symbol,
                  eosio::ship_protocol::transaction_status, eosio::ship_protocol::recurse_transaction_trace>;

              converter.register_basic_types<basic_types>();
              converter.schema_name = sql_connection->quote_name(config->schema);
          }

    std::string quote_name(std::string name) { return sql_connection->quote_name(name); }

    void start(asio::io_context& ioc) {
        if (config->drop_schema) {
            work_t t(*sql_connection);
            t.exec("drop schema if exists " + converter.schema_name + " cascade");
            t.commit();
            config->drop_schema = false;
        }

        connection = std::make_shared<state_history::connection>(ioc, *config, shared_from_this());
        connection->connect();
    }

    eosio::abi_type& get_type(std::string type_name) { return ::get_type(this->abi_types, type_name); }

    void received_abi(eosio::abi&& abi) override {
        auto& transaction_trace_abi = ::get_type(abi.abi_types, "transaction_trace");
        for (auto& member : std::get<eosio::abi_type::variant>(transaction_trace_abi._data)) {
            auto& member_abi = ::get_type(abi.abi_types, member.name);
            for (auto& field : std::get<eosio::abi_type::struct_>(member_abi._data).fields) {
                if (field.name == "status")
                    field.type = &abi.abi_types.try_emplace("transaction_status", "transaction_status", eosio::abi_type::builtin{}, nullptr)
                        .first->second;
                else if (field.name == "failed_dtrx_trace" && field.type->name == "transaction_trace?") {
                    field.type = eosio::add_type(abi, (std::vector<eosio::ship_protocol::recurse_transaction_trace>*)nullptr);
                }
            }
        }

        // Add the ABI type here
        //eosio::add_type(abi, (std::vector<eosio::ship_protocol::recurse_transaction_trace>*)nullptr);
        //eosio::add_type(abi, (signed_block_header_modified*)nullptr);
        //eosio::add_type(abi, (std::vector<eosio::ship_protocol::signed_block_header_modified>*)nullptr);

        abi_types = std::move(abi.abi_types);

        if (config->create_schema) {
            create_tables();
            config->create_schema = false;
        }
        else if (global_indexes.transaction_number == 0) {
            work_t t(*sql_connection);
            auto transaction_number = t.exec("select transaction_number from chain.transactions order by transaction_number desc limit 1");
            auto action_number = t.exec("select action_number from chain.actions order by action_number desc limit 1");
            auto action_data_number = t.exec("select action_data_number from chain.action_data order by action_data_number desc limit 1");
            auto table_row_number = t.exec("select table_row_number from chain.table_rows order by table_row_number desc limit 1");
            auto table_row_data_number = t.exec("select table_row_data_number from chain.table_row_data order by table_row_data_number desc limit 1");
            if (!transaction_number.empty()) {
                global_indexes.transaction_number = transaction_number[0][0].as<int64_t>();
            } else {
                global_indexes.transaction_number = 0;
            }
            if (!action_number.empty()) {
                global_indexes.action_number = action_number[0][0].as<int64_t>();
            } else {
                global_indexes.action_number = 0;
            }
            if (!action_data_number.empty()) {
                global_indexes.action_data_number = action_data_number[0][0].as<int64_t>();
            } else {
                global_indexes.action_data_number = 0;
            }
            if (!table_row_number.empty()) {
                global_indexes.table_row_number = table_row_number[0][0].as<int64_t>();
            } else {
                global_indexes.table_row_number = 0;
            }
            if (!table_row_data_number.empty()) {
                global_indexes.table_row_data_number = table_row_data_number[0][0].as<int64_t>();
            } else {
                global_indexes.table_row_data_number = 0;
            }

        }
        connection->send(get_status_request_v0{});
    }

    bool received(get_status_result_v0& status) override {
        work_t t(*sql_connection);
        load_fill_status(t);
        auto       positions = get_positions(t);
        pipeline_t pipeline(t);
        truncate(t, pipeline, head + 1);
        pipeline.complete();
        t.commit();

        connection->request_blocks(status, std::max(config->skip_to, head + 1), positions);
        return true;
    }

    void create_tables() {
        work_t t(*sql_connection);

        ilog("create schema ${s}", ("s", converter.schema_name));
        t.exec("create schema " + converter.schema_name);
        t.exec(
                "create type " + converter.schema_name +
                ".transaction_status_type as enum('executed', 'soft_fail', 'hard_fail', 'delayed', 'expired')");
        t.exec(
                "create table " + converter.schema_name +
                R"(.received_block ("block_num" bigint, "block_id" varchar(64), primary key("block_num")))");
        t.exec(
                "create table " + converter.schema_name +
                R"(.fill_status ("head" bigint, "head_id" varchar(64), "irreversible" bigint, "irreversible_id" varchar(64), "first" bigint))");
        t.exec("create unique index on " + converter.schema_name + R"(.fill_status ((true)))");
        t.exec("insert into " + converter.schema_name + R"(.fill_status values (0, '', 0, '', 0))");

        auto exec = [&t](const auto& stmt) { t.exec(stmt); };
        converter.create_table("block_info", get_type("signed_block_header"), "block_num bigint, block_id varchar(64)", {"block_num"}, exec);
        converter.create_table(
                "transaction_trace", get_type("transaction_trace"), "block_num bigint, transaction_ordinal integer",
                {"block_num", "transaction_ordinal"}, exec);

        t.exec("create table " + converter.schema_name + ".blocks " + R"((block_number BIGINT CONSTRAINT block_info_pk PRIMARY KEY, block_id varchar(64), timestamp TIMESTAMP, previous varchar(64), transaction_mroot varchar(64), action_mroot varchar(64), producer_signature varchar))");
        t.exec("create table " + converter.schema_name + ".transactions " + R"((transaction_number BIGINT PRIMARY KEY, block_number BIGINT, transaction_ordinal INT, id varchar(64), status varchar))");
        t.exec("create table " + converter.schema_name + ".actions " + R"((action_number BIGINT PRIMARY KEY, transaction_id TEXT, action_ordinal INT, creator_action_ordinal INT, receiver varchar(12), action_account varchar(12), action_name varchar(12), action_permission TEXT, action_data TEXT, context_free BOOL, console TEXT))");
        t.exec("create table " + converter.schema_name + ".action_data " + R"((action_data_number BIGINT PRIMARY KEY, action_number BIGINT, key TEXT, value TEXT))");
        t.exec("create table " + converter.schema_name + ".table_rows " + R"((table_row_number BIGINT PRIMARY KEY, block_number BIGINT, account TEXT, scope TEXT, table_name TEXT, primary_key TEXT))");
        t.exec("create table " + converter.schema_name + ".table_row_data " + R"((table_row_data_number BIGINT PRIMARY KEY, table_row_number BIGINT, key TEXT, value TEXT))");

        for (auto& table : connection->abi.tables) {
            std::vector<std::string> keys = {"block_num", "present"};
            keys.insert(keys.end(), table.key_names.begin(), table.key_names.end());
            converter.create_table(table.type, get_type(table.type), "block_num bigint, present smallint", keys, exec);
        }

        t.commit();
    } // create_tables()

    void create_trim() {
        if (created_trim)
            return;
        work_t t(*sql_connection);
        ilog("create_trim");
        for (auto& table : connection->abi.tables) {
            if (table.key_names.empty())
                continue;
            std::string query = "create index if not exists " + table.type;
            for (auto& k : table.key_names)
                query += "_" + k;
            query += "_block_present_idx on " + converter.schema_name + "." + quote_name(table.type) + "(\n";
            for (auto& k : table.key_names)
                query += "    " + quote_name(k) + ",\n";
            query += "    \"block_num\" desc,\n    \"present\" desc\n)";
            // std::cout << query << ";\n\n";
            t.exec(query);
        }

        std::string query = R"(
            drop function if exists )" +
        converter.schema_name + R"(.trim_history;
        )";
        // std::cout << query << "\n";
        t.exec(query);

        query = R"(
            create function )" +
        converter.schema_name + R"(.trim_history(
                prev_block_num bigint,
                irrev_block_num bigint
            ) returns void
            as $$
                declare
                    key_search record;
                begin)";

        static const char* const simple_cases[] = {
            "received_block",
            "transaction_trace",
            "blocks",
        };

        for (const char* table : simple_cases) {
            query += R"(
                    delete from )" +
            converter.schema_name + "." + quote_name(table) + R"(
                    where
                        block_num >= prev_block_num
                        and block_num < irrev_block_num;
                    )";
        }

        auto add_trim = [&](const std::string& table, const std::string& keys, const std::string& search_keys) {
            query += R"(
                    for key_search in
                        select
                            distinct on()" +
            keys + R"()
                            )" +
            keys + R"(, block_num
                        from
                            )" +
            converter.schema_name + "." + quote_name(table) + R"(
                        where
                            block_num > prev_block_num and block_num <= irrev_block_num
                        order by )" +
            keys + R"(, block_num desc, present desc
                    loop
                        delete from )" +
            converter.schema_name + "." + quote_name(table) + R"(
                        where
                            ()" +
            keys + R"() = ()" + search_keys + R"()
                            and block_num < key_search.block_num;
                    end loop;
                    )";
        };

        for (auto& table : connection->abi.tables) {
            if (table.key_names.empty()) {
                query += R"(
                    for key_search in
                        select
                            block_num
                        from
                            )" +
                converter.schema_name + "." + quote_name(table.type) + R"(
                        where
                            block_num > prev_block_num and block_num <= irrev_block_num
                        order by block_num desc, present desc
                        limit 1
                    loop
                        delete from )" +
                converter.schema_name + "." + quote_name(table.type) + R"(
                        where
                            block_num < key_search.block_num;
                    end loop;
                    )";
            } else {
                std::string keys, search_keys;
                for (auto& k : table.key_names) {
                    if (&k != &table.key_names.front()) {
                        keys += ", ";
                        search_keys += ", ";
                    }
                    keys += quote_name(k);
                    search_keys += "key_search." + quote_name(k);
                }
                add_trim(table.type, keys, search_keys);
            };
        }
        query += R"(
                end
            $$ language plpgsql;
        )";

        // std::cout << query << "\n\n";
        t.exec(query);
        t.commit();
        created_trim = true;
    } // create_trim

    void load_fill_status(work_t& t) {
        auto r  = t.exec("select head, head_id, irreversible, irreversible_id, first from " + converter.schema_name + ".fill_status")[0];
        head    = r[0].as<uint32_t>();
        head_id = r[1].as<std::string>();
        irreversible    = r[2].as<uint32_t>();
        irreversible_id = r[3].as<std::string>();
        first           = r[4].as<uint32_t>();
    }

    std::vector<block_position> get_positions(work_t& t) {
        std::vector<block_position> result;
        auto                        rows = t.exec(
                "select block_num, block_id from " + converter.schema_name + ".received_block where block_num >= " +
                std::to_string(irreversible) + " and block_num <= " + std::to_string(head) + " order by block_num");
        for (auto row : rows)
            result.push_back({row[0].as<uint32_t>(), sql_to_checksum256(row[1].as<std::string>().c_str())});
        return result;
    }

    void write_fill_status(work_t& t, pipeline_t& pipeline) {
        std::string query =
            "update " + converter.schema_name + ".fill_status set head=" + std::to_string(head) + ", head_id=" + quote(head_id) + ", ";
        if (irreversible < head)
            query += "irreversible=" + std::to_string(irreversible) + ", irreversible_id=" + quote(irreversible_id);
        else
            query += "irreversible=" + std::to_string(head) + ", irreversible_id=" + quote(head_id);
        query += ", first=" + std::to_string(first);
        pipeline.insert(query);
    }

    void truncate(work_t& t, pipeline_t& pipeline, uint32_t block) {
        auto trunc = [&](const std::string& name) {
            std::string query{"delete from " + converter.schema_name + "." + quote_name(name) +
                " where block_num >= " + std::to_string(block)};
            pipeline.insert(query);
        };
        trunc("received_block");
        trunc("transaction_trace");
        std::string query{"delete from " + converter.schema_name + "." + quote_name("blocks") +
            " where block_number >= " + std::to_string(block)};
        pipeline.insert(query);
        for (auto& table : connection->abi.tables) {
            trunc(table.type);
        }

        auto result = pipeline.retrieve(pipeline.insert(
                    "select block_id from " + converter.schema_name + ".received_block where block_num=" + std::to_string(block - 1)));
        if (result.empty()) {
            head    = 0;
            head_id = "";
        } else {
            head    = block - 1;
            head_id = result.front()[0].as<std::string>();
        }
        first = std::min(first, head);
    } // truncate


    template <typename GetBlockResult, typename HandleBlocksTracesDelta>
        bool process_blocks_result(GetBlockResult& result, HandleBlocksTracesDelta&& handler) {
            if (!result.this_block)
                return true;
            bool bulk         = result.this_block->block_num + 4 < result.last_irreversible.block_num;
            bool large_deltas = false;
            bool forks        = false;
            auto deltas_size  = num_bytes(result.deltas);

            if (!bulk && deltas_size >= 10 * 1024 * 1024) {
                ilog("large deltas size: ${s}", ("s", uint64_t(deltas_size)));
                bulk         = true;
                large_deltas = true;
            }

            if (config->stop_before && result.this_block->block_num >= config->stop_before) {
                close_streams();
                ilog("block ${b}: stop requested", ("b", result.this_block->block_num));
                return false;
            }

            if (result.this_block->block_num <= head) {
                close_streams();
                ilog("switch forks at block ${b}", ("b", result.this_block->block_num));
                bulk = false;
                forks = true;
            }

            if (!bulk || large_deltas || !(result.this_block->block_num % 200))
                close_streams();
            if (table_streams.empty())
                trim();
            if (!bulk)
                ilog("block ${b}", ("b", result.this_block->block_num));

            work_t     t(*sql_connection);
            pipeline_t pipeline(t);
            if (result.this_block->block_num <= head)
                truncate(t, pipeline, result.this_block->block_num);
            if (!head_id.empty() && (!result.prev_block || to_string(result.prev_block->block_id) != head_id))
                throw std::runtime_error("prev_block does not match");

            handler(bulk);

            head            = result.this_block->block_num;
            head_id         = to_string(result.this_block->block_id);
            irreversible    = result.last_irreversible.block_num;
            irreversible_id = to_string(result.last_irreversible.block_id);
            if (!first)
                first = head;
            if (!bulk) {

                if (!forks)
                    flush_streams();

                write_fill_status(t, pipeline);
            }
            pipeline.insert(
                    "insert into " + converter.schema_name + ".received_block (block_num, block_id) values (" +
                    std::to_string(result.this_block->block_num) + ", " + quote(to_string(result.this_block->block_id)) + ")");

            pipeline.complete();
            while (!pipeline.empty())
                pipeline.retrieve();
            t.commit();
            if (large_deltas)
                close_streams();
            return true;
        }

    bool received(get_blocks_result_v2& result) override {
        return process_blocks_result(result, [this,&result](bool bulk) {
                if (!result.block_header.empty())
                receive_block(result.this_block->block_num, result.this_block->block_id, result.block_header);
                if (!result.traces.empty())
                receive_traces(result.this_block->block_num, result.traces);
                if (!result.deltas.empty())
                receive_deltas(result.this_block->block_num, result.deltas, bulk);
                });
    }

    bool received(get_blocks_result_v1& result) override {
        return process_blocks_result(result, [this,&result](bool bulk) {
                if (result.block) {
                const signed_block_header& header = std::visit([](const auto& v) -> const signed_block_header& { return v; }, result.block.value());
                std::vector<char>   data   = eosio::convert_to_bin(header);
                receive_block(result.this_block->block_num, result.this_block->block_id, eosio::as_opaque<signed_block_header>(eosio::input_stream{data}));
                }
                if (!result.deltas.empty())
                receive_deltas(result.this_block->block_num, result.deltas, bulk);
                if (!result.traces.empty())
                receive_traces(result.this_block->block_num, result.traces);
                });
    }

    bool received(get_blocks_result_v0& result) override {
        return process_blocks_result(result, [this,&result](bool bulk) {
                if (result.block) {
                auto     block_bin = *result.block;
                receive_block(result.this_block->block_num, result.this_block->block_id, eosio::as_opaque<signed_block_header>(block_bin));
                }
                if (result.deltas)
                receive_deltas(
                        result.this_block->block_num, eosio::as_opaque<std::vector<eosio::ship_protocol::table_delta>>(*result.deltas), bulk);
                if (result.traces)
                receive_traces(
                        result.this_block->block_num, eosio::as_opaque<std::vector<eosio::ship_protocol::transaction_trace>>(*result.traces));
                });
    }

    void write_stream(uint32_t block_num, const std::string& name, const std::vector<std::string>& values) {
        if (!first_bulk)
            first_bulk = block_num;
        auto& ts = table_streams[name];
        if (!ts)
            ts = std::make_unique<table_stream>(converter.schema_name + "." + quote_name(name));
        ts->writer.write_raw_line(boost::algorithm::join(values, "\t"));
    }

    void write_stream_custom(uint32_t block_num, const std::string& name, const std::vector<std::string>& values) {
        if (!first_bulk)
            first_bulk = block_num;
        auto& ts = table_streams[name];
        if (!ts) {
            std::vector<std::string> columns;
            if (name == "transactions")
            {
                columns = {"transaction_number", "block_number", "transaction_ordinal", "id", "status"};
            }
            else if (name == "actions")
            {
                columns = {"action_number", "transaction_id", "action_ordinal", "creator_action_ordinal", "receiver", "action_account", "action_name", "action_permission", "action_data", "context_free", "console"};
            }
            else if (name == "action_data")
            {
                columns = {"action_data_number", "action_number", "key", "value"};
            }
            else if (name == "table_rows")
            {
                columns = {"table_row_number", "block_number", "account", "scope", "table_name", "primary_key"};
            }
            else if (name == "table_row_data")
            {
                columns = {"table_row_data_number", "table_row_number", "key", "value"};
            }

            ts = std::make_unique<table_stream>(converter.schema_name + "." + quote_name(name), columns);
        }
        ts->writer.write_raw_line(boost::algorithm::join(values, "\t"));
    }

    void flush_streams() {
        for (auto& [_, ts] : table_streams) {
            ts->writer.complete();
            ts->t.commit();
        }
        table_streams.clear();
    }

    void close_streams() {
        if (table_streams.empty())
            return;
        flush_streams();

        work_t     t(*sql_connection);
        pipeline_t pipeline(t);
        write_fill_status(t, pipeline);
        pipeline.complete();
        t.commit();

        ilog("block ${b} - ${e}", ("b", first_bulk)("e", head));
        first_bulk = 0;
    }

    void receive_block(uint32_t block_num, const eosio::checksum256& block_id, const eosio::opaque<signed_block_header>& opq) {
        auto&                    abi_type = get_type("signed_block_header");
        std::vector<std::string> values{std::to_string(block_num), sql_str(block_id)};
        auto                     bin = opq.get();
        converter.to_sql_values(bin, *abi_type.as_struct(), values);
        // Get rid of producer 3, confirmed 4, schedule_version 8, new_producers 9, header extensions 10
        values.erase(values.begin()+10);
        values.erase(values.begin()+9);
        values.erase(values.begin()+8);
        values.erase(values.begin()+4);
        values.erase(values.begin()+3);

        write_stream(block_num, "blocks", values);
    }

    void receive_deltas(uint32_t block_num, eosio::opaque<std::vector<eosio::ship_protocol::table_delta>> delta, bool bulk) {
        for_each(delta, [ this, block_num, bulk ](auto t_delta){
                std::string table_name = std::get<1>(t_delta).name;
                if (table_name == "contract_row")
                {
                write_table_delta(block_num, std::move(t_delta), bulk);
                }
                });
    }

    void write_table_delta(uint32_t block_num, table_delta&& t_delta, bool bulk) {
        std::visit(
                [&block_num, bulk, this](auto t_delta) {
                size_t num_processed = 0;
                auto&  type          = get_type(t_delta.name);
                if (type.as_variant() == nullptr && type.as_struct() == nullptr)
                throw std::runtime_error("don't know how to process " + t_delta.name);

                for (auto& row : t_delta.rows) {
                if (t_delta.rows.size() > 10000 && !(num_processed % 10000))
                ilog(
                        "block ${b} ${t} ${n} of ${r} bulk=${bulk}",
                        ("b", block_num)("t", t_delta.name)("n", num_processed)("r", t_delta.rows.size())("bulk", bulk));

                std::vector<std::string> values{std::to_string(block_num), std::to_string((unsigned)row.present)};
                if (type.as_variant())
                converter.to_sql_values(row.data, t_delta.name, *type.as_variant(), values);
                else if (type.as_struct())
                converter.to_sql_values(row.data, *type.as_struct(), values);

		std::string account = values.at(2);
		if (account != "eosio" && account != "eosio.token")
		{
                  process_table_row_delta(block_num, values);
		}
                ++num_processed;
                }
                },
            t_delta);
    }

    void process_table_row_delta(uint32_t const block_num, std::vector<std::string> values) 
    {
	    std::cout << "account name: " << values.at(2) << std::endl;
	    std::cout << "process table row delta" << std::endl;
	uint64_t table_row_number(0);
        if (values.at(1) == "2")
        {
		std::cout << "new table row" << std::endl;
            write_table_row(block_num, values);
	    table_row_number = global_indexes.table_row_number+1;
        }
	else
	{
	        std::cout << "Existing table row" << std::endl;
	   // Use table row number from existing table row
	   std::string account = values.at(2);
	   std::string scope = values.at(3);
	   std::string table_name = values.at(4);

           std::string command = "/usr/bin/psql -U postgres -h 172.17.0.3 -c 'select table_row_number from chain.table_rows where account = " + account + " and scope = " + scope + " and table_name = " + table_name + " limit 1'"; 
           std::string command_output = get_command_line_output(command);

	   std::cout << command_output << std::endl;
	   
	   //table_row_number = table_row[0][0].as<uint64_t>();
	}

	write_table_row_data(block_num, values, table_row_number);
    }

    void write_table_row_data(uint32_t const block_num, std::vector<std::string> values, uint64_t const table_row_number)
    {
	    std::cout << "write table row data" << std::endl;
	auto context = abieos_create();

	// make sure the abi is loaded into the context, add it if not
	eosio::name contract = eosio::name{values.at(2)};
	uint64_t contract_int = eosio::name{values.at(2)}.value;
	auto contract_itr = context->contracts.find(::abieos::name{contract_int});
	if (contract_itr == context->contracts.end())
  	{
           std::string command = "/usr/bin/psql -U postgres -h 172.17.0.3 -c 'select action_data from chain.actions where action_account = " + contract.to_string() + " and action_name = setabi limit 1'"; 
           std::string command_output = get_command_line_output(command);

	   std::cout << "Abi not loaded into context" << std::endl;
	   std::cout << command_output << std::endl;

//	   std::string hex_data = actions_row[0][8].as<std::string>();
//	   std::cout << "hex data: " << hex_data << std::endl;
//	   set_abi_hex(contract, hex_data);
	}

	eosio::name table_name = eosio::name{values.at(4)};
	const char* type = abieos_get_type_for_table(context, contract_int, table_name.value);
	const char* hex = values.at(6).c_str();
	
	// decode the data and record it
        const char* json_data = abieos_hex_to_json(context, contract_int, type, hex);
        nlohmann::json json = nlohmann::json::parse(json_data);

        for (auto itr = json.begin(); itr != json.end(); ++itr)
        {
        global_indexes.table_row_data_number++;
        std::vector<std::string> table_row_data_values;
        table_row_data_values.push_back(std::to_string(global_indexes.table_row_data_number));
        table_row_data_values.push_back(std::to_string(table_row_number));
        table_row_data_values.push_back(itr.key());
        table_row_data_values.push_back(itr.value().dump());
        write_stream_custom(block_num, "table_row_data", table_row_data_values);
        }
    }

    void write_table_row(uint32_t const block_num, std::vector<std::string> values)
    {
	    std::cout << "write table row" << std::endl;
        std::vector<std::string> table_row_values;

        global_indexes.table_row_number++;
        table_row_values.push_back(std::to_string(global_indexes.table_row_number));
        table_row_values.push_back(values.at(0));
        table_row_values.push_back(values.at(2));
        table_row_values.push_back(values.at(3));
        table_row_values.push_back(values.at(4));
        table_row_values.push_back(values.at(5));

        write_stream_custom(block_num, "table_rows", table_row_values);
    }

    void receive_traces(uint32_t const block_num, eosio::opaque<std::vector<eosio::ship_protocol::transaction_trace>> traces) {
        auto     bin = traces.get();
        uint32_t num;
        varuint32_from_bin(num, bin);
        uint32_t num_ordinals = 0;
        for (uint32_t i = 0; i < num; ++i) {
            auto              trace_bin = bin;
            transaction_trace trace;
            from_bin(trace, bin);
            if (filter(config->trx_filters, trace))
                write_transaction_trace(block_num, num_ordinals, trace, trace_bin);
        }
    }

    void write_transaction_trace(
            uint32_t const block_num, uint32_t& num_ordinals, const eosio::ship_protocol::transaction_trace& trace, eosio::input_stream trace_bin) {
        auto failed = std::visit(
                [](auto& ttrace) { return !ttrace.failed_dtrx_trace.empty() ? &ttrace.failed_dtrx_trace[0].recurse : nullptr; }, trace);
        if (failed != nullptr) {
            if (!filter(config->trx_filters, *failed))
                return;
            std::vector<char> data = eosio::convert_to_bin(*failed);
            write_transaction_trace(block_num, num_ordinals, *failed, eosio::input_stream{data});
        }

        write_action_traces(block_num, std::get<0>(trace).id, std::get<0>(trace).action_traces);

        auto                     transaction_ordinal = ++num_ordinals;
        std::vector<std::string> values{std::to_string(block_num), std::to_string(transaction_ordinal)};
        converter.to_sql_values(trace_bin, "transaction_trace", *get_type("transaction_trace").as_variant(), values);

        // delete unwanted values here.
        values.erase(values.begin()+14);
        values.erase(values.begin()+13);
        values.erase(values.begin()+12);
        values.erase(values.begin()+11);
        values.erase(values.begin()+10);
        values.erase(values.begin()+9);
        values.erase(values.begin()+8);
        values.erase(values.begin()+7);
        values.erase(values.begin()+6);
        values.erase(values.begin()+5);
        values.erase(values.begin()+4);

        global_indexes.transaction_number++;
        values.insert(values.begin(), std::to_string(global_indexes.transaction_number));

        write_stream_custom(block_num, "transactions", values);
    } // write_transaction_trace

    struct chintai_uint256_t
    {
        uint64_t bits[4];
    };

    void write_action_traces(uint32_t const block_number,
            eosio::checksum256 const transaction_id,
            std::vector<eosio::ship_protocol::action_trace> const &action_traces)
    {
	auto context = abieos_create();
        for (int i=0; i < action_traces.size(); ++i)
        {
            chintai_uint256_t xyz;
            memcpy(&xyz, &transaction_id, 32);
            char hexstring[65];  // needs to be at least 64 hex digits + 1 for the null terminator
            sprintf(hexstring, "%016llx%016llx%016llx%016llx", xyz.bits[3], xyz.bits[2], xyz.bits[1], xyz.bits[0]);
            char trx_id[32];
            memcpy(trx_id, &transaction_id, 32);
            std::vector<std::string> values{};
            values.push_back(std::string(hexstring));
            eosio::ship_protocol::action_trace_v1 trace = std::get<1>(action_traces.at(i));

            if (trace.act.name.to_string() == "onblock" || 
		trace.act.name.to_string() == "setcode" ||
                trace.act.account.to_string() == "eosio.null")
            {
                continue;
            }

            values.push_back(std::to_string(uint32_t(trace.action_ordinal)));
            values.push_back(std::to_string(uint32_t(trace.creator_action_ordinal)));
            values.push_back(trace.receiver.to_string());
            values.push_back(trace.act.account.to_string());
            values.push_back(trace.act.name.to_string());
            std::string authorization_string = get_authorization_string(trace.act.authorization);
            values.push_back(authorization_string);
            size_t remaining_bytes = trace.act.data.remaining();
            unsigned char * data = new unsigned char[remaining_bytes];
            trace.act.data.read(data, remaining_bytes);
            std::string hex_data = hexStr(data, remaining_bytes);
            values.push_back(hex_data);
            delete[] data;
            values.push_back(std::to_string(trace.context_free));
            values.push_back(trace.console);

            global_indexes.action_number++;
            values.insert(values.begin(), std::to_string(global_indexes.action_number));

            write_stream_custom(block_number, "actions", values);

            write_action_data(block_number, trace.act.account.to_string(), trace.act.name.to_string(), hex_data);
        }
    } //write_action_traces

    void set_abi_hex(eosio::name const &account, std::string const &hex_data)
    {
	auto context = abieos_create();
	nlohmann::json json = get_json("eosio", "setabi", hex_data);
	std::string abi_hex;
	for (auto itr = json.begin(); itr != json.end(); ++itr)
	{
           if (itr.key() == "abi")
	   {
	      abi_hex = itr.value().dump().c_str();
	      abi_hex.erase(remove(abi_hex.begin(), abi_hex.end(), '"'), abi_hex.end());
	   }
	}

	std::cout << "before set abi hex" << std::endl;
	std::cout << "Contracts loaded onto context: " << context->contracts.size() << std::endl;
        abieos_set_abi_hex(context, account.value, abi_hex.c_str());
	std::cout << "after set abi hex" << std::endl;
	std::cout << "Contracts loaded onto context: " << context->contracts.size() << std::endl;
       	for (auto const& element : context->contracts)
        {
            std::cout << "Contract name: " << element.first.to_string() << std::endl;
        }
    }

    nlohmann::json get_json(std::string const &action_account, std::string const &action_name, std::string const &action_data)
    {
        std::string command = "/usr/bin/cleos -u http://192.168.12.185:8888 convert unpack_action_data " + action_account + " " + action_name + " " + action_data; 
        std::string command_output = get_command_line_output(command);

        nlohmann::json command_json = nlohmann::json::parse(command_output);
	return command_json;
    }

    void write_action_data(uint32_t const block_number,
            std::string const &action_account,
            std::string const &action_name,
            std::string const &action_data)
    {
	nlohmann::json command_json = get_json(action_account, action_name, action_data);

        for (auto itr = command_json.begin(); itr != command_json.end(); ++itr)
        {
            std::vector<std::string> values{};

            global_indexes.action_data_number++;
            values.push_back(std::to_string(global_indexes.action_data_number));
            values.push_back(std::to_string(global_indexes.action_number));
            values.push_back(itr.key());
            values.push_back(itr.value().dump());
            write_stream_custom(block_number, "action_data", values);
        }

	if(action_name == "setabi")
	{
	  std::string account;
          for (auto itr = command_json.begin(); itr != command_json.end(); ++itr)
	  {
	    if (itr.key() == "account")
	    {
              account = itr.value();
	    }
	  }
	  set_abi_hex(eosio::name{account}, action_data);
	}
    } //write_action_data

    std::string get_command_line_output(std::string command) {
        const char* char_command = command.c_str(); 
	//TODO use the exit code to determine whether there was an error
        int exit_code = system(char_command);
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(char_command, "r"), pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

    std::string get_authorization_string(std::vector<permission_level> const &authorizations)
    {
        std::string authorization_string = "";
        for (int i = 0; i < authorizations.size(); ++i)
        {
            permission_level current = authorizations.at(i);
            authorization_string.append(current.actor.to_string());
            authorization_string.append("@");
            authorization_string.append(current.permission.to_string());
            if (i + 1 < authorizations.size())
            {
                authorization_string.append(", ");
            }
        }

        return authorization_string;
    }

    static constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    std::string hexStr(unsigned char *data, size_t len)
    {
        std::string s(len * 2, ' ');
        for (int i = 0; i < len; ++i) {
            s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
            s[2 * i + 1] = hexmap[data[i] & 0x0F];
        }
        return s;
    }

    void trim() {
        if (!config->enable_trim)
            return;
        auto end_trim = std::min(head, irreversible);
        if (first >= end_trim)
            return;
        create_trim();
        work_t t(*sql_connection);
        ilog("trim  ${b} - ${e}", ("b", first)("e", end_trim));
        t.exec("select * from " + converter.schema_name + ".trim_history(" + std::to_string(first) + ", " + std::to_string(end_trim) + ")");
        t.commit();
        ilog("      done");
        first = end_trim;
    }

    void closed(bool retry) override {
        if (my) {
            my->session.reset();
            if (retry)
                my->schedule_retry();
        }
    }

    ~fpg_session() {}
}; // fpg_session


static abstract_plugin& _fill_postgresql_plugin = app().register_plugin<fill_pg_plugin>();

fill_postgresql_plugin_impl::~fill_postgresql_plugin_impl() {
    if (session)
        session->my = nullptr;
}

void fill_postgresql_plugin_impl::start() {
    session = std::make_shared<fpg_session>(this);
    session->start(app().get_io_service());
}

fill_pg_plugin::fill_pg_plugin()
    : my(std::make_shared<fill_postgresql_plugin_impl>()) {}

    fill_pg_plugin::~fill_pg_plugin() {}

    void fill_pg_plugin::set_program_options(options_description& cli, options_description& cfg) {
        auto clop = cli.add_options();
        clop("fpg-drop", "Drop (delete) schema and tables");
        clop("fpg-create", "Create schema and tables");
    }

void fill_pg_plugin::plugin_initialize(const variables_map& options) {
    try {
        auto endpoint = options.at("fill-connect-to").as<std::string>();
        if (endpoint.find(':') == std::string::npos)
            throw std::runtime_error("invalid endpoint: " + endpoint);

        auto port                 = endpoint.substr(endpoint.find(':') + 1, endpoint.size());
        auto host                 = endpoint.substr(0, endpoint.find(':'));
        my->config->host          = host;
        my->config->port          = port;
        my->config->schema        = options["pg-schema"].as<std::string>();
        my->config->skip_to       = options.count("fill-skip-to") ? options["fill-skip-to"].as<uint32_t>() : 0;
        my->config->stop_before   = options.count("fill-stop") ? options["fill-stop"].as<uint32_t>() : 0;
        my->config->trx_filters   = fill_plugin::get_trx_filters(options);
        my->config->drop_schema   = options.count("fpg-drop");
        my->config->create_schema = options.count("fpg-create");
        my->config->enable_trim   = options.count("fill-trim");
    }
    FC_LOG_AND_RETHROW()
}

void fill_pg_plugin::plugin_startup() { my->start(); }

void fill_pg_plugin::plugin_shutdown() {
    if (my->session)
        my->session->connection->close(false);
    my->timer.cancel();
    ilog("fill_pg_plugin stopped");
}
