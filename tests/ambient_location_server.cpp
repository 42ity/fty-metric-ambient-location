#include "src/fty_ambient_location_server.h"
#include <catch2/catch.hpp>
#include <fty_shm.h>
#include <malamute.h>

//  --------------------------------------------------------------------------
//  Self test of this class

// publish metrics
auto publishOnShm = [](const std::string& name, const std::string& type, const std::string& value, const std::string& unit) {

    fty_proto_t* n_met = fty_proto_new(FTY_PROTO_METRIC);
    REQUIRE(n_met);

    int ttl = 60;
    fty_proto_set_name(n_met, name.c_str());
    fty_proto_set_type(n_met, type.c_str());
    fty_proto_set_value(n_met, "%s", value.c_str());
    fty_proto_set_unit(n_met, "%s", unit.c_str());
    fty_proto_set_ttl(n_met, uint32_t(ttl));
    fty_proto_set_time(n_met, uint64_t(std::time(nullptr)));

    char* aux_log = NULL;
    asprintf(&aux_log, "%s@%s (value: %s%s, ttl: %u)",
        fty_proto_type(n_met), fty_proto_name(n_met),
        fty_proto_value(n_met), fty_proto_unit(n_met),
        fty_proto_ttl(n_met));

    int rv = fty::shm::write_metric(n_met);
    REQUIRE(rv == 0);
    zstr_free(&aux_log);
    fty_proto_destroy(&n_met);
};


TEST_CASE("ambient location server test")
{
    static const char* endpoint = "inproc://fty_metric_ambient_location_test";

    zactor_t* server = zactor_new(mlm_server, const_cast<char*>("Malamute"));
    zstr_sendx(server, "BIND", endpoint, nullptr);

    const char* SELFTEST_DIR_RW = ".";

    fty_shm_set_test_dir(SELFTEST_DIR_RW);
    fty_shm_set_default_polling_interval(2);

    zactor_t* ambient_location = zactor_new(fty_ambient_location_server, nullptr);

    zstr_sendx(ambient_location, "CONNECT", endpoint, "fty-ambient-location", nullptr);
    zstr_sendx(ambient_location, "CONSUMER", FTY_PROTO_STREAM_ASSETS, ".*", nullptr);

    sleep(1);
    mlm_client_t* producer = mlm_client_new();
    mlm_client_connect(producer, endpoint, 1000, "producer");
    mlm_client_set_producer(producer, FTY_PROTO_STREAM_ASSETS);

    // Build hierarchy (two sensor -input- on a datacenter)
    zhash_t* aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("device"));
    zhash_insert(aux, "subtype", const_cast<char*>("sensor"));

    zhash_t* ext = zhash_new();
    zhash_autofree(ext);

    zhash_insert(ext, "logical_asset", const_cast<char*>("datacenter-1"));
    zhash_insert(ext, "sensor_function", const_cast<char*>("input"));

    const char* subject = "ASSET_MANIPULATION";
    zmsg_t*     msg     = fty_proto_encode_asset(aux, "sensor-1", FTY_PROTO_ASSET_OP_CREATE, ext);
    int         rv      = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    zhash_destroy(&aux);
    zhash_destroy(&ext);

    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("device"));
    zhash_insert(aux, "subtype", const_cast<char*>("sensor"));

    ext = zhash_new();
    zhash_autofree(ext);

    zhash_insert(ext, "logical_asset", const_cast<char*>("datacenter-1"));
    zhash_insert(ext, "sensor_function", const_cast<char*>("input"));

    msg = fty_proto_encode_asset(aux, "sensor-2", FTY_PROTO_ASSET_OP_CREATE, ext);
    rv  = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    if (aux) zhash_destroy(&aux);
    if (ext) zhash_destroy(&ext);

    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("datacenter"));
    zhash_insert(aux, "subtype", const_cast<char*>("N_A"));

    msg = fty_proto_encode_asset(aux, "datacenter-1", FTY_PROTO_ASSET_OP_CREATE, nullptr);
    rv  = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    if (aux) zhash_destroy(&aux);

    sleep(1);
    zstr_sendx(ambient_location, "START", nullptr);
    sleep(1);

    // send values for sensor-1 first
    publishOnShm("sensor-1", "humidity.default", "40", "%");
    publishOnShm("sensor-1", "temperature.default", "25", "C");

    // wait calculation
    sleep(5);

    fty_proto_t* m;
    {
        fty::shm::shmMetrics resultH;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultH);
        m = resultH.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "40.00")); // <<< 40 / 1
        m = nullptr;

        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*temperature", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "25.00")); // <<< 25 / 1
        m = nullptr;
    }

    // send values for sensor-2 first
    publishOnShm("sensor-2", "humidity.default", "100", "%");
    publishOnShm("sensor-2", "temperature.default", "27", "C");

    // wait calculation
    sleep(5);

    {
        fty::shm::shmMetrics resultH;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultH);
        m = resultH.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "70.00")); // <<< (100 + 40) / 2
        m = nullptr;

        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*temperature", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "26.00")); // <<< (25 + 27) / 2
        m = nullptr;
    }

    // send values for sensor-1 again
    publishOnShm("sensor-1", "humidity.default", "70", "%");
    publishOnShm("sensor-1", "temperature.default", "29", "C");

    // wait calculation
    sleep(5);

    {
        fty::shm::shmMetrics resultH;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultH);
        m = resultH.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "85.00")); // <<< (70 + 100)  / 2
        m = nullptr;

        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*temperature", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "28.00")); // <<< (27 + 29)  / 2
        m = nullptr;
    }

    zactor_destroy(&ambient_location);
    mlm_client_destroy(&producer);
    zactor_destroy(&server);
    fty_shm_delete_test_dir();
}
