/*
 * 64-bit Unique Id Generator
 *
 * ------------------------------------------------------------------------
 * |0| 11 bit machine ID |      40-bit timestamp         | 12-bit counter |
 * ------------------------------------------------------------------------
 *
 * 11-bit machine Id code by hasing the MAC address
 * 40-bit UNIX timestamp in millisecond precision with custom epoch
 * 12 bit counter which increases monotonically on single process
 *
 */

#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"  // for httplib server
#include "UniqueIdHandler.h"

using json = nlohmann::json;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "unique-id-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["unique-id-service"]["port"];
  std::string netif = config_json["unique-id-service"]["netif"];

  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex thread_lock;
  UniqueIdHandler handler(&thread_lock, machine_id);
  httplib::Server server;

  server.Post("/ComposeUniqueId", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"].get<int64_t>();
      int post_type = 0;
      if (j.contains("post_type")) post_type = j["post_type"].get<int>();
      std::map<std::string, std::string> carrier;
      if (j.contains("carrier")) carrier = j["carrier"].get<std::map<std::string, std::string>>();

      auto unique_id = handler.ComposeUniqueId(req_id, post_type, carrier);
      res.set_content(json({{"unique_id", unique_id}}).dump(), "application/json");
    } catch (const std::exception &e) {
      res.status = 500;
      res.set_content(json({{"error", e.what()}}).dump(), "application/json");
    }
  });

  LOG(info) << "Starting the unique-id-service HTTP server ...";
  server.listen("0.0.0.0", port);
}
