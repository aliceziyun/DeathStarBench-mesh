#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"  // for httplib::Server
#include "UserMentionHandler.h"

using namespace social_network;
using json = nlohmann::json;

static memcached_pool_st* memcached_client_pool;
static mongoc_client_pool_t* mongodb_client_pool;

void sigintHandler(int sig) {
  if (memcached_client_pool != nullptr) {
    memcached_pool_destroy(memcached_client_pool);
  }
  if (mongodb_client_pool != nullptr) {
    mongoc_client_pool_destroy(mongodb_client_pool);
  }
  exit(EXIT_SUCCESS);
}

int main(int argc, char* argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  // SetUpTracer("config/jaeger-config.yml", "user-mention-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["user-mention-service"]["port"];

  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];

  int memcached_conns = config_json["user-memcached"]["connections"];
  int memcached_timeout = config_json["user-memcached"]["timeout_ms"];

  memcached_client_pool =
      init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  UserMentionHandler handler(memcached_client_pool, mongodb_client_pool);
  httplib::Server server;

  server.Post("/ComposeUserMentions", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"].get<int64_t>();
      std::vector<std::string> usernames = j["usernames"].get<std::vector<std::string>>();
      std::map<std::string, std::string> carrier;
      if (j.contains("carrier")) carrier = j["carrier"].get<std::map<std::string, std::string>>();

      std::vector<UserMention> out;
      handler.ComposeUserMentions(out, req_id, usernames, carrier);
      json resp = json::object();
      resp["user_mentions"] = json::array();
      for (auto &um : out) {
        resp["user_mentions"].push_back({{"user_id", um.user_id}, {"username", um.username}});
      }
      res.set_content(resp.dump(), "application/json");
    } catch (const std::exception &e) {
      res.status = 500;
      res.set_content(json({{"error", e.what()}}).dump(), "application/json");
    }
  });

  LOG(info) << "Starting the user-mention-service HTTP server...";
  server.listen("0.0.0.0", port);
}