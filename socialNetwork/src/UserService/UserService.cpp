// HTTP-based UserService (no Thrift)
#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"
#include "UserHandler.h"

using json = nlohmann::json;
using namespace social_network;

static memcached_pool_st *memcached_client_pool = nullptr;
static mongoc_client_pool_t *mongodb_client_pool = nullptr;

void sigintHandler(int sig) {
  if (memcached_client_pool) memcached_pool_destroy(memcached_client_pool);
  if (mongodb_client_pool) mongoc_client_pool_destroy(mongodb_client_pool);
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "user-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  std::string secret = config_json["secret"];
  int port = config_json["user-service"]["port"];

  // downstream: social-graph (HTTP)
  ClientPool<HttpClientWrapper> social_graph_client_pool(
      "social-graph",
      config_json["social-graph-service"]["addr"],
      config_json["social-graph-service"]["port"],
      0,
      config_json["social-graph-service"]["connections"],
      config_json["social-graph-service"]["timeout_ms"],
      config_json["social-graph-service"]["keepalive_ms"]);

  // stores
  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int memcached_conns = config_json["user-memcached"]["connections"];

  memcached_client_pool =
      init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  // ensure index
  {
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(mongodb_client_pool);
    if (!mongodb_client) {
      LOG(fatal) << "Failed to pop mongoc client";
      return EXIT_FAILURE;
    }
    bool r = false;
    while (!r) {
      r = CreateIndex(mongodb_client, "user", "user_id", true);
      if (!r) {
        LOG(error) << "Failed to create mongodb index, try again";
        sleep(1);
      }
    }
    mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  }

  std::string netif = config_json["user-service"]["netif"];
  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex thread_lock;
  UserHandler handler(&thread_lock, machine_id, secret, memcached_client_pool,
                      mongodb_client_pool, &social_graph_client_pool);

  httplib::Server server;

  // POST /ComposeCreatorWithUserId
  server.Post("/ComposeCreatorWithUserId",
              [&](const httplib::Request &req, httplib::Response &res) {
                try {
                  auto j = json::parse(req.body);
                  int64_t req_id = j["req_id"].get<int64_t>();
                  int64_t user_id = j["user_id"].get<int64_t>();
                  std::string username = j["username"].get<std::string>();
                  std::map<std::string, std::string> carrier;
                  if (j.contains("carrier"))
                    carrier =
                        j["carrier"].get<std::map<std::string, std::string>>();

                  Creator out;
                  handler.ComposeCreatorWithUserId(out, req_id, user_id,
                                                   username, carrier);
                  json resp = {{"user_id", out.user_id},
                               {"username", out.username}};
                  res.set_content(resp.dump(), "application/json");
                } catch (const std::exception &e) {
                  res.status = 500;
                  res.set_content(json({{"error", e.what()}}).dump(),
                                  "application/json");
                }
              });

  // POST /GetUserId
  server.Post("/GetUserId", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"].get<int64_t>();
      std::string username;
      if (j.contains("user_name"))
        username = j["user_name"].get<std::string>();
      else
        username = j["username"].get<std::string>();
      std::map<std::string, std::string> carrier;
      if (j.contains("carrier"))
        carrier = j["carrier"].get<std::map<std::string, std::string>>();

      auto uid = handler.GetUserId(req_id, username, carrier);
      res.set_content(json({{"user_id", uid}}).dump(), "application/json");
    } catch (const std::exception &e) {
      res.status = 500;
      res.set_content(json({{"error", e.what()}}).dump(), "application/json");
    }
  });

  // Optional: user registration endpoints
  server.Post("/RegisterUser", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"].get<int64_t>();
      auto first_name = j["first_name"].get<std::string>();
      auto last_name = j["last_name"].get<std::string>();
      auto username = j["username"].get<std::string>();
      auto password = j["password"].get<std::string>();
      std::map<std::string, std::string> carrier;
      if (j.contains("carrier"))
        carrier = j["carrier"].get<std::map<std::string, std::string>>();
      handler.RegisterUser(req_id, first_name, last_name, username, password,
                           carrier);
      res.set_content(json({{"status", "ok"}}).dump(), "application/json");
    } catch (const std::exception &e) {
      res.status = 500;
      res.set_content(json({{"error", e.what()}}).dump(), "application/json");
    }
  });

  server.Post("/RegisterUserWithId",
              [&](const httplib::Request &req, httplib::Response &res) {
                try {
                  auto j = json::parse(req.body);
                  int64_t req_id = j["req_id"].get<int64_t>();
                  auto first_name = j["first_name"].get<std::string>();
                  auto last_name = j["last_name"].get<std::string>();
                  auto username = j["username"].get<std::string>();
                  auto password = j["password"].get<std::string>();
                  int64_t user_id = j["user_id"].get<int64_t>();
                  std::map<std::string, std::string> carrier;
                  if (j.contains("carrier"))
                    carrier = j["carrier"].get<std::map<std::string, std::string>>();
                  handler.RegisterUserWithId(req_id, first_name, last_name,
                                             username, password, user_id,
                                             carrier);
                  res.set_content(json({{"status", "ok"}}).dump(),
                                  "application/json");
                } catch (const std::exception &e) {
                  res.status = 500;
                  res.set_content(json({{"error", e.what()}}).dump(),
                                  "application/json");
                }
              });

  LOG(info) << "Starting the user-service HTTP server ...";
  server.listen("0.0.0.0", port);
}