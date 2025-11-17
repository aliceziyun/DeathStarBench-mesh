#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"  // for httplib::Server
#include "UrlShortenHandler.h"

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
  SetUpTracer("config/jaeger-config.yml", "url-shorten-service");
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }
  int port = config_json["url-shorten-service"]["port"];

  int mongodb_conns = config_json["url-shorten-mongodb"]["connections"];
  int mongodb_timeout = config_json["url-shorten-mongodb"]["timeout_ms"];

  int memcached_conns = config_json["url-shorten-memcached"]["connections"];
  int memcached_timeout = config_json["url-shorten-memcached"]["timeout_ms"];

  memcached_client_pool = init_memcached_client_pool(config_json, "url-shorten",
                                                     32, memcached_conns);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "url-shorten", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  mongoc_client_t* mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "url-shorten", "shortened_url", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  std::mutex thread_lock;
  UrlShortenHandler handler(memcached_client_pool, mongodb_client_pool, &thread_lock);
  httplib::Server server;

  server.Post("/ComposeUrls", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"].get<int64_t>();
      std::vector<std::string> urls = j["urls"].get<std::vector<std::string>>();
      std::map<std::string, std::string> carrier;
      if (j.contains("carrier")) carrier = j["carrier"].get<std::map<std::string, std::string>>();

      std::vector<Url> out;
      handler.ComposeUrls(out, req_id, urls, carrier);
      json resp = json::object();
      resp["urls"] = json::array();
      for (auto &u : out) {
        resp["urls"].push_back({{"shortened_url", u.shortened_url}, {"expanded_url", u.expanded_url}});
      }
      res.set_content(resp.dump(), "application/json");
    } catch (const std::exception &e) {
      res.status = 500;
      res.set_content(json({{"error", e.what()}}).dump(), "application/json");
    }
  });

  LOG(info) << "Starting the url-shorten-service HTTP server...";
  server.listen("0.0.0.0", port);
}