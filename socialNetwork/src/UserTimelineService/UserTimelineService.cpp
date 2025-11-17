// HTTP-based UserTimelineService
#include <signal.h>

#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include "../ClientPool.h"
#include "../HttpClientWrapper.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "UserTimelineHandler.h"

using json = nlohmann::json;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  // Command line options
  namespace po = boost::program_options;
  po::options_description desc("Options");
  desc.add_options()("help", "produce help message")(
      "redis-cluster",
      po::value<bool>()->default_value(false)->implicit_value(true),
      "Enable redis cluster mode");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 0;
  }

  bool redis_cluster_flag = false;
  if (vm.count("redis-cluster")) {
    if (vm["redis-cluster"].as<bool>()) {
      redis_cluster_flag = true;
    }
  }

  // SetUpTracer("config/jaeger-config.yml", "user-timeline-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["user-timeline-service"]["port"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_conns = config_json["post-storage-service"]["connections"];
  int post_storage_timeout = config_json["post-storage-service"]["timeout_ms"];
  int post_storage_keepalive =
      config_json["post-storage-service"]["keepalive_ms"];

  int mongodb_conns = config_json["user-timeline-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-timeline-mongodb"]["timeout_ms"];

  int redis_cluster_config_flag = config_json["user-timeline-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["user-timeline-redis"]["use_replica"];

  auto mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user-timeline", mongodb_conns);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<HttpClientWrapper> post_storage_client_pool(
    "post-storage-client", post_storage_addr, post_storage_port, 0,
    post_storage_conns, post_storage_timeout, post_storage_keepalive);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user-timeline", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_client_pool =
        init_redis_cluster_client_pool(config_json, "user-timeline");
    UserTimelineHandler handler(&redis_client_pool, mongodb_client_pool,
                                &post_storage_client_pool);
    httplib::Server server;
    server.Post("/WriteUserTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"].get<int64_t>();
                    int64_t post_id = j["post_id"].get<int64_t>();
                    int64_t user_id = j["user_id"].get<int64_t>();
                    int64_t timestamp = j["timestamp"].get<int64_t>();
                    std::map<std::string, std::string> carrier;
                    if (j.contains("carrier"))
                      carrier = j["carrier"].get<std::map<std::string, std::string>>();
                    handler.WriteUserTimeline(req_id, post_id, user_id, timestamp,
                                              carrier);
                    res.set_content(json({{"status", "ok"}}).dump(),
                                    "application/json");
                  } catch (const std::exception &e) {
                    res.status = 500;
                    res.set_content(json({{"error", e.what()}}).dump(),
                                    "application/json");
                  }
                });
    server.Post("/ReadUserTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"].get<int64_t>();
                    int64_t user_id = j["user_id"].get<int64_t>();
                    int start = j["start"].get<int>();
                    int stop = j["stop"].get<int>();
                    std::map<std::string, std::string> carrier;
                    if (j.contains("carrier"))
                      carrier = j["carrier"].get<std::map<std::string, std::string>>();
                    std::vector<Post> posts;
                    handler.ReadUserTimeline(posts, req_id, user_id, start, stop,
                                             carrier);
                    json out;
                    out["posts"] = json::array();
                    for (auto &p : posts) {
                      json pj = {
                          {"req_id", p.req_id},
                          {"timestamp", p.timestamp},
                          {"post_id", p.post_id},
                          {"creator",
                           {{"user_id", p.creator.user_id},
                            {"username", p.creator.username}}},
                          {"post_type", static_cast<int>(p.post_type)},
                          {"text", p.text},
                          {"media", json::array()},
                          {"user_mentions", json::array()},
                          {"urls", json::array()}};
                      for (auto &m : p.media) {
                        pj["media"].push_back(
                            {{"media_id", m.media_id},
                             {"media_type", m.media_type}});
                      }
                      for (auto &um : p.user_mentions) {
                        pj["user_mentions"].push_back(
                            {{"user_id", um.user_id},
                             {"username", um.username}});
                      }
                      for (auto &u : p.urls) {
                        pj["urls"].push_back(
                            {{"shortened_url", u.shortened_url},
                             {"expanded_url", u.expanded_url}});
                      }
                      out["posts"].push_back(std::move(pj));
                    }
                    res.set_content(out.dump(), "application/json");
                  } catch (const std::exception &e) {
                    res.status = 500;
                    res.set_content(json({{"error", e.what()}}).dump(),
                                    "application/json");
                  }
                });
    LOG(info) << "Starting the user-timeline-service HTTP server with Redis Cluster support...";
    server.listen("0.0.0.0", port);
  }
  else if (redis_replica_config_flag) {
      Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
      Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
      UserTimelineHandler handler(&redis_replica_client_pool,
                                  &redis_primary_client_pool,
                                  mongodb_client_pool,
                                  &post_storage_client_pool);
      httplib::Server server;
      server.Post("/WriteUserTimeline",
                  [&](const httplib::Request &req, httplib::Response &res) {
                    try {
                      auto j = json::parse(req.body);
                      int64_t req_id = j["req_id"].get<int64_t>();
                      int64_t post_id = j["post_id"].get<int64_t>();
                      int64_t user_id = j["user_id"].get<int64_t>();
                      int64_t timestamp = j["timestamp"].get<int64_t>();
                      std::map<std::string, std::string> carrier;
                      if (j.contains("carrier"))
                        carrier = j["carrier"].get<std::map<std::string, std::string>>();
                      handler.WriteUserTimeline(req_id, post_id, user_id, timestamp,
                                                carrier);
                      res.set_content(json({{"status", "ok"}}).dump(),
                                      "application/json");
                    } catch (const std::exception &e) {
                      res.status = 500;
                      res.set_content(json({{"error", e.what()}}).dump(),
                                      "application/json");
                    }
                  });
      server.Post("/ReadUserTimeline",
                  [&](const httplib::Request &req, httplib::Response &res) {
                    try {
                      auto j = json::parse(req.body);
                      int64_t req_id = j["req_id"].get<int64_t>();
                      int64_t user_id = j["user_id"].get<int64_t>();
                      int start = j["start"].get<int>();
                      int stop = j["stop"].get<int>();
                      std::map<std::string, std::string> carrier;
                      if (j.contains("carrier"))
                        carrier = j["carrier"].get<std::map<std::string, std::string>>();
                      std::vector<Post> posts;
                      handler.ReadUserTimeline(posts, req_id, user_id, start, stop,
                                               carrier);
                      json out;
                      out["posts"] = json::array();
                      for (auto &p : posts) {
                        json pj = {
                            {"req_id", p.req_id},
                            {"timestamp", p.timestamp},
                            {"post_id", p.post_id},
                            {"creator",
                             {{"user_id", p.creator.user_id},
                              {"username", p.creator.username}}},
                            {"post_type", static_cast<int>(p.post_type)},
                            {"text", p.text},
                            {"media", json::array()},
                            {"user_mentions", json::array()},
                            {"urls", json::array()}};
                        for (auto &m : p.media) {
                          pj["media"].push_back(
                              {{"media_id", m.media_id},
                               {"media_type", m.media_type}});
                        }
                        for (auto &um : p.user_mentions) {
                          pj["user_mentions"].push_back(
                              {{"user_id", um.user_id},
                               {"username", um.username}});
                        }
                        for (auto &u : p.urls) {
                          pj["urls"].push_back(
                              {{"shortened_url", u.shortened_url},
                               {"expanded_url", u.expanded_url}});
                        }
                        out["posts"].push_back(std::move(pj));
                      }
                      res.set_content(out.dump(), "application/json");
                    } catch (const std::exception &e) {
                      res.status = 500;
                      res.set_content(json({{"error", e.what()}}).dump(),
                                      "application/json");
                    }
                  });
      LOG(info) << "Starting the user-timeline-service HTTP server with replicated Redis support...";
      server.listen("0.0.0.0", port);

  }
  else {
    Redis redis_client_pool =
        init_redis_client_pool(config_json, "user-timeline");
    UserTimelineHandler handler(&redis_client_pool, mongodb_client_pool,
                                &post_storage_client_pool);
    httplib::Server server;
    server.Post("/WriteUserTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"].get<int64_t>();
                    int64_t post_id = j["post_id"].get<int64_t>();
                    int64_t user_id = j["user_id"].get<int64_t>();
                    int64_t timestamp = j["timestamp"].get<int64_t>();
                    std::map<std::string, std::string> carrier;
                    if (j.contains("carrier"))
                      carrier = j["carrier"].get<std::map<std::string, std::string>>();
                    handler.WriteUserTimeline(req_id, post_id, user_id, timestamp,
                                              carrier);
                    res.set_content(json({{"status", "ok"}}).dump(),
                                    "application/json");
                  } catch (const std::exception &e) {
                    res.status = 500;
                    res.set_content(json({{"error", e.what()}}).dump(),
                                    "application/json");
                  }
                });
    server.Post("/ReadUserTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"].get<int64_t>();
                    int64_t user_id = j["user_id"].get<int64_t>();
                    int start = j["start"].get<int>();
                    int stop = j["stop"].get<int>();
                    std::map<std::string, std::string> carrier;
                    if (j.contains("carrier"))
                      carrier = j["carrier"].get<std::map<std::string, std::string>>();
                    std::vector<Post> posts;
                    handler.ReadUserTimeline(posts, req_id, user_id, start, stop,
                                             carrier);
                    json out;
                    out["posts"] = json::array();
                    for (auto &p : posts) {
                      json pj = {
                          {"req_id", p.req_id},
                          {"timestamp", p.timestamp},
                          {"post_id", p.post_id},
                          {"creator",
                           {{"user_id", p.creator.user_id},
                            {"username", p.creator.username}}},
                          {"post_type", static_cast<int>(p.post_type)},
                          {"text", p.text},
                          {"media", json::array()},
                          {"user_mentions", json::array()},
                          {"urls", json::array()}};
                      for (auto &m : p.media) {
                        pj["media"].push_back(
                            {{"media_id", m.media_id},
                             {"media_type", m.media_type}});
                      }
                      for (auto &um : p.user_mentions) {
                        pj["user_mentions"].push_back(
                            {{"user_id", um.user_id},
                             {"username", um.username}});
                      }
                      for (auto &u : p.urls) {
                        pj["urls"].push_back(
                            {{"shortened_url", u.shortened_url},
                             {"expanded_url", u.expanded_url}});
                      }
                      out["posts"].push_back(std::move(pj));
                    }
                    res.set_content(out.dump(), "application/json");
                  } catch (const std::exception &e) {
                    res.status = 500;
                    res.set_content(json({{"error", e.what()}}).dump(),
                                    "application/json");
                  }
                });
    LOG(info) << "Starting the user-timeline-service HTTP server...";
    server.listen("0.0.0.0", port);
  }
}