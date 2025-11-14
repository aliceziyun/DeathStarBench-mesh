#include <signal.h>

#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include "../ClientPool.h"
#include "../HttpClientWrapper.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../utils_redis.h"
#include "HomeTimelineHandler.h"

using namespace social_network;
using json = nlohmann::json;

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

  // Optional: enable distributed tracing if desired
  // SetUpTracer("config/jaeger-config.yml", "home-timeline-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["home-timeline-service"]["port"];
  int redis_cluster_config_flag = config_json["home-timeline-redis"]["use_cluster"];

  int redis_replica_config_flag = config_json["home-timeline-redis"]["use_replica"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_conns = config_json["post-storage-service"]["connections"];
  int post_storage_timeout = config_json["post-storage-service"]["timeout_ms"];
  int post_storage_keepalive =
      config_json["post-storage-service"]["keepalive_ms"];

  int social_graph_port = config_json["social-graph-service"]["port"];
  std::string social_graph_addr = config_json["social-graph-service"]["addr"];
  int social_graph_conns = config_json["social-graph-service"]["connections"];
  int social_graph_timeout = config_json["social-graph-service"]["timeout_ms"];
  int social_graph_keepalive =
      config_json["social-graph-service"]["keepalive_ms"];

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<HttpClientWrapper> post_storage_client_pool(
    "post-storage-client", post_storage_addr, post_storage_port, 0,
    post_storage_conns, post_storage_timeout, post_storage_keepalive);

  ClientPool<HttpClientWrapper> social_graph_client_pool(
    "social-graph-client", social_graph_addr, social_graph_port, 0,
    social_graph_conns, social_graph_timeout, social_graph_keepalive);


  httplib::Server server;

  if (redis_replica_config_flag) {
    Redis redis_replica_client_pool =
        init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool =
        init_redis_replica_client_pool(config_json, "redis-primary");

    HomeTimelineHandler handler(&redis_replica_client_pool,
                                &redis_primary_client_pool,
                                &post_storage_client_pool,
                                &social_graph_client_pool);

    server.Post("/WriteHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t post_id = j["post_id"];
                    int64_t user_id = j["user_id"];
                    int64_t timestamp = j["timestamp"];
                    auto user_mentions_id =
                        j["user_mentions_id"].get<std::vector<int64_t>>();
                    std::map<std::string, std::string> carrier = j["carrier"];

                    handler.WriteHomeTimeline(req_id, post_id, user_id,
                                              timestamp, user_mentions_id,
                                              carrier);
                    res.set_content("{\"status\":\"ok\"}",
                                    "application/json");
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    server.Post("/ReadHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t user_id = j["user_id"];
                    int start_idx = j["start_idx"];
                    int stop_idx = j["stop_idx"];
                    std::map<std::string, std::string> carrier = j["carrier"];

                    std::vector<Post> posts;
                    handler.ReadHomeTimeline(posts, req_id, user_id, start_idx,
                                             stop_idx, carrier);
                    json out;
                    out["posts"] = json::array();
                    for (auto &p : posts) {
                      json pj = {
                          {"req_id", p.req_id},
                          {"timestamp", p.timestamp},
                          {"post_id", p.post_id},
                          {"post_type", static_cast<int>(p.post_type)},
                          {"text", p.text},
                          {"creator",
                           {{"user_id", p.creator.user_id},
                            {"username", p.creator.username}}},
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
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    LOG(info) << "Starting the home-timeline-service server (replica Redis) ...";
    server.listen("0.0.0.0", port);
  } else if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_cluster_client_pool =
        init_redis_cluster_client_pool(config_json, "home-timeline");

    HomeTimelineHandler handler(&redis_cluster_client_pool,
                                &post_storage_client_pool,
                                &social_graph_client_pool);

    server.Post("/WriteHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t post_id = j["post_id"];
                    int64_t user_id = j["user_id"];
                    int64_t timestamp = j["timestamp"];
                    auto user_mentions_id =
                        j["user_mentions_id"].get<std::vector<int64_t>>();
                    std::map<std::string, std::string> carrier = j["carrier"];

                    handler.WriteHomeTimeline(req_id, post_id, user_id,
                                              timestamp, user_mentions_id,
                                              carrier);
                    res.set_content("{\"status\":\"ok\"}",
                                    "application/json");
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    server.Post("/ReadHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t user_id = j["user_id"];
                    int start_idx = j["start_idx"];
                    int stop_idx = j["stop_idx"];
                    std::map<std::string, std::string> carrier = j["carrier"];

                    std::vector<Post> posts;
                    handler.ReadHomeTimeline(posts, req_id, user_id, start_idx,
                                             stop_idx, carrier);
                    json out;
                    out["posts"] = json::array();
                    for (auto &p : posts) {
                      json pj = {
                          {"req_id", p.req_id},
                          {"timestamp", p.timestamp},
                          {"post_id", p.post_id},
                          {"post_type", static_cast<int>(p.post_type)},
                          {"text", p.text},
                          {"creator",
                           {{"user_id", p.creator.user_id},
                            {"username", p.creator.username}}},
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
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    LOG(info) << "Starting the home-timeline-service server with Redis Cluster support...";
    server.listen("0.0.0.0", port);
  } else {
    Redis redis_client_pool =
        init_redis_client_pool(config_json, "home-timeline");

    HomeTimelineHandler handler(&redis_client_pool, &post_storage_client_pool,
                                &social_graph_client_pool);

    server.Post("/WriteHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t post_id = j["post_id"];
                    int64_t user_id = j["user_id"];
                    int64_t timestamp = j["timestamp"];
                    auto user_mentions_id =
                        j["user_mentions_id"].get<std::vector<int64_t>>();
                    std::map<std::string, std::string> carrier = j["carrier"];

                    handler.WriteHomeTimeline(req_id, post_id, user_id,
                                              timestamp, user_mentions_id,
                                              carrier);
                    res.set_content("{\"status\":\"ok\"}",
                                    "application/json");
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    server.Post("/ReadHomeTimeline",
                [&](const httplib::Request &req, httplib::Response &res) {
                  try {
                    auto j = json::parse(req.body);
                    int64_t req_id = j["req_id"];
                    int64_t user_id = j["user_id"];
                    int start_idx = j["start_idx"];
                    int stop_idx = j["stop_idx"];
                    std::map<std::string, std::string> carrier = j["carrier"];

                    std::vector<Post> posts;
                    handler.ReadHomeTimeline(posts, req_id, user_id, start_idx,
                                             stop_idx, carrier);
                    json out;
                    out["posts"] = json::array();
                    for (auto &p : posts) {
                      json pj = {
                          {"req_id", p.req_id},
                          {"timestamp", p.timestamp},
                          {"post_id", p.post_id},
                          {"post_type", static_cast<int>(p.post_type)},
                          {"text", p.text},
                          {"creator",
                           {{"user_id", p.creator.user_id},
                            {"username", p.creator.username}}},
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
                  } catch (std::exception &e) {
                    res.status = 500;
                    res.set_content("{\"error\":\"exception\"}",
                                    "application/json");
                  }
                });

    LOG(info) << "Starting the home-timeline-service server ...";
    server.listen("0.0.0.0", port);
  }
}