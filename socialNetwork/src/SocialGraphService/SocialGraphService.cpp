#include <signal.h>

#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "../logger.h"
#include "../tracing.h"
#include "../ClientPool.h"
#include "SocialGraphHandler.h"

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

  SetUpTracer("config/jaeger-config.yml", "social-graph-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["social-graph-service"]["port"];

  int mongodb_conns = config_json["social-graph-mongodb"]["connections"];
  int mongodb_timeout = config_json["social-graph-mongodb"]["timeout_ms"];

  std::string user_addr = config_json["user-service"]["addr"];
  int user_port = config_json["user-service"]["port"];
  int user_conns = config_json["user-service"]["connections"];
  int user_timeout = config_json["user-service"]["timeout_ms"];
  int user_keepalive = config_json["user-service"]["keepalive_ms"];

  int redis_cluster_config_flag = config_json["social-graph-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["social-graph-redis"]["use_replica"];
  mongoc_client_pool_t *mongodb_client_pool =
      init_mongodb_client_pool(config_json, "social-graph", mongodb_conns);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<HttpClientWrapper> user_client_pool(
      "social-graph", user_addr, user_port, 0, user_conns, user_timeout,
      user_keepalive);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "social-graph", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  httplib::Server server;

  if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_cluster_client_pool =
        init_redis_cluster_client_pool(config_json, "social-graph");
    SocialGraphHandler handler(mongodb_client_pool, &redis_cluster_client_pool,
                               &user_client_pool);

    server.Post("/GetFollowers", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followers_id;
        handler.GetFollowers(followers_id, req_id, user_id, carrier);
        res.set_content(json({{"followers_id", followers_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/GetFollowees", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followees_id;
        handler.GetFollowees(followees_id, req_id, user_id, carrier);
        res.set_content(json({{"followees_id", followees_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Follow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Follow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Unfollow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Unfollow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/FollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.FollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/UnfollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.UnfollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/InsertUser", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.InsertUser(req_id, user_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    LOG(info) << "Starting the social-graph-service server with Redis Cluster support...";
    server.listen("0.0.0.0", port);
  } else if (redis_replica_config_flag) {
    Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");

    SocialGraphHandler handler(
        mongodb_client_pool, &redis_replica_client_pool, &redis_primary_client_pool, &user_client_pool);

    server.Post("/GetFollowers", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followers_id;
        handler.GetFollowers(followers_id, req_id, user_id, carrier);
        res.set_content(json({{"followers_id", followers_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/GetFollowees", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followees_id;
        handler.GetFollowees(followees_id, req_id, user_id, carrier);
        res.set_content(json({{"followees_id", followees_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Follow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Follow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Unfollow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Unfollow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/FollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.FollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/UnfollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.UnfollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/InsertUser", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.InsertUser(req_id, user_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    LOG(info) << "Starting the social-graph-service server with Redis replica support";
    server.listen("0.0.0.0", port);
  } else {
    Redis redis_client_pool = init_redis_client_pool(config_json, "social-graph");
    SocialGraphHandler handler(mongodb_client_pool, &redis_client_pool, &user_client_pool);

    server.Post("/GetFollowers", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followers_id;
        handler.GetFollowers(followers_id, req_id, user_id, carrier);
        res.set_content(json({{"followers_id", followers_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/GetFollowees", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        std::vector<int64_t> followees_id;
        handler.GetFollowees(followees_id, req_id, user_id, carrier);
        res.set_content(json({{"followees_id", followees_id}}).dump(), "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Follow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Follow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/Unfollow", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        int64_t followee_id = j["followee_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.Unfollow(req_id, user_id, followee_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/FollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.FollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/UnfollowWithUsername", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        std::string user_name = j["user_name"];
        std::string followee_name = j["followee_name"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.UnfollowWithUsername(req_id, user_name, followee_name, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    server.Post("/InsertUser", [&](const httplib::Request &req, httplib::Response &res) {
      try {
        auto j = json::parse(req.body);
        int64_t req_id = j["req_id"];
        int64_t user_id = j["user_id"];
        std::map<std::string, std::string> carrier = j["carrier"];
        handler.InsertUser(req_id, user_id, carrier);
        res.set_content("{\"status\":\"ok\"}", "application/json");
      } catch (std::exception &e) {
        res.status = 500;
        res.set_content("{\"error\":\"exception\"}", "application/json");
      }
    });

    LOG(info) << "Starting the social-graph-service server ...";
    server.listen("0.0.0.0", port);
  }
}
