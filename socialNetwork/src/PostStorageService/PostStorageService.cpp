#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"  // brings in httplib Server
#include "PostStorageHandler.h"

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
  // SetUpTracer("config/jaeger-config.yml", "post-storage-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["post-storage-service"]["port"];

  int mongodb_conns = config_json["post-storage-mongodb"]["connections"];
  int mongodb_timeout = config_json["post-storage-mongodb"]["timeout_ms"];

  int memcached_conns = config_json["post-storage-memcached"]["connections"];
  int memcached_timeout = config_json["post-storage-memcached"]["timeout_ms"];

  memcached_client_pool = init_memcached_client_pool(
      config_json, "post-storage", 32, memcached_conns);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "post-storage", mongodb_conns);
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
    r = CreateIndex(mongodb_client, "post", "post_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  PostStorageHandler handler(memcached_client_pool, mongodb_client_pool);
  httplib::Server server;

  // StorePost endpoint
  server.Post("/StorePost", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"];
      std::map<std::string, std::string> carrier = j["carrier"];
      const auto &pj = j["post"];
      Post post;
      post.post_id = pj["post_id"];
      post.timestamp = pj["timestamp"];
      post.req_id = req_id;
      post.text = pj["text"];
      post.post_type = static_cast<PostType::type>((int)pj["post_type"]);
      post.creator.user_id = pj["creator"]["user_id"];
      post.creator.username = pj["creator"]["username"];
      for (auto &m : pj["media"]) {
        Media media;
        media.media_id = m["media_id"];
        media.media_type = m["media_type"];
        post.media.emplace_back(media);
      }
      for (auto &um : pj.value("user_mentions", json::array())) {
        UserMention u;
        u.user_id = um["user_id"];
        u.username = um["username"];
        post.user_mentions.emplace_back(u);
      }
      for (auto &u : pj.value("urls", json::array())) {
        Url url;
        url.shortened_url = u["shortened_url"];
        url.expanded_url = u["expanded_url"];
        post.urls.emplace_back(url);
      }
      handler.StorePost(req_id, post, carrier);
      res.set_content("{\"status\":\"ok\"}", "application/json");
    } catch (std::exception &e) {
      res.status = 500;
      res.set_content("{\"error\":\"exception\"}", "application/json");
    }
  });

  // ReadPost endpoint
  server.Post("/ReadPost", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"];
      int64_t post_id = j["post_id"];
      std::map<std::string, std::string> carrier = j["carrier"];
      Post p;
      handler.ReadPost(p, req_id, post_id, carrier);
      json out = {
          {"post_id", p.post_id},
          {"timestamp", p.timestamp},
          {"req_id", p.req_id},
          {"text", p.text},
          {"post_type", static_cast<int>(p.post_type)},
          {"creator", {{"user_id", p.creator.user_id}, {"username", p.creator.username}}},
          {"media", json::array()},
          {"user_mentions", json::array()},
          {"urls", json::array()}};
      for (auto &m : p.media) {
        out["media"].push_back({{"media_id", m.media_id}, {"media_type", m.media_type}});
      }
      for (auto &um : p.user_mentions) {
        out["user_mentions"].push_back({{"user_id", um.user_id}, {"username", um.username}});
      }
      for (auto &u : p.urls) {
        out["urls"].push_back({{"shortened_url", u.shortened_url}, {"expanded_url", u.expanded_url}});
      }
      res.set_content(out.dump(), "application/json");
    } catch (std::exception &e) {
      res.status = 500;
      res.set_content("{\"error\":\"exception\"}", "application/json");
    }
  });

  // ReadPosts endpoint
  server.Post("/ReadPosts", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"];
      auto post_ids = j["post_ids"].get<std::vector<int64_t>>();
      std::map<std::string, std::string> carrier = j["carrier"];
      std::vector<Post> posts;
      handler.ReadPosts(posts, req_id, post_ids, carrier);
      json out;
      out["posts"] = json::array();
      for (auto &p : posts) {
        json pj = {
            {"post_id", p.post_id},
            {"timestamp", p.timestamp},
            {"req_id", p.req_id},
            {"text", p.text},
            {"post_type", static_cast<int>(p.post_type)},
            {"creator", {{"user_id", p.creator.user_id}, {"username", p.creator.username}}},
            {"media", json::array()},
            {"user_mentions", json::array()},
            {"urls", json::array()}};
        for (auto &m : p.media) {
          pj["media"].push_back({{"media_id", m.media_id}, {"media_type", m.media_type}});
        }
        for (auto &um : p.user_mentions) {
          pj["user_mentions"].push_back({{"user_id", um.user_id}, {"username", um.username}});
        }
        for (auto &u : p.urls) {
          pj["urls"].push_back({{"shortened_url", u.shortened_url}, {"expanded_url", u.expanded_url}});
        }
        out["posts"].push_back(std::move(pj));
      }
      res.set_content(out.dump(), "application/json");
    } catch (std::exception &e) {
      res.status = 500;
      res.set_content("{\"error\":\"exception\"}", "application/json");
    }
  });

  LOG(info) << "Starting the post-storage-service server...";
  server.listen("0.0.0.0", port);
}
