#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_

#include <bson/bson.h>
#include <mongoc.h>
#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>

#include "../ClientPool.h"
#include "../HttpClientWrapper.h"
#include "../logger.h"
#include "../tracing.h"
#include "../social_network_types.h"

using namespace sw::redis;

namespace social_network {

class UserTimelineHandler {
 public:
  UserTimelineHandler(Redis *, mongoc_client_pool_t *,
                      ClientPool<HttpClientWrapper> *);

  UserTimelineHandler(Redis *, Redis *, mongoc_client_pool_t *,
      ClientPool<HttpClientWrapper> *);

  UserTimelineHandler(RedisCluster *, mongoc_client_pool_t *,
                      ClientPool<HttpClientWrapper> *);
  ~UserTimelineHandler() = default;

  bool IsRedisReplicationEnabled();

  void WriteUserTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier);

  void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
            const std::map<std::string, std::string> &);

 private:
  Redis *_redis_client_pool;
  Redis *_redis_replica_pool;
  Redis *_redis_primary_pool;
  RedisCluster *_redis_cluster_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<HttpClientWrapper> *_post_client_pool;
};

UserTimelineHandler::UserTimelineHandler(
    Redis *redis_pool, mongoc_client_pool_t *mongodb_pool,
    ClientPool<HttpClientWrapper> *post_client_pool) {
  _redis_client_pool = redis_pool;
  _redis_replica_pool = nullptr;
  _redis_primary_pool = nullptr;
  _redis_cluster_client_pool = nullptr;
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

UserTimelineHandler::UserTimelineHandler(
    Redis* redis_replica_pool, Redis* redis_primary_pool, mongoc_client_pool_t* mongodb_pool,
    ClientPool<HttpClientWrapper>* post_client_pool) {
    _redis_client_pool = nullptr;
    _redis_replica_pool = redis_replica_pool;
    _redis_primary_pool = redis_primary_pool;
    _redis_cluster_client_pool = nullptr;
    _mongodb_client_pool = mongodb_pool;
    _post_client_pool = post_client_pool;
}

UserTimelineHandler::UserTimelineHandler(
    RedisCluster *redis_pool, mongoc_client_pool_t *mongodb_pool,
    ClientPool<HttpClientWrapper> *post_client_pool) {
  _redis_cluster_client_pool = redis_pool;
  _redis_replica_pool = nullptr;
  _redis_primary_pool = nullptr;
  _redis_client_pool = nullptr;
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

bool UserTimelineHandler::IsRedisReplicationEnabled() {
    return (_redis_primary_pool || _redis_replica_pool);
}

void UserTimelineHandler::WriteUserTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client =
      mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    throw std::runtime_error("Failed to pop a client from MongoDB pool");
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "user-timeline", "user-timeline");
  if (!collection) {
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw std::runtime_error(
        "Failed to create collection user-timeline from MongoDB");
  }
  bson_t *query = bson_new();

  BSON_APPEND_INT64(query, "user_id", user_id);
  bson_t *update =
      BCON_NEW("$push", "{", "posts", "{", "$each", "[", "{", "post_id",
               BCON_INT64(post_id), "timestamp", BCON_INT64(timestamp), "}",
               "]", "$position", BCON_INT32(0), "}", "}");
  bson_error_t error;
  bson_t reply;
  auto update_span = opentracing::Tracer::Global()->StartSpan(
      "write_user_timeline_mongo_insert_client",
      {opentracing::ChildOf(&span->context())});
  bool updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                   update, nullptr, false, true,
                                                   true, &reply, &error);
  update_span->Finish();

  if (!updated) {
    // update the newly inserted document (upsert: false)
    updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                update, nullptr, false, false,
                                                true, &reply, &error);
    if (!updated) {
      LOG(error) << "Failed to update user-timeline for user " << user_id
                 << " to MongoDB: " << error.message;
      bson_destroy(update);
      bson_destroy(query);
      bson_destroy(&reply);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw std::runtime_error(error.message);
    }
  }

  bson_destroy(update);
  bson_destroy(&reply);
  bson_destroy(query);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  // Update user's timeline in redis
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "write_user_timeline_redis_update_client",
      {opentracing::ChildOf(&span->context())});
  try {
    if (_redis_client_pool)
      _redis_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                              timestamp, UpdateType::NOT_EXIST);
    else if (IsRedisReplicationEnabled()) {
        _redis_primary_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                              timestamp, UpdateType::NOT_EXIST);
    }
    else
      _redis_cluster_client_pool->zadd(std::to_string(user_id), std::to_string(post_id),
                              timestamp, UpdateType::NOT_EXIST);

  } catch (const Error &err) {
    LOG(error) << err.what();
    throw;
  }
  redis_span->Finish();
  span->Finish();
}

void UserTimelineHandler::ReadUserTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start,
    int stop, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop <= start || start < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool)
      _redis_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                  std::back_inserter(post_ids_str));
    else if (IsRedisReplicationEnabled()) {
        _redis_replica_pool->zrevrange(std::to_string(user_id), start, stop - 1,
            std::back_inserter(post_ids_str));
    }
    else
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start, stop - 1,
                                  std::back_inserter(post_ids_str));
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }

  // find in mongodb
  int mongo_start = start + post_ids.size();
  std::unordered_map<std::string, double> redis_update_map;
  if (mongo_start < stop) {
    // Instead find post_ids from mongodb
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client) {
      throw std::runtime_error("Failed to pop a client from MongoDB pool");
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "user-timeline", "user-timeline");
    if (!collection) {
      throw std::runtime_error(
          "Failed to create collection user-timeline from MongoDB");
    }

    bson_t *query = BCON_NEW("user_id", BCON_INT64(user_id));
    bson_t *opts = BCON_NEW("projection", "{", "posts", "{", "$slice", "[",
                            BCON_INT32(0), BCON_INT32(stop), "]", "}", "}");

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "user_timeline_mongo_find_client",
        {opentracing::ChildOf(&span->context())});
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(collection, query, opts, nullptr);
    find_span->Finish();
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t post_id_child;
      bson_iter_t timestamp_child;
      int idx = 0;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);
      while (bson_iter_find_descendant(
                 &iter_0, ("posts." + std::to_string(idx) + ".post_id").c_str(),
                 &post_id_child) &&
             BSON_ITER_HOLDS_INT64(&post_id_child) &&
             bson_iter_find_descendant(
                 &iter_1,
                 ("posts." + std::to_string(idx) + ".timestamp").c_str(),
                 &timestamp_child) &&
             BSON_ITER_HOLDS_INT64(&timestamp_child)) {
        auto curr_post_id = bson_iter_int64(&post_id_child);
        auto curr_timestamp = bson_iter_int64(&timestamp_child);
        if (idx >= mongo_start) {
          //In mixed workload condition, post may composed between redis and mongo read
          //mongodb index will shift and duplicate post_id occurs
          if ( std::find(post_ids.begin(), post_ids.end(), curr_post_id) == post_ids.end() ) {
            post_ids.emplace_back(curr_post_id);
          }
        }
        redis_update_map.insert(std::make_pair(std::to_string(curr_post_id),
                                               (double)curr_timestamp));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        idx++;
      }
    }
    bson_destroy(opts);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  std::future<std::vector<Post>> post_future =
      std::async(std::launch::async, [&]() {
        auto post_client = _post_client_pool->Pop();
        if (!post_client) {
          LOG(error) << "Failed to connect to post-storage-service";
          throw std::runtime_error(
              "Failed to connect to post-storage-service");
        }
        std::vector<Post> _return_posts;
        try {
          nlohmann::json req_json = {
              {"req_id", req_id}, {"post_ids", post_ids},
              {"carrier", writer_text_map}};
          auto res = post_client->PostJson("/ReadPosts", req_json);
          for (auto &item : res["posts"]) {
            Post p;
            p.req_id = item["req_id"];
            p.timestamp = item["timestamp"];
            p.post_id = item["post_id"];
            p.creator.user_id = item["creator"]["user_id"];
            p.creator.username = item["creator"]["username"];
            p.post_type = static_cast<PostType::type>((int)item["post_type"]);
            p.text = item["text"];
            for (auto &m : item["media"]) {
              Media media;
              media.media_id = m["media_id"];
              media.media_type = m["media_type"];
              p.media.emplace_back(media);
            }
            for (auto &um : item["user_mentions"]) {
              UserMention u;
              u.user_id = um["user_id"];
              u.username = um["username"];
              p.user_mentions.emplace_back(u);
            }
            for (auto &u : item["urls"]) {
              Url url;
              url.shortened_url = u["shortened_url"];
              url.expanded_url = u["expanded_url"];
              p.urls.emplace_back(url);
            }
            _return_posts.emplace_back(std::move(p));
          }
        } catch (...) {
          _post_client_pool->Remove(post_client);
          LOG(error) << "Failed to read posts from post-storage-service";
          throw;
        }
        _post_client_pool->Keepalive(post_client);
        return _return_posts;
      });

  if (redis_update_map.size() > 0) {
    auto redis_update_span = opentracing::Tracer::Global()->StartSpan(
        "user_timeline_redis_update_client",
        {opentracing::ChildOf(&span->context())});
    try {
      if (_redis_client_pool)
        _redis_client_pool->zadd(std::to_string(user_id),
                               redis_update_map.begin(),
                               redis_update_map.end());
      else if (IsRedisReplicationEnabled()) {
          _redis_primary_pool->zadd(std::to_string(user_id),
              redis_update_map.begin(),
              redis_update_map.end());
      }
      else
        _redis_cluster_client_pool->zadd(std::to_string(user_id),
                               redis_update_map.begin(),
                               redis_update_map.end());

    } catch (const Error &err) {
      LOG(error) << err.what();
      throw;
    }
    redis_update_span->Finish();
  }

  try {
    _return = post_future.get();
  } catch (...) {
    LOG(error) << "Failed to get post from post-storage-service";
    throw;
  }
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
