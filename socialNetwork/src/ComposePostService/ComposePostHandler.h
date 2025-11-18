#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_

#include <chrono>
#include <future>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "../social_network_types.h"
#include "../ClientPool.h"
#include "../HttpClientWrapper.h"
#include "../logger.h"
#include "../tracing.h"

namespace social_network {
using json = nlohmann::json;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class ComposePostHandler {
 public:
  ComposePostHandler(ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *,
                     ClientPool<HttpClientWrapper> *);
  ~ComposePostHandler() = default;

  void ComposePost(int64_t req_id, const std::string &username, int64_t user_id,
                   const std::string &text,
                   const std::vector<int64_t> &media_ids,
                   const std::vector<std::string> &media_types,
                   PostType::type post_type,
                   const std::map<std::string, std::string> &carrier);

 private:
  ClientPool<HttpClientWrapper> *_post_storage_client_pool;
  ClientPool<HttpClientWrapper> *_user_timeline_client_pool;

  ClientPool<HttpClientWrapper> *_user_service_client_pool;
  ClientPool<HttpClientWrapper> *_unique_id_service_client_pool;
  ClientPool<HttpClientWrapper> *_media_service_client_pool;
  ClientPool<HttpClientWrapper> *_text_service_client_pool;
  ClientPool<HttpClientWrapper> *_home_timeline_client_pool;

  void _UploadUserTimelineHelper(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> &carrier);

  void _UploadPostHelper(int64_t req_id, const Post &post,
                         const std::map<std::string, std::string> &carrier);

  void _UploadHomeTimelineHelper(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::vector<int64_t> &user_mentions_id,
      const std::map<std::string, std::string> &carrier);

  Creator _ComposeCreaterHelper(
      int64_t req_id, int64_t user_id, const std::string &username,
      const std::map<std::string, std::string> &carrier);
  TextServiceReturn _ComposeTextHelper(
      int64_t req_id, const std::string &text,
      const std::map<std::string, std::string> &carrier);
  std::vector<Media> _ComposeMediaHelper(
      int64_t req_id, const std::vector<std::string> &media_types,
      const std::vector<int64_t> &media_ids,
      const std::map<std::string, std::string> &carrier);
  int64_t _ComposeUniqueIdHelper(
      int64_t req_id, PostType::type post_type,
      const std::map<std::string, std::string> &carrier);
};

ComposePostHandler::ComposePostHandler(
    ClientPool<HttpClientWrapper>
        *post_storage_client_pool,
    ClientPool<HttpClientWrapper>
        *user_timeline_client_pool,
    ClientPool<HttpClientWrapper> *user_service_client_pool,
    ClientPool<HttpClientWrapper>
        *unique_id_service_client_pool,
    ClientPool<HttpClientWrapper> *media_service_client_pool,
    ClientPool<HttpClientWrapper> *text_service_client_pool,
    ClientPool<HttpClientWrapper>
        *home_timeline_client_pool) {
  _post_storage_client_pool = post_storage_client_pool;
  _user_timeline_client_pool = user_timeline_client_pool;
  _user_service_client_pool = user_service_client_pool;
  _unique_id_service_client_pool = unique_id_service_client_pool;
  _media_service_client_pool = media_service_client_pool;
  _text_service_client_pool = text_service_client_pool;
  _home_timeline_client_pool = home_timeline_client_pool;
}

Creator ComposePostHandler::_ComposeCreaterHelper(
    int64_t req_id, int64_t user_id, const std::string &username,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "compose_creator_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_client = _user_service_client_pool->Pop();
  if (!user_client) {
    LOG(error) << "Failed to connect to user-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to user-service");
  }

  Creator _return_creator;
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"user_id", user_id},
      {"username", username},
      {"carrier", writer_text_map}
    };

    auto res = user_client->PostJson("/ComposeCreatorWithUserId", req_json);
    _return_creator.user_id = res["user_id"];
    _return_creator.username = res["username"];
  } catch (...) {
    LOG(error) << "Failed to send compose-creator to user-service";
    _user_service_client_pool->Remove(user_client);
    // span->Finish();
    throw;
  }
  _user_service_client_pool->Keepalive(user_client);
  // span->Finish();
  return _return_creator;
}

TextServiceReturn ComposePostHandler::_ComposeTextHelper(
    int64_t req_id, const std::string &text,
    const std::map<std::string, std::string> &carrier) {
  // TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "compose_text_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto text_client = _text_service_client_pool->Pop();
  if (!text_client) {
    LOG(error) << "Failed to connect to text-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to text-service");
  }

  TextServiceReturn _return_text;
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"text", text},
      {"carrier", writer_text_map}
    };

    auto res = text_client->PostJson("/ComposeText", req_json);
    _return_text.text = res["text"];
    for (auto &item : res["user_mentions"]) {
      UserMention user_mention;
      user_mention.user_id = item["user_id"];
      user_mention.username = item["username"];
      _return_text.user_mentions.emplace_back(user_mention);
    }
    for (auto &item : res["urls"]) {
      Url url;
      url.shortened_url = item["shortened_url"];
      url.expanded_url = item["expanded_url"];
      _return_text.urls.emplace_back(url);
    }
  } catch (...) {
    LOG(error) << "Failed to send compose-text to text-service";
    _text_service_client_pool->Remove(text_client);
    // span->Finish();
    throw;
  }
  _text_service_client_pool->Keepalive(text_client);
  // span->Finish();
  return _return_text;
}

std::vector<Media> ComposePostHandler::_ComposeMediaHelper(
    int64_t req_id, const std::vector<std::string> &media_types,
    const std::vector<int64_t> &media_ids,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // // auto span = opentracing::Tracer::Global()->StartSpan(
      // "compose_media_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto media_client = _media_service_client_pool->Pop();
  if (!media_client) {
    LOG(error) << "Failed to connect to media-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to media-service");
  }

  std::vector<Media> _return_media;
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"media_types", media_types},
      {"media_ids", media_ids},
      {"carrier", writer_text_map}
    };

    auto res = media_client->PostJson("/ComposeMedia", req_json);
    for (auto &item : res["media"]) {
      Media media;
      media.media_id = item["media_id"];
      media.media_type = item["media_type"];
      _return_media.emplace_back(media);
    }
  } catch (...) {
    LOG(error) << "Failed to send compose-media to media-service";
    _media_service_client_pool->Remove(media_client);
    // span->Finish();
    throw;
  }
  _media_service_client_pool->Keepalive(media_client);
  // span->Finish();
  return _return_media;
}

int64_t ComposePostHandler::_ComposeUniqueIdHelper(
    int64_t req_id, const PostType::type post_type,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "compose_unique_id_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto unique_id_client = _unique_id_service_client_pool->Pop();
  if (!unique_id_client) {
    LOG(error) << "Failed to connect to unique_id-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to unique_id-service");
  }

  int64_t _return_unique_id;
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"post_type", static_cast<int>(post_type)},
      {"carrier", writer_text_map}
    };

    auto res = unique_id_client->PostJson("/ComposeUniqueId", req_json);
    _return_unique_id = res["unique_id"];
  } catch (...) {
    LOG(error) << "Failed to send compose-unique_id to unique_id-service";
    _unique_id_service_client_pool->Remove(unique_id_client);
    // span->Finish();
    throw;
  }
  _unique_id_service_client_pool->Keepalive(unique_id_client);
  // span->Finish();
  return _return_unique_id;
}

void ComposePostHandler::_UploadPostHelper(
    int64_t req_id, const Post &post,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "store_post_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto post_storage_client = _post_storage_client_pool->Pop();
  if (!post_storage_client) {
    LOG(error) << "Failed to connect to post-storage-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to post-storage-service");
  }
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"post", {
          {"post_id", post.post_id},
          {"creator", {
              {"user_id", post.creator.user_id},
              {"username", post.creator.username}
          }},
          {"text", post.text},
          {"user_mentions", nlohmann::json::array()},
          {"media", nlohmann::json::array()},
          {"urls", nlohmann::json::array()},
          {"timestamp", post.timestamp},
          {"post_type", static_cast<int>(post.post_type)}
      }},
      {"carrier", writer_text_map}
    };
    auto res = post_storage_client->PostJson("/StorePost", req_json);
  } catch (...) {
    _post_storage_client_pool->Remove(post_storage_client);
    LOG(error) << "Failed to store post to post-storage-service";
    throw;
  }
  _post_storage_client_pool->Keepalive(post_storage_client);

  // span->Finish();
}

void ComposePostHandler::_UploadUserTimelineHelper(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "write_user_timeline_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto user_timeline_client = _user_timeline_client_pool->Pop();
  if (!user_timeline_client) {
    LOG(error) << "Failed to connect to user-timeline-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to user-timeline-service");
  }
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"post_id", post_id},
      {"user_id", user_id},
      {"timestamp", timestamp},
      {"carrier", writer_text_map}
    };
    user_timeline_client->PostJson("/WriteUserTimeline", req_json);
  } catch (...) {
    _user_timeline_client_pool->Remove(user_timeline_client);
    throw;
  }
  _user_timeline_client_pool->Keepalive(user_timeline_client);

  // span->Finish();
}

void ComposePostHandler::_UploadHomeTimelineHelper(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "write_home_timeline_client", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto home_timeline_client = _home_timeline_client_pool->Pop();
  if (!home_timeline_client) {
    LOG(error) << "Failed to connect to home-timeline-service";
    // span->Finish();
    throw std::runtime_error("Failed to connect to home-timeline-service");
  }
  try {
    nlohmann::json req_json = {
      {"req_id", req_id},
      {"post_id", post_id},
      {"user_id", user_id},
      {"timestamp", timestamp},
      {"user_mentions_id", user_mentions_id},
      {"carrier", writer_text_map}
    };
    home_timeline_client->PostJson("/WriteHomeTimeline", req_json);
  } catch (...) {
    _home_timeline_client_pool->Remove(home_timeline_client);
    LOG(error) << "Failed to write home timeline to home-timeline-service";
    throw;
  }
  _home_timeline_client_pool->Keepalive(home_timeline_client);

  // span->Finish();
}

void ComposePostHandler::ComposePost(
    const int64_t req_id, const std::string &username, int64_t user_id,
    const std::string &text, const std::vector<int64_t> &media_ids,
    const std::vector<std::string> &media_types, const PostType::type post_type,
    const std::map<std::string, std::string> &carrier) {
  TextMapReader reader(carrier);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
      // "compose_post_server", {opentracing::ChildOf(parent_span->get())});
  std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto text_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeTextHelper,
                 this, req_id, text, writer_text_map);
  auto creator_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeCreaterHelper,
                 this, req_id, user_id, username, writer_text_map);
  auto media_future =
      std::async(std::launch::async, &ComposePostHandler::_ComposeMediaHelper,
                 this, req_id, media_types, media_ids, writer_text_map);
  auto unique_id_future = std::async(
      std::launch::async, &ComposePostHandler::_ComposeUniqueIdHelper, this,
      req_id, post_type, writer_text_map);

  Post post;
  auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();
  post.timestamp = timestamp;

  // try
  // {
  post.post_id = unique_id_future.get();
  post.creator = creator_future.get();
  post.media = media_future.get();
  auto text_return = text_future.get();
  post.text = text_return.text;
  post.urls = text_return.urls;
  post.user_mentions = text_return.user_mentions;
  post.req_id = req_id;
  post.post_type = post_type;
  // }
  // catch (...)
  // {
  //   throw;
  // }

  std::vector<int64_t> user_mention_ids;
  for (auto &item : post.user_mentions) {
    user_mention_ids.emplace_back(item.user_id);
  }

  //In mixed workloed condition, need to make sure _UploadPostHelper execute
  //Before _UploadUserTimelineHelper and _UploadHomeTimelineHelper.
  //Change _UploadUserTimelineHelper and _UploadHomeTimelineHelper to deferred.
  //To let them start execute after post_future.get() return.
  auto post_future =
      std::async(std::launch::async, &ComposePostHandler::_UploadPostHelper,
                 this, req_id, post, writer_text_map);
  auto user_timeline_future = std::async(
      std::launch::deferred, &ComposePostHandler::_UploadUserTimelineHelper, this,
      req_id, post.post_id, user_id, timestamp, writer_text_map);
  auto home_timeline_future = std::async(
      std::launch::deferred, &ComposePostHandler::_UploadHomeTimelineHelper, this,
      req_id, post.post_id, user_id, timestamp, user_mention_ids,
      writer_text_map);

  // try
  // {
  post_future.get();
  user_timeline_future.get();
  home_timeline_future.get();
  // }
  // catch (...)
  // {
  //   throw;
  // }
  // span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
