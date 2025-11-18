#ifndef SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H

#include <future>
#include <iostream>
#include <regex>
#include <string>
#include <nlohmann/json.hpp>

#include "../ClientPool.h"
#include "../HttpClientWrapper.h"
#include "../logger.h"
#include "../tracing.h"

namespace social_network {

class TextHandler {
 public:
  TextHandler(ClientPool<HttpClientWrapper> *url_pool,
              ClientPool<HttpClientWrapper> *user_mention_pool)
      : _url_client_pool(url_pool), _user_mention_client_pool(user_mention_pool) {}
  ~TextHandler() = default;

  void ComposeText(std::string &updated_text,
                   std::vector<nlohmann::json> &urls_out,
                   std::vector<nlohmann::json> &user_mentions_out,
                   int64_t req_id,
                   const std::string &text,
                   const std::map<std::string, std::string> &carrier);

 private:
  ClientPool<HttpClientWrapper> *_url_client_pool;
  ClientPool<HttpClientWrapper> *_user_mention_client_pool;
};

void TextHandler::ComposeText(
    std::string &updated_text,
    std::vector<nlohmann::json> &urls_out,
    std::vector<nlohmann::json> &user_mentions_out,
    int64_t req_id, const std::string &text,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "compose_text_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::vector<std::string> mention_usernames;
  std::smatch m;
  std::regex e("@[a-zA-Z0-9-_]+");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }

  std::vector<std::string> urls;
  e = "(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)";
  s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }

  auto shortened_urls_future = std::async(std::launch::async, [&]() {
    // auto url_span = opentracing::Tracer::Global()->StartSpan(
    //     "compose_urls_client", {opentracing::ChildOf(&span->context())});
    std::map<std::string, std::string> url_writer_text_map;
    // TextMapWriter url_writer(url_writer_text_map);
    // opentracing::Tracer::Global()->Inject(url_span->context(), url_writer);

    auto url_client = _url_client_pool->Pop();
    if (!url_client) {
      LOG(error) << "Failed to connect to url-shorten-service";
      throw std::runtime_error("url-shorten-service connection failed");
    }
    nlohmann::json req_json = {
        {"req_id", req_id}, {"urls", urls}, {"carrier", url_writer_text_map}};
    nlohmann::json resp_json;
    try {
      resp_json = url_client->PostJson("/ComposeUrls", req_json);
      _url_client_pool->Keepalive(url_client);
    } catch (const std::exception &e) {
      _url_client_pool->Remove(url_client);
      LOG(error) << "Failed HTTP call to url-shorten-service: " << e.what();
      throw;
    }
    // url_span->Finish();
    std::vector<nlohmann::json> result_urls;
    if (resp_json.contains("urls")) {
      for (auto &item : resp_json["urls"]) {
        result_urls.push_back(item);
      }
    }
    return result_urls;
  });

  auto user_mention_future = std::async(std::launch::async, [&]() {
    // auto user_mention_span = opentracing::Tracer::Global()->StartSpan(
    //     "compose_user_mentions_client", {opentracing::ChildOf(&span->context())});
    std::map<std::string, std::string> user_mention_writer_text_map;
    // TextMapWriter user_mention_writer(user_mention_writer_text_map);
    // opentracing::Tracer::Global()->Inject(user_mention_span->context(), user_mention_writer);

    auto user_mention_client = _user_mention_client_pool->Pop();
    if (!user_mention_client) {
      LOG(error) << "Failed to connect to user-mention-service";
      throw std::runtime_error("user-mention-service connection failed");
    }
    nlohmann::json req_json = {{"req_id", req_id},
                                {"usernames", mention_usernames},
                                {"carrier", user_mention_writer_text_map}};
    nlohmann::json resp_json;
    try {
      resp_json = user_mention_client->PostJson("/ComposeUserMentions", req_json);
      _user_mention_client_pool->Keepalive(user_mention_client);
    } catch (const std::exception &e) {
      _user_mention_client_pool->Remove(user_mention_client);
      LOG(error) << "Failed HTTP call to user-mention-service: " << e.what();
      throw;
    }
    // user_mention_span->Finish();
    std::vector<nlohmann::json> result_mentions;
    if (resp_json.contains("user_mentions")) {
      for (auto &item : resp_json["user_mentions"]) {
        result_mentions.push_back(item);
      }
    }
    return result_mentions;
  });

  std::vector<nlohmann::json> target_urls;
  try {
    target_urls = shortened_urls_future.get();
  } catch (...) {
    LOG(error) << "Failed to get shortened urls from url-shorten-service";
    throw;
  }

  std::vector<nlohmann::json> user_mentions;
  try {
    user_mentions = user_mention_future.get();
  } catch (...) {
    LOG(error) << "Failed to upload user mentions to user-mention-service";
    throw;
  }

  std::string updated_text_;
  if (!urls.empty()) {
    s = text;
    int idx = 0;
    while (std::regex_search(s, m, e)) {
      auto url = m.str();
      urls.emplace_back(url);
  updated_text_ += m.prefix().str() + target_urls[idx]["shortened_url"].get<std::string>();
      s = m.suffix().str();
      idx++;
    }
  } else {
    updated_text_ = text;
  }

  user_mentions_out = user_mentions;
  urls_out = target_urls;
  updated_text = updated_text_;
  // span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H
