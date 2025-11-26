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
#include "../RequestHeaderHelper.h"

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
                   const std::string &x_request_id);

 private:
  ClientPool<HttpClientWrapper> *_url_client_pool;
  ClientPool<HttpClientWrapper> *_user_mention_client_pool;
};

void TextHandler::ComposeText(
    std::string &updated_text,
    std::vector<nlohmann::json> &urls_out,
    std::vector<nlohmann::json> &user_mentions_out,
    int64_t req_id,
    const std::string &text,
    const std::string &x_request_id) {
  // Extract mentions
  std::vector<std::string> mention_usernames;
  std::smatch m;
  std::regex mention_regex("@[a-zA-Z0-9-_]+");
  auto s_mentions = text;
  while (std::regex_search(s_mentions, m, mention_regex)) {
    auto user_mention = m.str().substr(1); // drop '@'
    mention_usernames.emplace_back(user_mention);
    s_mentions = m.suffix().str();
  }

  // Extract URLs
  std::vector<std::string> urls;
  std::regex url_regex("(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)");
  auto s_urls = text;
  while (std::regex_search(s_urls, m, url_regex)) {
    urls.emplace_back(m.str());
    s_urls = m.suffix().str();
  }

  // Async shorten URLs
  auto shortened_urls_future = std::async(std::launch::async, [&]() {
    auto url_client = _url_client_pool->Pop();
    if (!url_client) {
      LOG(error) << "Failed to connect to url-shorten-service";
      throw std::runtime_error("url-shorten-service connection failed");
    }
    nlohmann::json req_json = {{"req_id", req_id}, {"urls", urls}}; // no carrier
    nlohmann::json resp_json;
    try {
      resp_json = url_client->PostJson("/ComposeUrls", req_json, BuildRequestIdHeader(x_request_id));
      _url_client_pool->Keepalive(url_client);
    } catch (const std::exception &e) {
      _url_client_pool->Remove(url_client);
      throw;  // propagate
    }
    std::vector<nlohmann::json> result_urls;
    if (resp_json.contains("urls")) {
      for (auto &item : resp_json["urls"]) result_urls.push_back(item);
    }
    return result_urls;
  });

  // Async resolve user mentions
  auto user_mentions_future = std::async(std::launch::async, [&]() {
    auto user_mention_client = _user_mention_client_pool->Pop();
    if (!user_mention_client) {
      LOG(error) << "Failed to connect to user-mention-service";
      throw std::runtime_error("user-mention-service connection failed");
    }
    nlohmann::json req_json = {{"req_id", req_id}, {"usernames", mention_usernames}}; // no carrier
    nlohmann::json resp_json;
    try {
      resp_json = user_mention_client->PostJson("/ComposeUserMentions", req_json, BuildRequestIdHeader(x_request_id));
      _user_mention_client_pool->Keepalive(user_mention_client);
    } catch (const std::exception &e) {
      _user_mention_client_pool->Remove(user_mention_client);
      throw;  // propagate
    }
    std::vector<nlohmann::json> result_mentions;
    if (resp_json.contains("user_mentions")) {
      for (auto &item : resp_json["user_mentions"]) result_mentions.push_back(item);
    }
    return result_mentions;
  });

  // Gather results
  std::vector<nlohmann::json> shortened_urls;
  try {
    shortened_urls = shortened_urls_future.get();
  } catch (...) {
    LOG(error) << "Failed to get shortened urls from url-shorten-service";
    throw;
  }
  std::vector<nlohmann::json> user_mentions;
  try {
    user_mentions = user_mentions_future.get();
  } catch (...) {
    LOG(error) << "Failed to resolve user mentions";
    throw;
  }

  // Replace URLs in text with shortened ones
  std::string result_text;
  if (!urls.empty() && !shortened_urls.empty()) {
    auto working = text;
    std::smatch mm;
    int idx = 0;
    while (std::regex_search(working, mm, url_regex) && idx < (int)shortened_urls.size()) {
      result_text += mm.prefix().str();
      result_text += shortened_urls[idx]["shortened_url"].get<std::string>();
      working = mm.suffix().str();
      idx++;
    }
    result_text += working; // tail
  } else {
    result_text = text;
  }

  updated_text = result_text;
  urls_out = shortened_urls;
  user_mentions_out = user_mentions;
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_TEXTHANDLER_H
