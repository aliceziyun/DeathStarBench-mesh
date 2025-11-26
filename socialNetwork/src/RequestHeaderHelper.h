#ifndef SOCIAL_NETWORK_MICROSERVICES_REQUESTHEADERHELPER_H_
#define SOCIAL_NETWORK_MICROSERVICES_REQUESTHEADERHELPER_H_

#include <algorithm>
#include <map>
#include <string>

namespace social_network {
// New overload: build header from already extracted x-request-id string (no fabrication).
inline std::map<std::string,std::string> BuildRequestIdHeader(const std::string &x_request_id) {
  if (x_request_id.empty()) return {};
  return {{"x-request-id", x_request_id}};
}

// Extract x-request-id directly from an HTTP header map (e.g., httplib::Headers) case-insensitively.
template <typename HeaderMap>
inline std::string GetXRequestIdFromHeaders(const HeaderMap &headers) {
  for (const auto &kv : headers) {
    std::string key = kv.first;
    std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c){ return std::tolower(c); });
    if (key == "x-request-id" && !kv.second.empty()) {
      return kv.second;
    }
  }
  return "";
}

} // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_REQUESTHEADERHELPER_H_
