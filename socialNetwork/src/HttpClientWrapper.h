#ifndef SOCIALNETWORK_SRC_HTTPCLIENTWRAPPER_H_
#define SOCIALNETWORK_SRC_HTTPCLIENTWRAPPER_H_

#include <string>
#include "httplib.h"

class HttpClientWrapper {
public:
    HttpClientWrapper(const std::string& host, int port, int timeout_ms)
        : cli(host, port) {
        cli.set_connection_timeout(
            timeout_ms / 1000,
            (timeout_ms % 1000) * 1000
        );
    }

    void Connect() {
        // no use, but keep for interface consistency
        long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count();
        _connect_timestamp = now;
    }
    

    nlohmann::json PostJson(const std::string& path,
                            const nlohmann::json& body) {
        auto res = cli.Post(path.c_str(),
                            body.dump(),
                            "application/json");

        if (!res) {
            throw std::runtime_error("HTTP request failed: " + path);
        }
        if (res->status >= 400) {
            throw std::runtime_error("HTTP " + std::to_string(res->status) +
                                     " on " + path);
        }

        return nlohmann::json::parse(res->body);
    }

public:
    long _connect_timestamp;
    long _keepalive_ms;

private:
    httplib::Client cli;
};

#endif  // SOCIALNETWORK_SRC_HTTPCLIENTWRAPPER_H_