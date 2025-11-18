#include <signal.h>
// #include "./httplib.h"

#include "../utils.h"
#include "ComposePostHandler.h"
#include "../ClientPool.h"
#include "../HttpClientWrapper.h"

using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
    signal(SIGINT, sigintHandler);
    init_logger();
    // TODO: should I enable tracing?
    // SetUpTracer("config/jaeger-config.yml", "compose-post-service");

    json config_json;
    if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
    }

    int port = config_json["compose-post-service"]["port"];

    ClientPool<HttpClientWrapper> post_storage_client_pool(
        "post-storage-client",
        config_json["post-storage-service"]["addr"],
        config_json["post-storage-service"]["port"],
        0,
        config_json["post-storage-service"]["connections"],
        config_json["post-storage-service"]["timeout_ms"],
        config_json["post-storage-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> user_timeline_client_pool(
        "user-timeline-client",
        config_json["user-timeline-service"]["addr"],
        config_json["user-timeline-service"]["port"],
        0,
        config_json["user-timeline-service"]["connections"],
        config_json["user-timeline-service"]["timeout_ms"],
        config_json["user-timeline-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> text_client_pool(
        "text-service-client",
        config_json["text-service"]["addr"],
        config_json["text-service"]["port"],
        0,
        config_json["text-service"]["connections"],
        config_json["text-service"]["timeout_ms"],
        config_json["text-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> user_client_pool(
        "user-service-client",
        config_json["user-service"]["addr"],
        config_json["user-service"]["port"],
        0,
        config_json["user-service"]["connections"],
        config_json["user-service"]["timeout_ms"],
        config_json["user-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> media_client_pool(
        "media-service-client",
        config_json["media-service"]["addr"],
        config_json["media-service"]["port"],
        0,
        config_json["media-service"]["connections"],
        config_json["media-service"]["timeout_ms"],
        config_json["media-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> home_timeline_client_pool(
        "home-timeline-service-client",
        config_json["home-timeline-service"]["addr"],
        config_json["home-timeline-service"]["port"],
        0,
        config_json["home-timeline-service"]["connections"],
        config_json["home-timeline-service"]["timeout_ms"],
        config_json["home-timeline-service"]["keepalive_ms"]
    );

    ClientPool<HttpClientWrapper> unique_id_client_pool(
        "unique-id-service-client",
        config_json["unique-id-service"]["addr"],
        config_json["unique-id-service"]["port"],
        0,
        config_json["unique-id-service"]["connections"],
        config_json["unique-id-service"]["timeout_ms"],
        config_json["unique-id-service"]["keepalive_ms"]
    );

//   ClientPool<ThriftClient<PostStorageServiceClient>> post_storage_client_pool(
//       "post-storage-client", post_storage_addr, post_storage_port, 0,
//       post_storage_conns, post_storage_timeout, post_storage_keepalive, config_json);
//   ClientPool<ThriftClient<UserTimelineServiceClient>> user_timeline_client_pool(
//       "user-timeline-client", user_timeline_addr, user_timeline_port, 0,
//       user_timeline_conns, user_timeline_timeout, user_timeline_keepalive, config_json);
//   ClientPool<ThriftClient<TextServiceClient>> text_client_pool(
//       "text-service-client", text_addr, text_port, 0, text_conns, text_timeout,
//       text_keepalive, config_json);
//   ClientPool<ThriftClient<UserServiceClient>> user_client_pool(
//       "user-service-client", user_addr, user_port, 0, user_conns, user_timeout,
//       user_keepalive, config_json);
//   ClientPool<ThriftClient<MediaServiceClient>> media_client_pool(
//       "media-service-client", media_addr, media_port, 0, media_conns,
//       media_timeout, media_keepalive, config_json);
//   ClientPool<ThriftClient<HomeTimelineServiceClient>> home_timeline_client_pool(
//       "home-timeline-service-client", home_timeline_addr, home_timeline_port, 0,
//       home_timeline_conns, home_timeline_timeout, home_timeline_keepalive, config_json);
//   ClientPool<ThriftClient<UniqueIdServiceClient>> unique_id_client_pool(
//       "unique-id-service-client", unique_id_addr, unique_id_port, 0,
//       unique_id_conns, unique_id_timeout, unique_id_keepalive, config_json);

    ComposePostHandler handler(
        &post_storage_client_pool,
        &user_timeline_client_pool,
        &user_client_pool,
        &unique_id_client_pool,
        &media_client_pool,
        &text_client_pool,
        &home_timeline_client_pool
    );

    httplib::Server server;

    server.Post("/ComposePost", [&](const httplib::Request& req, httplib::Response& res) {
        try {
            auto j = json::parse(req.body);

            int64_t req_id = j["req_id"];
            std::string username = j["username"];
            int64_t user_id = j["user_id"];
            std::string text = j["text"];
            auto media_ids = j["media_ids"].get<std::vector<int64_t>>();
            auto media_types = j["media_types"].get<std::vector<std::string>>();
            PostType::type post_type = (PostType::type) j["post_type"].get<int>();
            std::map<std::string, std::string> carrier = j["carrier"];

            handler.ComposePost(req_id, username, user_id, text,
                                media_ids, media_types, post_type, carrier);

            res.set_content("{\"status\":\"ok\"}", "application/json");
        } catch (std::exception &e) {
            res.status = 500;
            LOG(error) << "Error occurred while composing post: " << e.what();
            res.set_content("{\"error\":\"exception\"}", "application/json");
        }
    });

    LOG(info) << "Starting the compose-post-service server ...";
    server.listen("0.0.0.0", port);
}