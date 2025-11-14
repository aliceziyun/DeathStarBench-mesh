#include <signal.h>

#include <nlohmann/json.hpp>

#include "../utils.h"
#include "../logger.h"
#include "../tracing.h"
#include "../HttpClientWrapper.h"  // brings in httplib
#include "MediaHandler.h"

using namespace social_network;
using json = nlohmann::json;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  // SetUpTracer("config/jaeger-config.yml", "media-service");
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["media-service"]["port"];

  MediaHandler handler;
  httplib::Server server;

  server.Post("/ComposeMedia", [&](const httplib::Request &req, httplib::Response &res) {
    try {
      auto j = json::parse(req.body);
      int64_t req_id = j["req_id"];
      auto media_types = j["media_types"].get<std::vector<std::string>>();
      auto media_ids = j["media_ids"].get<std::vector<int64_t>>();
      std::map<std::string, std::string> carrier = j["carrier"];

      std::vector<Media> media;
      handler.ComposeMedia(media, req_id, media_types, media_ids, carrier);

      json out;
      out["media"] = json::array();
      for (auto &m : media) {
        out["media"].push_back({{"media_id", m.media_id}, {"media_type", m.media_type}});
      }
      res.set_content(out.dump(), "application/json");
    } catch (std::exception &e) {
      res.status = 500;
      res.set_content("{\"error\":\"exception\"}", "application/json");
    }
  });

  LOG(info) << "Starting the media-service server...";
  server.listen("0.0.0.0", port);
}
