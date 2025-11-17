 #include <signal.h>

 #include <nlohmann/json.hpp>
 #include <boost/program_options.hpp>

 #include "../utils.h"
 #include "../HttpClientWrapper.h"
 #include "../ClientPool.h"
 #include "../logger.h"
 #include "../tracing.h"
 #include "TextHandler.h"

 using json = nlohmann::json;
 using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

 int main(int argc, char *argv[]) {
     signal(SIGINT, sigintHandler);
     init_logger();
     SetUpTracer("config/jaeger-config.yml", "text-service");

     json config_json;
     if (load_config_file("config/service-config.json", &config_json) != 0) {
         exit(EXIT_FAILURE);
     }

     int port = config_json["text-service"]["port"];
     std::string url_addr = config_json["url-shorten-service"]["addr"];
     int url_port = config_json["url-shorten-service"]["port"];
     int url_conns = config_json["url-shorten-service"]["connections"];
     int url_timeout = config_json["url-shorten-service"]["timeout_ms"];
     int url_keepalive = config_json["url-shorten-service"]["keepalive_ms"];

     std::string user_mention_addr = config_json["user-mention-service"]["addr"];
     int user_mention_port = config_json["user-mention-service"]["port"];
     int user_mention_conns = config_json["user-mention-service"]["connections"];
     int user_mention_timeout = config_json["user-mention-service"]["timeout_ms"];
     int user_mention_keepalive = config_json["user-mention-service"]["keepalive_ms"];

     ClientPool<HttpClientWrapper> url_client_pool(
             "url-shorten-service", url_addr, url_port, 0, url_conns, url_timeout,
             url_keepalive);
     ClientPool<HttpClientWrapper> user_mention_client_pool(
             "user-mention-service", user_mention_addr, user_mention_port, 0,
             user_mention_conns, user_mention_timeout, user_mention_keepalive);

     TextHandler handler(&url_client_pool, &user_mention_client_pool);
     httplib::Server server;

     server.Post("/ComposeText", [&](const httplib::Request &req, httplib::Response &res) {
         try {
             auto j = json::parse(req.body);
             int64_t req_id = j["req_id"];
             std::string text = j["text"];
             std::map<std::string, std::string> carrier;
             if (j.contains("carrier")) carrier = j["carrier"].get<std::map<std::string, std::string>>();

             std::string updated_text;
             std::vector<json> urls_out;
             std::vector<json> user_mentions_out;
             handler.ComposeText(updated_text, urls_out, user_mentions_out, req_id, text, carrier);
             json resp = {{"text", updated_text}, {"urls", urls_out}, {"user_mentions", user_mentions_out}};
             res.set_content(resp.dump(), "application/json");
         } catch (const std::exception &e) {
             res.status = 500;
             res.set_content(json({{"error", e.what()}}).dump(), "application/json");
         }
     });

     LOG(info) << "Starting the text-service HTTP server...";
     server.listen("0.0.0.0", port);
 }
