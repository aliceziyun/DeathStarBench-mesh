namespace social_network {

struct Creator {
    int64_t user_id;
    std::string username;
};

struct TextServiceReturn {
    std::string text;
    std::vector<UserMention> user_mentions;
    std::vector<Url> urls;

};

struct Media {
    int64_t media_id;
    std::string media_type;
};

struct Post {
    int64_t post_id;
    Creator creator;
    int64_t req_id;
    std::string text;
    std::vector<UserMention> user_mentions;
    std::vector<Media> media;
    std::vector<Url> urls;
    int64_t timestamp;
    PostType::type post_type;
};

struct Url {
    std::string shortened_url;
    std::string expanded_url;
};

}  // namespace social_network