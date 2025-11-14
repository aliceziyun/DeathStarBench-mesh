namespace social_network {
struct PostType {
  enum type {
    POST = 0,
    REPOST = 1,
    REPLY = 2,
    DM = 3
  };
};

struct Creator {
    int64_t user_id;
    std::string username;
};

struct UserMention {
    int64_t user_id;
    std::string username;
    int32_t start_index;
    int32_t end_index;
};

struct Media {
    int64_t media_id;
    std::string media_type;
};

struct Url {
    std::string shortened_url;
    std::string expanded_url;
};

struct TextServiceReturn {
    std::string text;
    std::vector<UserMention> user_mentions;
    std::vector<Url> urls;

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

}  // namespace social_network