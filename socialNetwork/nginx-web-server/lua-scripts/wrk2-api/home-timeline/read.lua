local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

local function _LoadTimeline(data)
  local timeline = {}
  for _, timeline_post in ipairs(data) do
    local new_post = {}
    new_post["post_id"] = tostring(timeline_post.post_id)
    new_post["creator"] = {}
    new_post["creator"]["user_id"] = tostring(timeline_post.creator.user_id)
    new_post["creator"]["username"] = timeline_post.creator.username
    new_post["req_id"] = tostring(timeline_post.req_id)
    new_post["text"] = timeline_post.text
    new_post["user_mentions"] = {}
    for _, user_mention in ipairs(timeline_post.user_mentions) do
      local new_user_mention = {}
      new_user_mention["user_id"] = tostring(user_mention.user_id)
      new_user_mention["username"] = user_mention.username
      table.insert(new_post["user_mentions"], new_user_mention)
    end
    new_post["media"] = {}
    for _, media in ipairs(timeline_post.media) do
      local new_media = {}
      new_media["media_id"] = tostring(media.media_id)
      new_media["media_type"] = media.media_type
      table.insert(new_post["media"], new_media)
    end
    new_post["urls"] = {}
    for _, url in ipairs(timeline_post.urls) do
      local new_url = {}
      new_url["shortened_url"] = url.shortened_url
      new_url["expanded_url"] = url.expanded_url
      table.insert(new_post["urls"], new_url)
    end
    new_post["timestamp"] = tostring(timeline_post.timestamp)
    new_post["post_type"] = timeline_post.post_type
    table.insert(timeline, new_post)
  end
  return timeline
end

function _M.ReadHomeTimeline()
  -- local bridge_tracer = require "opentracing_bridge_tracer"
  local ngx = ngx
  local cjson = require "cjson"

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  -- local tracer = bridge_tracer.new_from_global()
  -- local parent_span_context = tracer:binary_extract(
  --     ngx.var.opentracing_binary_context)

  -- local span = tracer:start_span("read_home_timeline_client",
  --     { ["references"] = { { "child_of", parent_span_context } } })
  local carrier = {}
  -- tracer:text_map_inject(span:context(), carrier)

  ngx.req.read_body()
  local args = ngx.req.get_uri_args()

  if (_StrIsEmpty(args.user_id) or _StrIsEmpty(args.start) or _StrIsEmpty(args.stop)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end
  -- Switch from Thrift client to HTTP/JSON POST to home-timeline-service
  local start_idx = tonumber(args.start)
  local stop_idx = tonumber(args.stop)

  local body_tbl = {
    req_id = req_id,
    user_id = tonumber(args.user_id),
    start_idx = start_idx,
    stop_idx = stop_idx,
    carrier = carrier
  }
  local body = cjson.encode(body_tbl)

  local sock = ngx.socket.tcp()
  sock:settimeout(2000)
  local host = "home-timeline-service" .. k8s_suffix
  local port = 9090
  local ok, err = sock:connect(host, port)
  if not ok then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Get home-timeline failure: cannot connect: " .. (err or ""))
    ngx.log(ngx.ERR, "home-timeline connect error: " .. (err or ""))
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local req_lines = {
    "POST /ReadHomeTimeline HTTP/1.1",
    "Host: " .. host,
    "Content-Type: application/json",
    "Content-Length: " .. tostring(#body),
    "Connection: close",
    "",
    body
  }
  local req_str = table.concat(req_lines, "\r\n")

  local bytes, send_err = sock:send(req_str)
  if not bytes then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Get home-timeline failure: send error: " .. (send_err or ""))
    ngx.log(ngx.ERR, "home-timeline send error: " .. (send_err or ""))
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local status_line, lerr = sock:receive("*l")
  if not status_line then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Get home-timeline failure: receive error: " .. (lerr or ""))
    ngx.log(ngx.ERR, "home-timeline receive error: " .. (lerr or ""))
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local http_code = tonumber(string.match(status_line, "HTTP/%d%.%d%s+(%d%d%d)")) or 0

  -- skip headers
  while true do
    local line, errh = sock:receive("*l")
    if not line or line == "" then break end
    if not line then
      ngx.log(ngx.WARN, "home-timeline header read ended: " .. (errh or ""))
      break
    end
  end

  local resp_body, rerr = sock:receive("*a")
  if not resp_body then resp_body = "" end

  sock:close()

  if http_code >= 200 and http_code < 300 then
    local okj, decoded = pcall(cjson.decode, resp_body)
    if not okj then
      ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
      ngx.say("Get home-timeline failure: invalid JSON response")
      ngx.log(ngx.ERR, "home-timeline invalid JSON: " .. (resp_body or ""))
      -- span:finish()
      ngx.exit(ngx.status)
    end
    local posts = (decoded and decoded.posts) or {}
    local home_timeline = _LoadTimeline(posts)
    ngx.header.content_type = "application/json; charset=utf-8"
    ngx.say(cjson.encode(home_timeline))
    -- span:finish()
    ngx.exit(ngx.HTTP_OK)
  else
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Get home-timeline failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    ngx.log(ngx.ERR, "home-timeline failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    -- span:finish()
    ngx.exit(ngx.status)
  end
end

return _M