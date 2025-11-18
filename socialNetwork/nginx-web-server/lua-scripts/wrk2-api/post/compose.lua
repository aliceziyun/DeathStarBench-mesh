local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.ComposePost()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local ngx = ngx
  local cjson = require "cjson"

  local tcp = ngx.socket.tcp

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  -- local tracer = bridge_tracer.new_from_global()
  -- local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)


  ngx.req.read_body()
  local raw = ngx.req.get_body_data()
  local post = ngx.decode_args(raw, 0) 

  if (_StrIsEmpty(post.user_id) or _StrIsEmpty(post.username) or
      _StrIsEmpty(post.post_type) or _StrIsEmpty(post.text)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  -- prepare tracing span and carrier
  -- local span = tracer:start_span("compose_post_client",
  --     { ["references"] = { { "child_of", parent_span_context } } })
  local carrier = {}
  -- tracer:text_map_inject(span:context(), carrier)

  -- build request body
  local media_ids = {}
  local media_types = {}
  if (not _StrIsEmpty(post.media_ids)) then
    local ok_decode, decoded = pcall(cjson.decode, post.media_ids)
    if ok_decode and type(decoded) == "table" then media_ids = decoded end
  end
  if (not _StrIsEmpty(post.media_types)) then
    local ok_decode, decoded = pcall(cjson.decode, post.media_types)
    if ok_decode and type(decoded) == "table" then media_types = decoded end
  end

  local body_tbl = {
    req_id = req_id,
    username = post.username,
    user_id = tonumber(post.user_id),
    text = post.text,
    media_ids = media_ids,
    media_types = media_types,
    post_type = tonumber(post.post_type),
    carrier = carrier
  }

  local payload = cjson.encode(body_tbl)

  -- HTTP POST to compose-post-service
  local host = "compose-post-service" .. k8s_suffix
  local port = 9090
  local path = "/ComposePost"

  local sock = tcp()
  sock:settimeout(60000)
  local ok, err = sock:connect(host, port)
  if not ok then
    ngx.status = ngx.HTTP_SERVICE_UNAVAILABLE
    ngx.say("compose_post connect failure: " .. (err or "unknown"))
    ngx.log(ngx.ERR, "compose_post connect failure: ", err)
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local req = "POST " .. path .. " HTTP/1.1\r\n"
    .. "Host: " .. host .. ":" .. port .. "\r\n"
    .. "Content-Type: application/json\r\n"
    .. "Content-Length: " .. #payload .. "\r\n"
    .. "Connection: close\r\n\r\n"
    .. payload

  local bytes, send_err = sock:send(req)
  if not bytes then
    ngx.status = ngx.HTTP_BAD_GATEWAY
    ngx.say("compose_post send failure: " .. (send_err or "unknown"))
    ngx.log(ngx.ERR, "compose_post send failure: ", send_err)
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  -- read status line
  local status_line, rerr = sock:receive("*l")
  if not status_line then
    ngx.status = ngx.HTTP_BAD_GATEWAY
    ngx.say("compose_post read failure: " .. (rerr or "unknown"))
    ngx.log(ngx.ERR, "compose_post read failure: ", rerr)
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local status_code = tonumber(status_line:match("HTTP/%d%.%d%s+(%d%d%d)")) or 0

  -- consume headers
  while true do
    local line = sock:receive("*l")
    if not line or line == "" then break end
  end

  -- read body (optional)
  local body = sock:receive("*a") or ""
  sock:close()

  if status_code ~= 200 then
    ngx.status = ngx.HTTP_BAD_GATEWAY
    local msg = body ~= "" and body or ("compose_post HTTP " .. tostring(status_code))
    ngx.say(msg)
    ngx.log(ngx.ERR, "compose_post non-200: ", status_code, ", body: ", body)
    -- span:finish()
    ngx.exit(ngx.status)
  end

  ngx.status = ngx.HTTP_OK
  ngx.say("Successfully upload post")
  -- span:finish()
  ngx.exit(ngx.status)
end

return _M