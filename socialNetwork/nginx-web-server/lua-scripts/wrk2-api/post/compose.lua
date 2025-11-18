local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.ComposePost()
  -- local bridge_tracer = require "opentracing_bridge_tracer"
  local ngx = ngx
  local cjson = require "cjson"
  -- Replace Thrift client usage with simple HTTP/JSON POST to ComposePostService
  -- Keep tracing: inject carrier map into request body

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  -- local tracer = bridge_tracer.new_from_global()
  -- local parent_-- span:finish()_context = tracer:binary_extract(ngx.var.opentracing_binary_context)

  ngx.req.read_body()
  local post = ngx.req.get_post_args()

  if (_StrIsEmpty(post.user_id) or _StrIsEmpty(post.username) or
      _StrIsEmpty(post.post_type) or _StrIsEmpty(post.text)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  -- local -- span:finish() = tracer:start_-- span:finish()("compose_post_client",
  --     { ["references"] = { { "child_of", parent_-- span:finish()_context } } })
  local carrier = {}
  -- tracer:text_map_inject(-- span:finish():context(), carrier)

  -- prepare media arrays
  local media_ids = {}
  local media_types = {}
  if (not _StrIsEmpty(post.media_ids) and not _StrIsEmpty(post.media_types)) then
    local ok1, dec1 = pcall(cjson.decode, post.media_ids)
    local ok2, dec2 = pcall(cjson.decode, post.media_types)
    if ok1 and type(dec1) == 'table' then media_ids = dec1 end
    if ok2 and type(dec2) == 'table' then media_types = dec2 end
  end

  -- build request body matching ComposePostService HTTP contract
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
  local body = cjson.encode(body_tbl)

  -- HTTP POST via ngx.socket.tcp
  local sock = ngx.socket.tcp()
  sock:settimeout(2000) -- 2s timeout
  local host = "compose-post-service" .. k8s_suffix
  local port = 9090
  local ok, err = sock:connect(host, port)
  if not ok then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("compose_post failure: cannot connect to compose-post-service: " .. (err or ""))
    ngx.log(ngx.ERR, "compose_post connect error: " .. (err or ""))
    -- span:finish():finish()
    ngx.exit(ngx.status)
  end

  local req_lines = {
    "POST /ComposePost HTTP/1.1",
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
    ngx.say("compose_post failure: send error: " .. (send_err or ""))
    ngx.log(ngx.ERR, "compose_post send error: " .. (send_err or ""))
    sock:close()
    -- span:finish():finish()
    ngx.exit(ngx.status)
  end

  -- read status line
  local status_line, lerr = sock:receive("*l")
  if not status_line then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("compose_post failure: receive error: " .. (lerr or ""))
    ngx.log(ngx.ERR, "compose_post receive error: " .. (lerr or ""))
    sock:close()
    -- span:finish():finish()
    ngx.exit(ngx.status)
  end

  local http_code = tonumber(string.match(status_line, "HTTP/%d%.%d%s+(%d%d%d)")) or 0

  -- skip headers
  while true do
    local line, errh = sock:receive("*l")
    if not line or line == "" then break end
    if not line then
      ngx.log(ngx.WARN, "compose_post header read ended: " .. (errh or ""))
      break
    end
  end

  local resp_body, rerr = sock:receive("*a")
  if not resp_body then resp_body = "" end

  sock:close()

  if http_code >= 200 and http_code < 300 then
    ngx.status = ngx.HTTP_OK
    ngx.say("Successfully upload post")
    -- span:finish():finish()
    ngx.exit(ngx.status)
  else
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("compose_post failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    ngx.log(ngx.ERR, "compose_post failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    -- span:finish():finish()
    ngx.exit(ngx.status)
  end
end

return _M