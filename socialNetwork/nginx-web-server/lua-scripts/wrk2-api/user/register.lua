local _M = {}
local k8s_suffix = os.getenv("fqdn_suffix")
if (k8s_suffix == nil) then
  k8s_suffix = ""
end

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.RegisterUser()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local ngx = ngx
  local cjson = require "cjson"

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  -- local tracer = bridge_tracer.new_from_global()
  -- local parent_span_context = tracer:binary_extract(
  --     ngx.var.opentracing_binary_context)
  -- local span = tracer:start_span("register_client",
  --     {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  -- tracer:text_map_inject(span:context(), carrier)

  ngx.req.read_body()
  local post = ngx.req.get_post_args()

  if (_StrIsEmpty(post.first_name) or _StrIsEmpty(post.last_name) or
      _StrIsEmpty(post.username) or _StrIsEmpty(post.password) or
      _StrIsEmpty(post.user_id)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  -- Use HTTP/JSON to call user-service /RegisterUserWithId
  local body_tbl = {
    req_id = req_id,
    first_name = post.first_name,
    last_name = post.last_name,
    username = post.username,
    password = post.password,
    user_id = tonumber(post.user_id),
    carrier = carrier
  }
  local body = cjson.encode(body_tbl)

  local sock = ngx.socket.tcp()
  sock:settimeout(2000)
  local host = "user-service" .. k8s_suffix
  local port = 9090
  local ok, err = sock:connect(host, port)
  if not ok then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("User registration failure: cannot connect: " .. (err or ""))
    ngx.log(ngx.ERR, "register connect error: " .. (err or ""))
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local req_lines = {
    "POST /RegisterUserWithId HTTP/1.1",
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
    ngx.say("User registration failure: send error: " .. (send_err or ""))
    ngx.log(ngx.ERR, "register send error: " .. (send_err or ""))
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local status_line, rerr = sock:receive("*l")
  if not status_line then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("User registration failure: receive error: " .. (rerr or ""))
    ngx.log(ngx.ERR, "register receive error: " .. (rerr or ""))
    sock:close()
    -- span:finish()
    ngx.exit(ngx.status)
  end

  local http_code = tonumber(string.match(status_line, "HTTP/%d%.%d%s+(%d%d%d)")) or 0

  -- skip headers
  while true do
    local line, herr = sock:receive("*l")
    if not line or line == "" then break end
    if not line then
      ngx.log(ngx.WARN, "register header read ended: " .. (herr or ""))
      break
    end
  end
  local resp_body = sock:receive("*a")
  if not resp_body then resp_body = "" end
  sock:close()

  if http_code >= 200 and http_code < 300 then
    ngx.say("Success!")
    -- span:finish()
  else
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("User registration failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    ngx.log(ngx.ERR, "User registration failure: HTTP " .. tostring(http_code) .. " body: " .. (resp_body or ""))
    -- span:finish()
    ngx.exit(ngx.status)
  end
end

return _M