local plugin = {}
-- local dkjson = require "dkjson"

local API_KEY = "YOUR_DATADOG_API_KEY" -- Replace with your Datadog API key
local METRICS_ENDPOINT = "YOUR_METRICS_ENDPOINT"
local hostname = "YOUR_HOSTNAME"
local metrics_name = "YOUR_METRICS_NAME"
local pkt_metrics_name = "YOUR_PKT_METRICS_NAME"
local octets_metrics_name = "YOUR_OCTETS_METRICS_NAME"
local src_port_metrics_name = "YOUR_SRC_PORT_METRICS_NAME"
local dst_port_metrics_name = "YOUR_DST_PORT_METRICS_NAME"

-- Initialization function
function plugin.init(config)
  API_KEY = config.api_key or error("No API key provided")
  METRICS_ENDPOINT = config.endpoint or error("No metrics endpoint provided")

  hostname = config.hostname or "fluerehost"
  metrics_name = config.metrics_name or "fluere.flow"
  pkt_metrics_name = config.pkt_metrics_name or "pkts"
  octets_metrics_name = config.octets_metrics_name or "octets"
  src_port_metrics_name = config.src_port_metrics_name or "port.src"
  dst_port_metrics_name = config.dst_port_metrics_name or "port.dst"

end


local function escape_string(str)
    local escapes = {
        ['"'] = '\\"',
        ['\\'] = '\\\\',
        ['/'] = '\\/',
        ['\b'] = '\\b',
        ['\f'] = '\\f',
        ['\n'] = '\\n',
        ['\r'] = '\\r',
        ['\t'] = '\\t'
    }

    return str:gsub('["\\/%b]', escapes)
end

local function encode_json(value, indent)
    local t = type(value)
    if t == "string" then
        return '"' .. escape_string(value) .. '"'
    elseif t == "number" or t == "boolean" then
        return tostring(value)
    elseif t == "nil" then
        return "null"
    elseif t == "table" then
        local has_keys = false
        for k, v in pairs(value) do
            if type(k) ~= "number" then
                has_keys = true
                break
            end
        end
        
        local items = {}
        if has_keys then  -- Object
            for k, v in pairs(value) do
                table.insert(items, '"' .. escape_string(k) .. '":' .. encode_json(v))
            end
            return '{' .. table.concat(items, ",") .. '}'
        else  -- Array
            for i, v in ipairs(value) do
                table.insert(items, encode_json(v))
            end
            return '[' .. table.concat(items, ",") .. ']'
        end
    else
        error("Unsupported data type: " .. t)
    end
end

local function execute_curl_command(endpoint, payload)
    local json_payload = encode_json(payload)
    local curl_command = string.format('curl -s -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -d \'%s\' > /dev/null 2>&1', endpoint, API_KEY, json_payload)
    os.execute(curl_command)
end

-- ... rest of your code ...




-- Helper function to create payload
local function create_payload(data, metric_name, data_field)
  return {
    series = {
      {
        metric = metric_name,
        type = 0,
        points = {
          {
            timestamp = data.first // 1000000,
            value = data[data_field] + 0.0
          }
        },
        resources = {
          {
            name = hostname,
            type = "host"
          }
        },
        tags = {
          "SourceIp:" .. data.source,
          "DestIp:" .. data.destination,
          "Protocol:" .. data.prot,
          "SrcPort:" .. data.src_port,
          "DstPort:" .. data.dst_port
        },
        unit = ""
      }
    }
  }
end

-- Process data function
function plugin.process_data(data)
  local metrics = {pkt_metrics_name, octets_metrics_name, src_port_metrics_name, dst_port_metrics_name}
  local data_fields = {"d_pkts", "d_octets", "src_port", "dst_port"}

  for i, metric in ipairs(metrics) do
    local payload = create_payload(data, metrics_name .. "." .. metric, data_fields[i])
    execute_curl_command(METRICS_ENDPOINT, payload)
  end
end

-- Cleanup function
function plugin.cleanup()
  -- Cleanup resources before the plugin is unloaded
end

return plugin

