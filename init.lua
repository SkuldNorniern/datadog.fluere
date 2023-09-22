local plugin = {}
local dkjson = require "dkjson"

local API_KEY = "YOUR_DATADOG_API_KEY" -- Replace with your Datadog API key

local hostname = "YOUR_HOSTNAME"

-- Initialization function
function plugin.init(config)
  API_KEY = config.api_key or error("No API key provided")
  hostname = config.hostname or "fluerehost"
end


local function execute_curl_command(endpoint, payload)
  local curl_command = string.format('curl -s -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -H "DD-APPLICATION-KEY: %s" -d \'%s\' > /dev/null 2>&1', endpoint, API_KEY, APP_KEY, dkjson.encode(payload))
  os.execute(curl_command)
end


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
          "source_ip:" .. data.source,
          "dest_ip:" .. data.destination,
          "protocol:" .. data.prot,
          "src_port:" .. data.src_port,
          "dst_port:" .. data.dst_port
        },
        unit = ""
      }
    }
  }
end

-- Process data function
function plugin.process_data(data)
  local metrics_endpoint = "https://api.us5.datadoghq.com/api/v2/series"
  local metrics = {"pkt", "octects", "port.src_port", "port.dst_port"}
  local data_fields = {"d_pkts", "d_octets", "src_port", "dst_port"}

  for i, metric in ipairs(metrics) do
    local payload = create_payload(data, "fluere.net.flow." .. metric, data_fields[i])
    execute_curl_command(metrics_endpoint, payload)
  end
end

-- Cleanup function
function plugin.cleanup()
  -- Cleanup resources before the plugin is unloaded
end

return plugin

