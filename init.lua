local plugin = {}
local dkjson = require "dkjson"

local API_KEY = "YOUR_DATADOG_API_KEY" -- Replace with your Datadog API key
local APP_KEY = "YOUR_DATADOG_APPLICATION_KEY" -- Replace with your Datadog Application key
local hostname = "YOUR_HOSTNAME"

-- Initialization function
function plugin.init(config)
  -- Initialize the plugin with the provided configuration
  if config.api_key then
    API_KEY = config.api_key
  else
    error("No api key provided")
  end
  if config.app_key then
    APP_KEY = config.app_key
  else 
    error("No app key provided")
  end
  if config.hostname then
    hostname = config.hostname
  else
    hostname = "fluerehost"
  end
end

-- Create tag function
function plugin.create_tag(metric_name, tags)
  local tags_str = ""
  for i, tag in ipairs(tags) do
    tags_str = tags_str .. tag
    if i < #tags then
      tags_str = tags_str .. ", "
    end
  end

  print("Creating tags: " .. tags_str .. " for metric: " .. metric_name)

  local tags_endpoint = string.format("https://api.us5.datadoghq.com/api/v2/metrics/%s/tags", metric_name)
  local create_tag_payload = {
    data = {
      type = "create_tags",
      id = "ExampleMetric",
      attributes = {
        tags = tags,
        metric_type = "gauge"
      }
    }
  }

  os.execute(string.format('curl -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -H "DD-APPLICATION-KEY: %s" -d \'%s\'', tags_endpoint, API_KEY, APP_KEY, dkjson.encode(create_tag_payload)))
end



-- Process data function
function plugin.process_data(data)
  -- print("Using API key: " .. API_KEY)

  -- Create a tag (you can remove this line if you don't want to create a tag every time data is processed)
  --plugin.create_tag("fluere.net.flow", {"source_ip", "dest_ip", "protocol", "src_port", "dst_port","first","last"})

  -- Submit the metrics using v2 API
  local metrics_endpoint = "https://api.us5.datadoghq.com/api/v2/series"
  -- print(" d_pkts: " .. data.d_pkts)
  local pkt_metrics_payload = {
    series = {
      {
        metric = "fluere.net.flow.pkt",
        type = 0,
        points = {
          {
            timestamp = data.first // 1000000,
            value = data.d_pkts + 0.0 -- Ensure this is a double (64-bit float)
          }
        },
        resources = {
          {
            name = hostname, -- Replace with a valid name if needed
            type = "host"-- Replace with a valid type if needed
          }
        },
        tags = {
          "source_ip:" .. data.source,
          "dest_ip:" .. data.destination,
          "protocol:" .. data.prot,
          "src_port:" .. data.src_port,
          "dst_port:" .. data.dst_port
        },
        unit = "" -- Empty string for now, add a valid unit if needed
      }
    }
  }
  local octets_metrics_payload = {
    series = {
      {
        metric = "fluere.net.flow.octects",
        type = 0,
        points = {
          {
            timestamp = data.first // 1000000,
            value = data.d_octets + 0.0 -- Ensure this is a double (64-bit float)
          }
        },
        resources = {
          {
            name = hostname, -- Replace with a valid name if needed
            type = "host"-- Replace with a valid type if needed
          }
        },
        tags = {
          "source_ip:" .. data.source,
          "dest_ip:" .. data.destination,
          "protocol:" .. data.prot,
          "src_port:" .. data.src_port,
          "dst_port:" .. data.dst_port
        },
        unit = "" -- Empty string for now, add a valid unit if needed
      }
    }
  }
  -- print(" d_pkts: " .. data.d_pkts)
  local src_port_metrics_payload = {
    series = {
      {
        metric = "fluere.net.flow.port.src_port",
        type = 0,
        points = {
          {
            timestamp = data.first // 1000000,
            value = data.src_port + 0.0 -- Ensure this is a double (64-bit float)
          }
        },
        resources = {
          {
            name = hostname, -- Replace with a valid name if needed
            type = "host"-- Replace with a valid type if needed
          }
        },
        tags = {
          "source_ip:" .. data.source,
          "dest_ip:" .. data.destination,
          "protocol:" .. data.prot,
          "src_port:" .. data.src_port,
          "dst_port:" .. data.dst_port
        },
        unit = "" -- Empty string for now, add a valid unit if needed
      }
    }
  }
  local dst_port_metrics_payload = {
    series = {
      {
        metric = "fluere.net.flow.port.dst_port",
        type = 0,
        points = {
          {
            timestamp = data.first // 1000000,
            value = data.dst_port + 0.0 -- Ensure this is a double (64-bit float)
          }
        },
        resources = {
          {
            name = hostname, -- Replace with a valid name if needed
            type = "host"-- Replace with a valid type if needed
          }
        },
        tags = {
          "source_ip:" .. data.source,
          "dest_ip:" .. data.destination,
          "protocol:" .. data.prot,
          "src_port:" .. data.src_port,
          "dst_port:" .. data.dst_port
        },
        unit = "" -- Empty string for now, add a valid unit if needed
      }
    }
  }

  os.execute(string.format('curl -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -d \'%s\'', metrics_endpoint, API_KEY, dkjson.encode(pkt_metrics_payload)))
  os.execute(string.format('curl -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -d \'%s\'', metrics_endpoint, API_KEY, dkjson.encode(octets_metrics_payload)))
  os.execute(string.format('curl -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -d \'%s\'', metrics_endpoint, API_KEY, dkjson.encode(src_port_metrics_payload)))
  os.execute(string.format('curl -X POST "%s" -H "Accept: application/json" -H "Content-Type: application/json" -H "DD-API-KEY: %s" -d \'%s\'', metrics_endpoint, API_KEY, dkjson.encode(dst_port_metrics_payload)))


end

-- Cleanup function
function plugin.cleanup()
  -- Cleanup resources before the plugin is unloaded
end

return plugin



