require 'influx_util/config'

require 'logger'

module InfluxUtil
  module Log
    def self.info(msg)
      @@log ||= Logger.new(Config.log_path)
      @@log.info(msg)
    end
  end
end