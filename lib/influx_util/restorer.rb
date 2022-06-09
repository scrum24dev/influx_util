require 'influx_util/config'
require 'influx_util/log'

module InfluxUtil
  class Restorer
    attr_reader :temp_db_name, :db_name, :rp_name, :shard_id, :year, :new_db_name

    def self.check(args)
      errs = []
      [:db, :rp, :new_db].each do |key|
        errs << "#{key} is not specifed" if args[key].nil?
      end

      errs
    end

    def initialize(args)
      @db_name      = args[:db]
      @rp_name      = args[:rp]
      @new_db_name  = args[:new_db]
      @temp_db_name = "temp_#{db_name}"
      @shard_id     = args[:sid].nil? ? nil : args[:sid].to_i
      @year         = args[:year].nil? ? nil : args[:year].to_i
    end

    def operate
      items = Shard.new(db: db_name, rp: rp_name, output: false, list_bd: true).operate

      if !shard_id.nil?
        items = items.select {|item| item.id == shard_id}
      elsif !year.nil?
        puts year
        items = items.select {|item| item.start_at.year == year }
      end

      items.sort_by!(&:id)

      Log.info "temp database ->  #{temp_db_name}"
      Log.info("restore candidate ids -> #{items.map(&:id)}")

      run_influx_api("create database #{new_db_name}")
      run_influx_api("drop database #{temp_db_name}")

      sleep_time = 20
      items.each do |item|
        Log.info "restore -> #{item.db}.#{item.rp}.#{item.id}"

        cmd = "influxd restore -portable -db #{item.db} -rp #{item.rp} -shard #{item.id}"
        cmd = cmd + " -newdb #{temp_db_name}"
        cmd = cmd + " #{item.path}"

        Log.info "[cmd] -> #{cmd}"
        influx_output = `#{cmd}`
        Log.info "[cmd output] -> #{influx_output}"

        Log.info "[sleep] #{sleep_time}"
        sleep sleep_time # https://github.com/node-influx/node-influx/issues/344

        run_influx_api("SELECT * INTO #{new_db_name}..:MEASUREMENT FROM /.*/ GROUP BY *", db: temp_db_name) 

        Log.info "[sleep] #{sleep_time}"
        sleep sleep_time # https://github.com/node-influx/node-influx/issues/344

        run_influx_api("drop database #{temp_db_name}")

        Log.info "[sleep] #{sleep_time}"
        sleep sleep_time # https://github.com/node-influx/node-influx/issues/344
      end
    end

    private

    def run_influx_api(q, options={})
      r                 = true
      uri               = URI(API_URL)
      http              = Net::HTTP.new(uri.host, uri.port)
      http.read_timeout = 7200
      path              = options[:db].nil? ? uri.path : "#{uri.path}?db=#{options[:db]}"

      req                 = Net::HTTP::Post.new(path)
      req.body            = URI.encode_www_form(q: q)
      req['Content-Type'] = 'application/x-www-form-urlencoded'

      begin
        Log.info "[influx API] -> db: #{options[:db]} q: #{q}"
        res = http.request(req)

        data = JSON.parse(res.body)
        Log.info "[influx API output] -> #{data.to_s}"

        r = false if !data['error'].nil? || (data['results'] && data['results'][0] && !data['results'][0]["error"].nil?)
      rescue => e
        r = false
        Log.info "[influx API error] -> #{e.message}"
      end

      raise 'call influx api fail' if !r
    end

  end
end