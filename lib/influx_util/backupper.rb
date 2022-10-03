require 'influx_util/config'
require 'influx_util/log'

module InfluxUtil
  class Backupper
    attr_reader :db_name, :rp_name, :start_sid, :log

    def self.check(args)
      errs = []
      [:db, :rp].each do |key|
        errs << "#{key} is not specifed" if args[key].nil?
      end

      errs
    end

    def initialize(args)
      @db_name   = args[:db]
      @rp_name   = args[:rp]
      @start_sid = args[:start_sid].nil? ? nil : args[:start_sid].to_i
    end

    def operate
      if start_sid.nil?
        sid = get_last_backup_shard_id()
      else
        sid = start_sid
        Log.info "start_sid present -> #{sid}"
      end
      Log.info "start backup from ->  #{db_name}.#{rp_name}.#{sid}"
      
      backup_shard_items = get_backup_shard_items(sid)
      backup_shard_items.each do |si|
        Log.info "backup #{si.db}.#{si.rp}.#{si.id} -> #{si.store_path}"

        # record
        ok = run_backup_cmd(si)
        if ok && start_sid.nil?
          shard_record = JSON.parse(File.read(Config.shard_record_path))
          shard_record[db_name] = {} if shard_record[db_name].nil?
          shard_record[db_name][rp_name] = si.id
  
          File.open(Config.shard_record_path, 'w') {|f| f.write(shard_record.to_json)}
          Log.info "record shard id -> #{si.db}.#{si.rp}.#{si.id}"
        end
      end
    end

    private

    def run_backup_cmd(shard_item)
      FileUtils.rm_rf(shard_item.store_path) if File.directory?(shard_item.store_path) 
      FileUtils.mkdir_p(shard_item.store_path)

      cmd = "influxd backup -portable -database #{shard_item.db} -retention #{shard_item.rp} -shard #{shard_item.id}"
      cmd = cmd + " #{shard_item.store_path}"

      Log.info "[cmd] -> #{cmd}"
      influx_output = `#{cmd}`
      Log.info "[cmd output] -> #{influx_output}"
    end

    def record_backup_shard_id(last_backup_sid)
      File.open(Config.shard_record_path, 'w') { |f| f.write(last_backup_sid) }
    end

    def get_last_backup_shard_id
      sid = 0

      if File.exist?(Config.shard_record_path)
        shard_record = JSON.parse(File.read(Config.shard_record_path))
        if !shard_record[db_name].nil? && !shard_record[db_name][rp_name].nil?
          sid = shard_record[db_name][rp_name]
        end
      else
        File.open(Config.shard_record_path, 'w') do |f|     
          f.write({
            db_name => {
              rp_name => sid
            }
          }.to_json)   
        end
      end

      sid
    end

    def get_backup_shard_items(shard_id)
      shard_items = Shard.new(db: db_name, rp: rp_name, output: false).operate
      shard_ids   = shard_items.map{ |v| v.id }

      Log.info("backup candidate ids -> #{shard_ids.to_s}")
      if shard_ids.empty?
        Log.info("shard_ids is empty")
        return []
      end

      if shard_ids.max < shard_id
        Log.info("id is overflow -> #{shard_id}")
        return []
      end

      shard_items = shard_items.select { |v| v.id >= shard_id }

      # Check shard files is exist
      shard_items = shard_items.select do |v| 
        path = File.join(Config.shard_source_path, v.db, v.rp, v.id.to_s)
        cmd  = "ls #{path} > /dev/null 2>&1"
        cmd  = "sudo #{cmd}" if ENV["INFLUX_UTIL_SUDO"] == '1'
        ok   = system(cmd)

        Log.info("backup file is not found -> #{v.db}.#{v.rp}.#{v.id}") if !ok
        ok
      end

      shard_items.each do |v|
        start_time = Time.parse(v.start_at)
        end_time   = Time.parse(v.end_at)
        store_path = File.join(Config.shard_backup_path, 
                               v.db,
                               v.rp,
                               start_time.year.to_s,
                               "#{v.id}_#{start_time.strftime(T_FORMAT)}_#{end_time.strftime(T_FORMAT)}")

        v.store_path = store_path
      end

      shard_items
    end
  end
end