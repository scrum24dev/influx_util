require 'influx_util/config'
require 'influx_util/log'

require 'json'
require 'ostruct'
require 'time'  
require 'net/http'   

module InfluxUtil
  class Shard
    attr_reader :db_name, :rp_name, :output, :list_bd
  
    def self.check(args)
      []
    end
  
    def initialize(args)
      @db_name  = args[:db]
      @rp_name  = args[:rp]
      @list_bd  = args[:list_bd].nil? ? false : (args[:list_bd] == true ? true : false)
      @output   = args[:output].nil? ? true : (args[:output] == true ? true : false)
    end
  
    def operate
      items, label_info = list_bd ? list_backup_data : list_shards

      output ? as_table(items, label_info) : items
    end
  
    # TODO: 優化
    def list_backup_data
      items    = []
      db_paths = Dir[File.join(Config.shard_backup_path, '*')]

      db_paths.each do |db_path|
        db = File.basename(db_path)
        next if db_name && db_name != db

        Dir[File.join(db_path, '*')].each do |rp_path|
          rp = File.basename(rp_path)
          next if rp_name && rp_name != rp

          Dir[File.join(rp_path, '*')].each do |year_path|
            year = File.basename(year_path)

            Dir[File.join(year_path, '*')].each do |backup_path|
              id, start_at, end_at = File.basename(backup_path).split('_')

              items << OpenStruct.new(
                db:       db,
                rp:       rp,
                year:     year,
                id:       id.to_i,
                start_at: Time.strptime(start_at, T_FORMAT),
                end_at:   Time.strptime(end_at, T_FORMAT),
                path:     backup_path
              )
            end
          end
        end
      end

      [items, {db: 'db', rp: 'rp', year: 'year', id: 'shard id', start_at: 'start_at', end_at: 'end_at'}]
    end
  
    # TODO: 優化
    def list_shards
      uri         = URI("#{API_URL}?q=show shards")
      data        = JSON.parse(Net::HTTP.get(uri))['results'][0]
      db_items    = [] 
      shard_items = []

      data['series'].each do |db_item| 
        next if db_name && db_item['name'] != db_name
        next if db_item['name'] == '_internal'
        db_items << db_item
      end

      db_items.each do |db_item|
        next if db_item['values'].nil?

        id_idx    = db_item['columns'].find_index('id')
        db_idx    = db_item['columns'].find_index('database')
        rp_idx    = db_item['columns'].find_index('retention_policy')
        start_idx = db_item['columns'].find_index('start_time')
        end_idx   = db_item['columns'].find_index('end_time')

        
        db_item['values'] = db_item['values'].select { |v| v[rp_idx] == rp_name } if rp_name
        db_item['values'] = db_item['values'].sort_by{ |v| v[id_idx] }

        db_item['values'].each do |v|
          shard_items << OpenStruct.new(
            id:       v[id_idx],
            db:       v[db_idx],
            rp:       v[rp_idx],
            start_at: v[start_idx],
            end_at:   v[end_idx]
          )
        end
      end

      [shard_items, { id: 'id', db: 'db', rp: 'rp',  start_at: 'start_at', end_at: 'end_at'}]
    end
  
    private

    def as_table(shard_items, label_info)
      columns = label_info.each_with_object({}) do |(col, label), h|
        h[col] = {
          label: label,
          width: [shard_items.map { |g| g.send(col).to_s.size }.max.to_i, label.size].max 
        }
      end

      write_header  = Proc.new { "| #{ columns.map { |_,g| g[:label].ljust(g[:width]) }.join(' | ') } |\n"}
      write_divider = Proc.new { "+-#{ columns.map { |_,g| "-"*g[:width] }.join("-+-") }-+\n" }
      write_line    = Proc.new do |h|
        str = label_info.keys.map { |k| h[k].to_s.ljust(columns[k][:width]) }.join(" | ") 
        "| #{str} |\n"
      end

      table = write_divider.call
      table = table + write_header.call
      table = table + write_divider.call
      shard_items.each { |shard_item| table = table + write_line.call(shard_item) }
      table = table + write_divider.call if !shard_items.empty?

      IO.popen('less', 'w') { |f| f.puts table }
    end

  end
end