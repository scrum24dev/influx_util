require 'influx_util/config'
require 'influx_util/log'
require 'influx_util/misc'
require 'influx_util/shard'
require 'influx_util/backupper'
require 'influx_util/restorer'

require 'fileutils'
require 'optparse'
require 'ostruct'


# 命令處理
# TODO: 錯誤處理
module InfluxUtil
  T_FORMAT = '%Y%m%d%H%S'
  API_URL  = 'http://127.0.0.1:8086/query'

  def self.parse_args
    action  = ARGV[0]
    options = {}
  
    action_parser_help = "
      Action are:
        backup  : backup shard
        restore : restore shard
        shard   : list shard
    "
  
    action_parser = OptionParser.new do |opts|
      opts.banner = "Usage: influx_util action [options]"
  
      opts.separator ''
      opts.separator action_parser_help
    end
  
    action_processor = {
      'shard' => {
        'operator' => InfluxUtil::Shard,
        'opt' => OptionParser.new do |opts|
          opts.banner = "list shard"
  
          opts.on('-d db', '--db db', 'shard database name') do |v|
            options[:db] = v
          end
  
          opts.on('-r rp', '--rp rp', 'shard retion policy name') do |v|
            options[:rp] = v
          end
  
          opts.on('-b', '--list_backup_data', 'list backup data') do |v|
            options[:list_bd] = true
          end
        end
      },
      'backup' => {
        'operator' => InfluxUtil::Backupper,
        'opt' => OptionParser.new do |opts|
          opts.banner = "backup shard"
  
          opts.on('-d db', '--db db', '[required] name of the database that will be backup') do |v|
            options[:db] = v
          end
  
          opts.on('-r rp', '--rp rp', '[required] name of the retention policy that will be backup') do |v|
            options[:rp] = v
          end
  
          opts.on('-s id', '--start_shard_id id', 'id of shard that will be backup. the id is not update shard record file') do |v|
            options[:start_sid] = v
          end
        end
      },
      'restore' => {
        'operator' => InfluxUtil::Restorer,
        'opt' => OptionParser.new do |opts|
          opts.banner = 'Usage: influx_util restore [options]'
  
          opts.on('-d db', '--db db', '[required] name of the database from the backup that will be restored') do |v|
            options[:db] = v
          end
  
          opts.on('-r rp', '--rp rp', '[required] name of the retention policy from the backup that will be restored') do |v|
            options[:rp] = v
          end
  
          opts.on('-s id', '--shard_id id', 'id of shard that will be restored, it ignore "-y year" argument if specified') do |v|
            options[:sid] = v
          end
  
          opts.on('-n import_db', '--import_db import_db', '[required] name of the imported database') do |v|
            options[:import_db] = v
          end
  
          opts.on('-y year', '--year year', 'restore shard by the year, it ignore "-s id" argument if specified') do |v|
            options[:year] = v
          end
        end
      }
    }
  
    action_parser.order!
    return ['action is not specified', nil] if action.nil?
    return ["action is invalid, valid actions is #{action_processor.keys}", nil] if !action_processor.keys.include?(action)
  
    ARGV.shift
    action_processor[action]['opt'].order!
    
    # Operate action
    op   = action_processor[action]['operator']
    errs = op.check(options)
    if !errs.empty?
      return [errs.join(', '), nil]
    end
  
    [nil, op.new(options)]
  end

  def self.run
     # 檢查是否在執行
    pid_file_path = File.join(Config.dir_path, 'pid')
    abort("process #{Misc.get_file_content(pid_file_path)} is running") if File.exist?(pid_file_path)

    # Signal catch
    Signal.trap('QUIT') do  
      FileUtils.rm(pid_file_path) if File.exist?(pid_file_path)
      abort("QUIT signal catched")
    end

    begin
      # 寫pid
      Misc.write_file(pid_file_path, Process.pid)

      # 分析參數
      err, op = parse_args
      abort(err) if !err.nil?

      # 執行
      op.operate
    rescue => e
      if e.class == OptionParser::MissingArgument
        puts e.message 
      else
        Log.info(e.message)
        Log.info(e.backtrace)
      end
    ensure
      FileUtils.rm(pid_file_path) if File.exist?(pid_file_path)
    end
  end
end

InfluxUtil.run