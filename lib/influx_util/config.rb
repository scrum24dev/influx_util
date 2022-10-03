require 'influx_util/misc'

require 'yaml'
require 'fileutils'
require 'ostruct'

module InfluxUtil
  module Config
    CONFIG_DIR_PATH  = File.join(Dir.home, '.influx_util')
    CONFIG_FILE_PATH = File.join(CONFIG_DIR_PATH, 'config.yml')

    # 建立必要的資料夾和檔案
    FileUtils.mkdir_p(CONFIG_DIR_PATH) if !File.exist?(CONFIG_DIR_PATH)

    Misc.write_file(CONFIG_FILE_PATH, {
      'shard_source_path' => '/var/lib/influxdb/data',
      'shard_backup_path' => File.join(CONFIG_DIR_PATH, 'data'),
      'shard_record_path' => File.join(CONFIG_DIR_PATH, 'record.json'),
      'log_path'          => File.join(CONFIG_DIR_PATH, 'util.log')
    }.to_yaml) if !File.exist?(CONFIG_FILE_PATH)

    # Define method
    YAML.load_file(CONFIG_FILE_PATH).each do |k, v|
      define_singleton_method k.to_sym do
        v
      end
    end

    def self.dir_path
      CONFIG_DIR_PATH
    end

    def self.file_path
      CONFIG_FILE_PATH
    end
  end
end