module InfluxUtil
  module Misc
    def self.get_file_content(path)
      content = ''
      File.open(path, 'r') { |f| content = f.read.strip }
      content
    end

    def self.write_file(path, content)
      File.open(path, 'w') do |f|     
        f.write(content)   
      end if !File.exist?(path)
    end
  end
end