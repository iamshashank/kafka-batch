require "bundler/gem_tasks" if File.exist?("kafka_batch.gemspec")

begin
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new(:spec)
  task default: :spec
rescue LoadError
  # rspec not available (e.g. production install) – skip the test tasks
end
