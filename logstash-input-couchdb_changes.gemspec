Gem::Specification.new do |s|

  s.name            = 'logstash-input-couchdb_changes'
  s.version         = ::File.read('version').split("\n").first
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Streams events from CouchDB's `_changes` URI"
  s.description     = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir["lib/**/*","spec/**/*","*.gemspec","*.md","CONTRIBUTORS","Gemfile","LICENSE","NOTICE.TXT", "vendor/jar-dependencies/**/*.jar", "vendor/jar-dependencies/**/*.rb", "VERSION", "version", "docs/**/*"]

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "stud", '>= 0.0.22'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'json'

  s.add_development_dependency 'ftw', '~> 0.0.42'
  # ftw depends on http_parser.rb without a version constraint. Pin to 0.6.x so
  # bundler resolves the precompiled java-platform gem instead of upgrading to a
  # source-only release (0.8.1) that fails to compile in the CI image (no toolchain).
  # 0.6.0 is the last release shipping a -java gem; see
  # https://github.com/tmm1/http_parser.rb/issues/72
  s.add_development_dependency 'http_parser.rb', '~> 0.6.0'
  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'insist'
  s.add_development_dependency 'logstash-output-elasticsearch'

end
