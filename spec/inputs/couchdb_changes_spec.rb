require "logstash/devutils/rspec/spec_helper"
require "ftw"
require "logstash/plugin"
require "logstash/json"
require "logstash/inputs/couchdb_changes"

module Helpers
  def createdb
    ftw = FTW::Agent.new
    ftw.put!("http://127.0.0.1:5984/db")
  end
  def deletedb
    ftw = FTW::Agent.new
    ftw.delete!("http://127.0.0.1:5984/db")
  end
  def populatedb
    ftw = FTW::Agent.new
    ftw.put!("http://127.0.0.1:5984/db/1", :body => '{"name":"Peter Parker"}')
    ftw.put!("http://127.0.0.1:5984/db/2", :body => '{"name":"Mary Jane Watson"}')
    ftw.put!("http://127.0.0.1:5984/db/3", :body => '{"name":"Captain America"}')
    ftw.put!("http://127.0.0.1:5984/db/4", :body => '{"name":"J. Jonah Jameson"}')
    ftw.put!("http://127.0.0.1:5984/db/5", :body => '{"name":"Otto Octavius"}')
    ftw.put!("http://127.0.0.1:5984/db/6", :body => '{"name":"May Parker"}')
    ftw.put!("http://127.0.0.1:5984/db/7", :body => '{"name":"Harry Osborne"}')
    ftw.put!("http://127.0.0.1:5984/db/8", :body => '{"name":"Norman Osborne"}')
    ftw.put!("http://127.0.0.1:5984/db/9", :body => '{"name":"Ben Parker"}')
    ftw.put!("http://127.0.0.1:5984/db/10", :body => '{"name":"Stan Lee"}')
  end
  def updatedocs
    ftw = FTW::Agent.new
    data = ""
    response = ftw.get!("http://127.0.0.1:5984/db/_changes?include_docs=true")
    response.read_body { |chunk| data << chunk }
    result = LogStash::Json.load(data)
    result["results"].each do |doc|
      upd = false
      body = doc["doc"]
      case doc["id"]
      when "1"
        body["Alter-ego"] = "Spider-man"
        upd = true
      when "5"
        body["Alter-ego"] = "Doctor Octopus"
        upd = true
      when "8"
        body["Alter-ego"] = "Green Goblin"
        upd = true
      end
      if upd
        ftw.put!("http://127.0.0.1:5984/db/#{doc["id"]}", :body => LogStash::Json.dump(body))
      end
    end
  end
  def deletedoc
    ftw = FTW::Agent.new
    data = ""
    response = ftw.get!("http://127.0.0.1:5984/db/9")
    response.read_body { |chunk| data << chunk }
    doc = LogStash::Json.load(data)
    ftw.delete!("http://127.0.0.1:5984/db/9?rev=#{doc["_rev"]}")
  end
  def createuser
    ftw = FTW::Agent.new
    ftw.put!("http://127.0.0.1:5984/_config/admins/logstash", :body => '"logstash"')
  end
  def deleteuser
    user = "logstash"
    pass = "logstash"
    auth = "#{user}:#{pass}@"
    ftw = FTW::Agent.new
    ftw.delete!("http://#{auth}127.0.0.1:5984/_config/admins/logstash")
  end
  def deleteindex
    ftw = FTW::Agent.new
    ftw.delete!("http://127.0.0.1:9200/couchdb_test")
  end
  def buildup
    # BEGIN: The following calls are a safety net in case of an aborted test
    deleteuser
    teardown
    # END
    createdb
    populatedb
  end
  def teardown
    deletedb
    deleteindex
    sequence = "/tmp/.couchdb_seq"
    File.delete(sequence) if File.exist?(sequence)
  end
end
    
describe "inputs/couchdb_changes", :elasticsearch => true, :couchdb => true do
  describe "Load couchdb documents", :elasticsearch => true, :couchdb => true do
    include Helpers
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"

    before do
      buildup
    end

    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG
      
    agent do
      # Verify the count
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      insist { count } == 10
      # Get the docs and do a couple spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["hits"]["hits"].each do |doc|
        # With no 'index_type' set, the document type should be the type
        # set on the input
        insist { doc["_type"] } == "couchdb"
        insist { doc["_index"] } == index
        case doc["_id"]
        when 1
          insist { doc["_source"]["name"] } == "Peter Parker"
        when 5
          insist { doc["_source"]["name"] } == "Otto Octavius"
        when 8
          insist { doc["_source"]["name"] } == "Norman Osborne"
        end
      end
    end
    after do
      teardown
    end
  end
    
  describe "Test document updates", :elasticsearch => true, :couchdb => true do
    include Helpers
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"

    before do
      buildup
      updatedocs
    end

    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG
      
    agent do
      # Verify the count (which should still be 10)
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      insist { count } == 10
      # Get the docs and do a couple more spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["hits"]["hits"].each do |doc|
        case doc["_id"]
        when 1
          insist { doc["_source"]["Alter-ego"] } == "Spider-man"
        when 5
          insist { doc["_source"]["Alter-ego"] } == "Doctor Octopus"
        when 8
          insist { doc["_source"]["Alter-ego"] } == "Green Goblin"
        end
      end
    end

    after do
      teardown
    end

  end

  describe "Test sequence", :elasticsearch => true, :couchdb => true do
    include Helpers
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"
    
    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG

    before do
      # This puts 10 docs into CouchDB
      buildup
      # And updates 3
      updatedocs
      # But let's set sequence to say we only read the 10th change
      # so it will start with change #11
      File.open(sequence, 'w') { |file| file.write("10") }
    end

    agent do
      # Verify the count (which should still be 10)
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      # We should only have 3 documents here because of the sequence change
      insist { count } == 3
      # Get the docs and do a couple more spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      counter = 0
      result["hits"]["hits"].each do |doc|
        case doc["_id"]
        when 1
          insist { doc["_source"]["Alter-ego"] } == "Spider-man"
        when 5
          insist { doc["_source"]["Alter-ego"] } == "Doctor Octopus"
        when 8
          insist { doc["_source"]["Alter-ego"] } == "Green Goblin"
        end
      end
      # Logstash should have updated the sequence to 13 after all this
      insist { File.read(sequence) } == "13"
    end

    after do
      teardown
    end

  end

  describe "Test document deletion", :elasticsearch => true, :couchdb => true do
    include Helpers
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"

    before do
      buildup
      deletedoc # from CouchDB
    end
    
    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG
    
    agent do
      # Verify the count (should now be 9)
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      insist { count } == 9
      # Get the docs and do a couple more spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      found = false
      result["hits"]["hits"].each do |doc|
        if doc["_id"] == "9"
          found = true
        end
      end
      insist { found } == false
    end
    
    after do
      teardown
    end

  end

  describe "Test authenticated connectivity", :elasticsearch => true, :couchdb => true do
    include Helpers
    user = "logstash"
    pass = "logstash"
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"

    before do
      buildup
      createuser
    end

    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
        username => "#{user}"
        password => "#{pass}"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG
      
    agent do
      # Verify the count
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      insist { count } == 10
      # Get the docs and do a couple spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["hits"]["hits"].each do |doc|
        case doc["_id"]
        when 3
          insist { doc["_source"]["name"] } == "Captain America"
        end
      end
    end
    
    after do
      deleteuser
      teardown
    end
  end

  describe "Test Secure Connection", :elasticsearch => true, :couchdb => true do
    include Helpers
    sequence = "/tmp/.couchdb_seq"
    index = "couchdb_test"
    ca_file = File.dirname(__FILE__) + "/ca_cert.pem"

    before do
      buildup
    end

    ftw = FTW::Agent.new

    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "localhost"
        port => 6984
        timeout => 2000
        always_reconnect => false
        sequence_path => "#{sequence}"
        type => "couchdb"
        secure => true
        ca_file => "#{ca_file}"
      }
    }
    output {
      elasticsearch {
        action => "%{[@metadata][action]}"
        document_id => "%{[@metadata][_id]}"
        host => "127.0.0.1"
        index => "#{index}"
        protocol => "http"
      }
    }
    CONFIG
      
    agent do
      # Verify the count
      ftw.post!("http://127.0.0.1:9200/#{index}/_refresh")
      data = ""
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_count?q=*")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      count = result["count"]
      insist { count } == 10
      # Get the docs and do a couple spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=10")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["hits"]["hits"].each do |doc|
        case doc["_id"]
        when 8
          insist { doc["_source"]["name"] } == "Norman Osborne"
        end
      end
    end

    after do
      teardown
    end
  end
  
end



