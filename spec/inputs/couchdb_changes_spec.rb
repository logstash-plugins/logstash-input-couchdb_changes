require "spec_helper"
require "ftw"
require "logstash/plugin"
require "logstash/json"
require "logstash/inputs/couchdb_changes"

describe "inputs/couchdb_changes", :elasticsearch => true do
  describe "CouchDB preparations", :elasticsearch => true do
    it "checks that CouchDB responds" do
      ftw = FTW::Agent.new
      data = ""    
      response = ftw.get!("http://127.0.0.1:5984/")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      insist { result["couchdb"] } == "Welcome"
    end
  
    it "will delete the CouchDB, if it exists" do
      result = true
      ftw = FTW::Agent.new
      data = ""    
      response = ftw.delete!("http://127.0.0.1:5984/db")
      insist { result } == true
    end

    it "should create a new CouchDB for testing" do
      ftw = FTW::Agent.new
      data = ""    
      response = ftw.put!("http://127.0.0.1:5984/db")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      insist { result["ok"] } == true
    end
  end
  
  describe "CouchDB Initial Loading", :elasticsearch => true do
    it "should create 10 documents in CouchDB" do
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
      response = ftw.get!("http://127.0.0.1:5984/db/_changes")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      insist { result["results"].length } == 10
    end
  end

  describe "Load first couchdb documents into elasticsearch", :elasticsearch => true do
    ftw = FTW::Agent.new
    ftw.delete!("http://127.0.0.1:9200/couchdb_test")
    sincedb = "/tmp/.couchdb_seq"
    index = "couchdb_test"
    File.delete(sincedb) if File.exist?(sincedb)
  
    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sincedb_path => "#{sincedb}"
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
  end
  
  describe "Document Updates", :elasticsearch => true do
    it "should update our spot-check docs" do
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
      data = ""
      response = ftw.get!("http://127.0.0.1:5984/db/_changes?include_docs=true")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["results"].each do |doc|
        case doc["id"]
        when "1"
          insist { doc["id"] } == "1"
          insist { doc["doc"]["Alter-ego"] } == "Spider-man"
        when "5"
          insist { doc["id"] } == "5"
          insist { doc["doc"]["Alter-ego"] } == "Doctor Octopus"
        when "8"
          insist { doc["id"] } == "8"
          insist { doc["doc"]["Alter-ego"] } == "Green Goblin"
        end
      end
    end
    
    describe "Update our elasticsearch docs", :elasticsearch => true do
      ftw = FTW::Agent.new
      sincedb = "/tmp/.couchdb_seq"
      index = "couchdb_test"
  
      config <<-CONFIG
      input {
        couchdb_changes {
          db => "db"
          host => "127.0.0.1"
          timeout => 2000
          always_reconnect => false
          sincedb_path => "#{sincedb}"
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
    end
  end

  describe "Document deletes", :elasticsearch => true do
    it "should delete a doc" do
      ftw = FTW::Agent.new
      data = ""
      response = ftw.get!("http://127.0.0.1:5984/db/9")
      response.read_body { |chunk| data << chunk }
      doc = LogStash::Json.load(data)
      ftw.delete!("http://127.0.0.1:5984/db/9?rev=#{doc["_rev"]}")
      data = ""
      response = ftw.get!("http://127.0.0.1:5984/db/_changes?include_docs=true")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["results"].each do |doc|
        case doc["seq"]
        when "14"
          insist { doc["id"] } == "9"
          insist { doc["doc"]["_deleted"] } == true
        end
      end
    end
    
    describe "Delete from elasticsearch docs", :elasticsearch => true do
      ftw = FTW::Agent.new
      sincedb = "/tmp/.couchdb_seq"
      index = "couchdb_test"
  
      config <<-CONFIG
      input {
        couchdb_changes {
          db => "db"
          host => "127.0.0.1"
          timeout => 2000
          always_reconnect => false
          sincedb_path => "#{sincedb}"
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
    end
  end
  
  describe "HTTP Authentication", :elasticsearch => true do
    user = "logstash"
    pass = "logstash"
    auth = "#{user}:#{pass}@"
    
    it "should have no admin users" do
      ftw = FTW::Agent.new
      ftw.delete!("http://#{auth}127.0.0.1:5984/_config/admins/logstash")
      data = ""
      response = ftw.get!("http://127.0.0.1:5984/_config/admins")
      response.read_body { |chunk| data << chunk }
      insist { data.chomp } == '{}'
    end
    
    it "should create an admin user" do
      ftw = FTW::Agent.new
      data = ""
      ftw.put!("http://127.0.0.1:5984/_config/admins/logstash", :body => '"logstash"')
      response = ftw.get!("http://#{auth}127.0.0.1:5984/_config/admins")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      insist { result.has_key?(user) } == true
    end
    
    it "should create a document" do
      ftw = FTW::Agent.new
      data = ""
      ftw.put!("http://#{auth}127.0.0.1:5984/db/11", :body => '{"name":"Tony Stark"}')
      response = ftw.get!("http://#{auth}127.0.0.1:5984/db/_changes?include_docs=true")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["results"].each do |doc|
        case doc["seq"]
        when "15"
          insist { doc["id"] } == "11"
          insist { doc["doc"]["name"] } == "Tony Stark"
        end
      end
    end
  end
  
  describe "insert new document into elasticsearch", :elasticsearch => true do
    user = "logstash"
    pass = "logstash"
    
    ftw = FTW::Agent.new
    sincedb = "/tmp/.couchdb_seq"
    index = "couchdb_test"
  
    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "127.0.0.1"
        timeout => 2000
        always_reconnect => false
        sincedb_path => "#{sincedb}"
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
        when 11
          insist { doc["_source"]["name"] } == "Tony Stark"
        end
      end
    end
  end
    
  describe "Cleanup after authentication test", :elasticsearch => true do
    user = "logstash"
    pass = "logstash"
    auth = "#{user}:#{pass}@"
    
    it "should delete the admin user" do
      ftw = FTW::Agent.new
      ftw.delete!("http://#{auth}127.0.0.1:5984/_config/admins/logstash")
      data = ""
      response = ftw.get!("http://127.0.0.1:5984/_config/admins")
      response.read_body { |chunk| data << chunk }
      insist { data.chomp } == '{}'
    end    
  end

  describe "SSL Connectivity", :elasticsearch => true do
    it "should create a document (for SSL testing)" do
      ftw = FTW::Agent.new
      data = ""
      ftw.put!("http://127.0.0.1:5984/db/12", :body => '{"name":"Bruce Banner"}')
      response = ftw.get!("http://127.0.0.1:5984/db/_changes?include_docs=true")
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["results"].each do |doc|
        case doc["seq"]
        when "16"
          insist { doc["id"] } == "12"
          insist { doc["doc"]["name"] } == "Bruce Banner"
        end
      end
    end
  end
  
  describe "pull document from CouchDB (via SSL) & insert into elasticsearch", :elasticsearch => true do
    
    ftw = FTW::Agent.new
    sincedb = "/tmp/.couchdb_seq"
    index = "couchdb_test"
    ca_file = File.dirname(__FILE__) + "/ca_cert.pem"
  
    config <<-CONFIG
    input {
      couchdb_changes {
        db => "db"
        host => "localhost"
        port => 6984
        timeout => 2000
        always_reconnect => false
        sincedb_path => "#{sincedb}"
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
      insist { count } == 11
      # Get the docs and do a couple spot checks
      response = ftw.get!("http://127.0.0.1:9200/#{index}/_search?q=*&size=11")
      data = ""
      response.read_body { |chunk| data << chunk }
      result = LogStash::Json.load(data)
      result["hits"]["hits"].each do |doc|
        case doc["_id"]
        when 12
          insist { doc["_source"]["name"] } == "Bruce Banner"
        end
      end
    end
  end
end



