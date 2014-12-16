# encoding: utf-8

require "logstash/inputs/base"
require "logstash/namespace"
require "net/http"
require "uri"

# Stream events from the CouchDB _changes URI.
# Use event metadata to allow for upsert and
# document deletion.
class LogStash::Inputs::CouchDBChanges < LogStash::Inputs::Base
  config_name "couchdb_changes"
  milestone 1

  # IP or hostname of your CouchDB instance
  # Default: localhost
  config :host, :validate => :string, :default => "localhost"

  # Port of your CouchDB instance
  # Default: 5984
  config :port, :validate => :number, :default => 5984

  # The CouchDB db to connect to.
  # Required parameter.
  config :db, :validate => :string, :required => true

  # Connect to CouchDB's _changes feed securely (via https)
  # Default: false (via http)
  config :secure, :validate => :boolean, :default => false
  
  # Path to a CA certificate file, used to validate certificates
  config :ca_file, :validate => :path

  # Username, if authentication is needed to connect to 
  # CouchDB
  config :username, :validate => :string, :default => nil

  # Password, if authentication is needed to connect to 
  # CouchDB
  config :password, :validate => :string, :default => nil
  
  # Logstash connects to CouchDB's _changes with feed=continuous
  # The heartbeat is how often (in milliseconds) Logstash will ping
  # CouchDB to ensure the connection is maintained.  Changing this 
  # setting is not recommended unless you know what you are doing.
  config :heartbeat, :validate => :number, :default => 1000

  # Where to write the sincedb database (keeps track of the last
  # sequence of in the _changes stream). The default will write
  # to a file matching "$HOME/.couchdb_seq"
  config :sincedb_path, :validate => :string

  # If unspecified, Logstash will attempt to read the last sequence number
  # from the sincedb file.  If that is empty or non-existent, it will 
  # begin with 0 (the beginning).
  # 
  # If you specify this value, it is anticipated that you will 
  # only be doing so for an initial read under special circumstances
  # and that you will change this value back to nil after that
  # special ingest.
  config :initial_sequence, :validate => :number, :default => nil
  
  # Preserve the CouchDB document revision "_rev" value in the
  # output.
  # Default: false
  config :keep_revision, :validate => :boolean, :default => false
  
  # Future feature! Until implemented, changing this from the default 
  # will not do anything.
  #
  # Ignore attachments associated with CouchDB documents. Default: True
  config :ignore_attachments, :validate => :boolean, :default => true
  
  # Add type, tags, and add_field values to each event.
  #
  # Disabling this may only be desirable if you are passing all 
  # documents straight through to Elasticsearch with no filters
  # Default: true
  config :decorate_event, :validate => :boolean, :default => true
  
  # Reconnect flag.  When true, always try to reconnect after a failure
  # Default: true
  config :always_reconnect, :validate => :boolean, :default => true
  
  # Reconnect delay: time between reconnect attempts, in seconds.
  # Default: 10
  config :reconnect_delay, :validate => :number, :default => 10
  
  # Timeout: Number of milliseconds to wait for new data before
  # terminating the connection.  If a timeout is set it will disable
  # the heartbeat configuration option.
  # Default: nil
  config :timeout, :validate => :number

  public
  def register
    require "logstash/util/buftok"
    if @sincedb_path.nil?
      if ENV["HOME"].nil?
        @logger.error("No HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME in your environment, or set sincedb_path in " \
                      "in your Logstash config.")
        raise 
      end
      sincedb_dir = ENV["HOME"]
      @sincedb_path = File.join(sincedb_dir, ".couchdb_seq")

      @logger.info("No sincedb_path set, generating one...",
                   :sincedb_path => @sincedb_path)
    end

    @sincedb      = SinceDB::File.new(@sincedb_path)
    @feed         = 'continuous'
    @include_docs = 'true'
    @path         = '/' + @db + '/_changes'

    @scheme = @secure ? 'https' : 'http'

    @since = @initial_sequence ? @initial_sequence : @sincedb.read

    if !@username.nil? && !@password.nil?
      @userinfo = @username + ':' + @password
    else
      @userinfo = nil
    end
    
  end
  
  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      def read
        ::File.exists?(@sincedb_path) ? ::File.read(@sincedb_path).chomp.strip : 0
      end

      def write(since = nil)
        since = 0 if since.nil?
        ::File.write(@sincedb_path, since.to_s)
      end
    end
  end
  
  public
  def run(queue)
    buffer = FileWatch::BufferedTokenizer.new
    @logger.info("Connecting to CouchDB _changes stream at:", :host => @host.to_s, :port => @port.to_s, :db => @db)
    uri = build_uri
    Net::HTTP.start(@host, @port, :use_ssl => (@secure == true), :ca_file => @ca_file) do |http|
      request = Net::HTTP::Get.new(uri.request_uri)
      http.request request do |response|
        raise ArgumentError, "Database not found!" if response.code == "404"
        response.read_body do |chunk|
          buffer.extract(chunk).each do |changes|
            next if changes.chomp.empty?
            event = build_event(changes)
            @logger.debug("event", :event => event.to_hash_with_metadata) if @logger.debug?
            decorate(event) if @decorate_event
            unless event["empty"]
              queue << event
              @since = event['@metadata']['seq']
              @sincedb.write(@since.to_s)
            end
          end
        end
      end
    end
  rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Errno::EHOSTUNREACH, Errno::ECONNREFUSED,
    Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
    @logger.error("Connection problem encountered: Retrying connection in 10 seconds...", :error => e.to_s)
    retry if reconnect?
  rescue Errno::EBADF => e
    @logger.error("Connction refused due to bad file descriptor: ", :error => e.to_s)
    retry if reconnect?
  rescue ArgumentError => e
    @logger.error("Unable to connect to database", :db => @db, :error => e.to_s)
    retry if reconnect?
  end
  
  private
  def build_uri
    options = {:feed => @feed, :include_docs => @include_docs, :since => @since}
    options = options.merge(@timeout ? {:timeout => @timeout} : {:heartbeat => @heartbeat})
    URI::HTTP.build(:scheme => @scheme, :userinfo => @userinfo, :host => @host, :port => @port, :path => @path, :query => URI.encode_www_form(options))
  end

  private
  def reconnect?
    sleep(@always_reconnect ? @reconnect_delay : 0)
    @always_reconnect
  end

  private
  def build_event(line)
    # In lieu of a codec, build the event here
    line = LogStash::Json.load(line)
    return LogStash::Event.new({"empty" => true}) if line.has_key?("last_seq")
    hash = Hash.new
    hash['@metadata'] = { '_id' => line['doc']['_id'] }
    if line['doc']['_deleted']
      hash['@metadata']['action'] = 'delete'
    else
      hash['doc'] = line['doc']
      hash['@metadata']['action'] = 'update'
      hash['doc'].delete('_id')
      hash['doc_as_upsert'] = true
      hash['doc'].delete('_rev') unless @keep_revision
    end
    hash['@metadata']['seq'] = line['seq']
    event = LogStash::Event.new(hash)
    @logger.debug("event", :event => event.to_hash_with_metadata) if @logger.debug?
    event
  end
end