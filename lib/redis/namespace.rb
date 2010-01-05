require 'redis'

class Redis
  class Namespace
    # Generated from http://code.google.com/p/redis/wiki/CommandReference
    # using the following jQuery:
    #
    # $('.vt li a').map(function(){ return $(this).text().toLowerCase() }).sort()
    COMMANDS = [
      "auth",
      "bgrewriteaof",
      "bgsave",
      "blpop",
      "brpop",
      "dbsize",
      "decr",
      "decrby",
      "del",
      "exists",
      "expire",
      "flushall",
      "flushdb",
      "get",
      "getset",
      "incr",
      "incrby",
      "info",
      "keys",
      "lastsave",
      "lindex",
      "llen",
      "lpop",
      "lpush",
      "lrange",
      "lrem",
      "lset",
      "ltrim",
      "mget",
      "monitor",
      "move",
      "mset",
      "msetnx",
      "quit",
      "randomkey",
      "rename",
      "renamenx",
      "rpop",
      "rpoplpush",
      "rpush",
      "sadd",
      "save",
      "scard",
      "sdiff",
      "sdiffstore",
      "select",
      "set",
      "setnx",
      "shutdown",
      "sinter",
      "sinterstore",
      "sismember",
      "slaveof",
      "smembers",
      "smove",
      "sort",
      "spop",
      "srandmember",
      "srem",
      "sunion",
      "sunionstore",
      "ttl",
      "type",
      "zadd",
      "zcard",
      "zincrby",
      "zrange",
      "zrangebyscore",
      "zrem",
      "zremrangebyscore",
      "zrevrange",
      "zscore",
      "[]",
      "[]="
    ]

    def initialize(namespace, options = {})
      @namespace = namespace
      @redis = options[:redis]
    end

    # Ruby defines a now deprecated type method so we need to override it here
    # since it will never hit method_missing
    def type(key)
      method_missing(:type, key)
    end

    def mapped_mget(*keys)
      result = {}
      mget(*keys).each do |value|
        key = keys.shift
        result.merge!(key => value) unless value.nil?
      end
      result
    end

    def mget(*keys)
      keys = keys.map { |key| "#{@namespace}:#{key}"} if @namespace
      call_command([:mget] + keys)
    end

    def mset(keys)
      call_command([:mset] + [prepare_mset(keys)])
    end

    def msetnx(keys)
      call_command([:msetnx] + [prepare_mset(keys)])
    end

    def pipelined(&block)
      pipeline = NamespacePipeline.new(self, @namespace)
      yield pipeline
      pipeline.execute
    end

    def method_missing(command, *args, &block)
      @redis.send(command, *prepare_args(command, *args), &block)
    end


    private


    def prepare_args(command, *args)
      if COMMANDS.include?(command.to_s) && args[0]
        args[0] = "#{@namespace}:#{args[0]}"
      end

      args
    end

    def prepare_mset(keys)
      if @namespace
        namespaced_keys = {}
        keys.each { |key, value| namespaced_keys["#{@namespace}:#{key}"] = value }
        keys = namespaced_keys
      end

      keys
    end
  end
end

require File.join(File.dirname(__FILE__), "namespace_pipeline")