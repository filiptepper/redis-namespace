class Redis
  class NamespacePipeline < Redis::Namespace
    BUFFER_SIZE = 50_000

    def initialize(namespace_redis, namespace)
      @redis = namespace_redis
      @namespace = namespace
      @commands = []
    end

    alias :namespaced_method_missing :method_missing
    alias :namespaced_mget :mget
    alias :namespaced_mset :mset
    alias :namespaced_msetnx :msetnx

    def mget(*keys)
      raise RuntimeError.new("mget command is not allowed in pipeline")
    end

    def mset(keys)
      raise RuntimeError.new("mset command is not allowed in pipeline")
    end

    def msetnx(keys)
      raise RuntimeError.new("msetnx command is not allowed in pipeline")
    end

    def method_missing(command, *args, &block)
      @commands << [command, args]
    end

    def execute
      return if @commands.empty?

      commands = @commands.map do |command|
        name = command_name(command[0])
        [name] + prepare_args(name, *command[1])
      end
      @redis.call_command(commands)
      @commands.clear
    end


    private


    def command_name(command)
      case command
      when :[]=
        "set"
      when :[]
        "get"
      else
        command
      end
    end
  end
end