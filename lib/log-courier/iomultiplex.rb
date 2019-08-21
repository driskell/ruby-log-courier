# encoding: utf-8

# Copyright 2014 Jason Woods.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'cabin'
require 'nio'

module IOMultiplex
  class NotEnoughData < StandardError; end

  class Logger
    def self.create(logger, static_args = {})
      if logger.is_a? IOMultiplex::Logger
        logger.clone_with_args(static_args)
      else
        IOMultiplex::Logger.new(logger, static_args)
      end
    end

    def initialize(logger, static_args)
      @logger = logger
      @static_args = static_args
    end

    def clone_with_args(args)
      IOMultiplex::Logger.create(@logger, args)
    end

    def add_static_args(args = {})
      @static_args = @static_args.merge(args)
    end

    def debug(message, args = {})
      @logger.debug message, args.merge(@static_args)
    end

    def info(message, args = {})
      @logger.info message, args.merge(@static_args)
    end

    def warn(message, args = {})
      @logger.warn message, args.merge(@static_args)
    end

    def error(message, args = {})
      @logger.debug message, args.merge(@static_args)
    end
  end

  class StringBuffer
    attr_accessor :length

    def initialize
      reset
      return
    end

    def reset
      @buffer = []
      @length = 0
      return
    end

    def push(data)
      data = data.to_s
      @buffer.push data
      @length += data.bytesize
    end
    alias << push

    def read(n)
      process n, false
    end

    def peek
      return '' if length == 0
      s = @buffer[0].dup
      # Coalesce small writes
      i = 1
      while @buffer[i] && s.bytesize + @buffer[i].bytesize < 4096
        s << @buffer[i]
        i += 1
      end
      s
    end

    def shift(n)
      process n, true
    end

    private

    def process(n, discard=false)
      data = nil unless discard
      n = n.to_i
      return '' if n <= 0 or length == 0
      while n != 0
        if @buffer[0].bytesize > n
          s = @buffer[0].slice!(0, n)
        else
          s = @buffer.shift
        end
        n -= s.bytesize
        @length -= s.bytesize
        unless discard
          if data
            data << s
          else
            data = s
          end
        end
      end
      return if discard
      data
    end
  end

  class Timer
    attr_accessor :time

    def inspect
      return @time.to_i.to_s
    end
  end

  class IOReactor
    attr_reader :id
    attr_reader :io
    attr_reader :mode
    attr_reader :peer

    def initialize(io, mode='rw', id=nil)
      @io = io
      @attached = false
      @read_buffer = StringBuffer.new
      @write_buffer = StringBuffer.new
      @mode = mode
      @read_on_write = false
      @write_immediately = false
      if id
        @id = id
      elsif @io.respond_to?(:peeraddr)
        begin
          peer = @io.peeraddr(:numeric)
          @id = "#{peer[2]}:#{peer[1]}"
        rescue NotImplementedError
          @id = @io.inspect
        end
      else
        @id = @io.inspect
      end
      return
    end

    def addr
      @io.addr
    end

    def peeraddr
      @io.peeraddr
    end

    def attached(multiplexer, logger)
      @multiplexer = multiplexer
      @logger = IOMultiplex::Logger.create(logger, client: @id)
      @attached = true
      return
    end

    def detached(multiplexer)
      @attached = false
      return
    end

    def handle_read
      force = false

      begin
        read_action
      rescue IO::WaitReadable, Errno::EINTR, Errno::EAGAIN
        @multiplexer.wait_read self if force_read?
      rescue EOFError
        # TODO: This means EOF is called BEFORE we've read all data...
        @logger.debug 'EOF'
        eof if respond_to?(:eof)
        @multiplexer.stop_read self
        close
      rescue IOError, Errno::ECONNRESET, OpenSSL::SSL::SSLError => e
        exception e if respond_to?(:exception)
        force_close
        return false
      else
        force = force_read?
      end

      handle_data if @read_buffer.length != 0

      if force and not @pause
        @multiplexer.stop_read self
        @multiplexer.force_read self
      end

      # If we were wanting a write, we no longer need it now
      if @read_on_write
        @read_on_write = false
        unless write_full?
          @multiplexer.wait_read self
        end
      end
      true
    rescue IO::WaitWritable
      # TODO: handle_data should really be triggered
      # This captures an OpenSSL read wanting a write
      @multiplexer.stop_read self
      @multiplexer.wait_write self
      @read_on_write = true
      @write_immediately = false
      false
    end

    def handle_data
      process
    rescue NotEnoughData
    else
      # To balance threads, process is allowed to return without processing
      # all data, and will get called again after one round even if read not
      # ready again. This allows us to spread processing more evenly if the
      # processor is smart
      # If the read buffer is >=4096 we can also skip read polling otherwise
      # we will add another 4096 bytes and not process it as fast as we are
      # adding the data
      # Also, allow the processor to pause read which has the same effect -
      # it is expected a timer or something will then resume read - this can be
      # if the client is waiting on a background thread
      # NOTE: Processor should be careful, if it processes nothing this can
      #       cause a busy loop
      if @read_buffer.length != 0
        @multiplexer.defer self unless @pause
      end
      if @read_buffer.length >= 4096 or @pause
        @multiplexer.stop_read self
      else
        @multiplexer.wait_read self
      end
    end

    def handle_write
      if @read_on_write
        handle_read
        return if @read_on_write
        if @write_buffer.length == 0
          @multiplexer.wait_write self
          return
        end
      elsif @write_buffer.length == 0
        @multiplexer.stop_write self
        @write_immediately = true
        return
      end

      begin
        n = write_nonblock(@write_buffer.peek)
      rescue IO::WaitReadable
      rescue IO::WaitWritable, Errno::EINTR, Errno::EAGAIN
        # Keep waiting for write
        return
      rescue IOError, Errno::ECONNRESET, OpenSSL::SSL::SSLError => e
        exception e if respond_to?(:exception)
        force_close
        return
      end

      @write_buffer.shift n

      if @write_buffer.length == 0
        if @close_scheduled
          force_close
        elsif not can_write_immediately?
          @multiplexer.stop_write self
        end
      elsif not write_full?
        @logger.debug 'write buffer no longer full, resuming read', count: @write_buffer.length
        @multiplexer.wait_read self
      end
      return
    end

    def read(n)
      fail RuntimeError, 'Socket is not attached', nil unless @attached
      fail IOError, 'Socket is closed' if @io.closed?
      fail NotEnoughData, 'Not enough data', nil if @read_buffer.length < n

      @read_buffer.read(n)
    end

    def discard
      @read_buffer.reset
      return
    end

    def pause
      @logger.debug 'pause read'
      @pause = true
      return
    end

    def resume
      return unless @pause
      @logger.debug 'resume read'
      @pause = false
      @multiplexer.defer self if @read_buffer.length != 0
      if force_read?
        @multiplexer.force_read self
      elsif @read_buffer.length < 4096
        @multiplexer.wait_read self
      end
      return
    end

    def write(data)
      fail RuntimeError, 'Socket is not attached' unless @attached
      fail IOError, 'Socket is closed' if @io.closed?

      @write_buffer.push data
      @multiplexer.wait_write self

      if @write_immediately
        handle_write if can_write_immediately?
        @write_immediately = false
      end

      # Write buffer too large - pause read polling
      if write_full?
        @logger.debug 'write buffer full, pausing read', count: @write_buffer.length
        @multiplexer.stop_read self
      end
      return
    end

    def write_full?
      # TODO: Make write buffer max customisable?
      @write_buffer.length >= 16*1024
    end

    def close
      @read_buffer = ''
      if @mode == 'rw' or @mode == 'w'
        @close_scheduled = true
      else
        force_close
      end
      return
    end

    def force_close
      @multiplexer.remove self
      @io.close unless @io.closed?
      return
    end

    def can_write_immediately?
      true
    end

    private

    # Can be overridden for OpenSSL
    def read_nonblock(n)
      @logger.debug 'read_nonblock', count: n
      @io.read_nonblock(n)
    end

    # Can be overridden for OpenSSL
    def write_nonblock(data)
      @logger.debug 'write_nonblock', count: data.bytesize
      @io.write_nonblock(data)
    end

    def read_action
      @read_buffer << read_nonblock(4096)
      return
    end

    # Some socket types (such as OpenSSL within JRuby) can buffer data
    # internally and provide no way to check (OpenSSL MRI does though).
    # This means when we finish this read, and poll the socket, there's a
    # possibility it does not signal as read ready, but there is data
    # available (within the internal buffer).
    # As such, allow those socket types to use force read, which will defer
    # read rather than poll read, and rely on the WaitReadable throwing to
    # know when to stop reading and start polling again.
    def force_read?
      false
    end
  end

  module SSLCommon
    def peer_cert
      @ssl.peer_cert
    end

    def peer_cert_cn
      return nil unless peer_cert
      peer_cert.subject.to_a.find do |oid, value|
        return value if oid == "CN"
        nil
      end
    end

    def handshake_completed?
      @handshake_completed
    end

    def can_write_immediately?
      false
    end

    private

    def force_read?
      true
    end
  end

  class SSLIO < IOReactor
    include SSLCommon

    def initialize(io, mode='rw', id=nil, ssl_ctx=nil)
      super io, mode, id
      @ssl = OpenSSL::SSL::SSLSocket.new(io, ssl_ctx)
      @ssl_ctx = ssl_ctx
      @handshake_completed = false
      return
    end

    private

    def read_nonblock(n)
      @logger.debug 'SSL read_nonblock', count: n
      @ssl.read_nonblock n
    end

    def write_nonblock(data)
      @logger.debug 'SSL write_nonblock', count: data.bytesize
      @ssl.write_nonblock data
    end

    def read_action
      return super if @handshake_completed

      @ssl.accept_nonblock
      @handshake_completed = true
      handshake_completed if respond_to?(:handshake_completed)
      @logger.debug 'Handshake completed'
      super
    end
  end

  class SSLUpgradingIO < IOReactor
    include SSLCommon

    def initialize(io, mode, id=nil)
      super io, mode, nil
      @ssl_enabled = false
    end

    def start_ssl(ssl_ctx)
      fail RuntimeError, 'SSL already started' if @ssl_enabled
      @ssl = OpenSSL::SSL::SSLSocket.new(@io, ssl_ctx)
      @ssl_ctx = ssl_ctx
      @ssl_enabled = true
      @handshake_completed = false
      @logger.debug 'Upgrading connection to SSL'
      return
    end

    def can_write_immediately?
      not @ssl_enabled
    end

    private

    def read_nonblock(n)
      return super unless @ssl_enabled
      @ssl.read_nonblock n
    end

    def write_nonblock(data)
      return super unless @ssl_enabled
      @ssl.write_nonblock data
    end

    def read_action
      return super unless @ssl_enabled and not @handshake_completed

      @ssl.accept_nonblock
      @handshake_completed = true
      handshake_completed if respond_to?(:handshake_completed)
      @logger.debug 'Handshake completed'
      super
    end
  end

  class PipeProxy < IOReactor
    def initialize(io, direction, id=nil)
      super io, direction, id
      @forward = nil
      return
    end

    def forward(obj, callback, args=[])
      @forward = obj
      @callback = callback
      @args = args
      return
    end

    def process
      if @forward
        @forward.send @callback, *@args
        return
      end
      discard
      return
    end
  end

  class PipeForwarder
    attr_reader :reader
    attr_reader :writer

    def initialize(id)
      reader, writer = IO.pipe()
      @reader = PipeProxy.new(reader, 'r', id + '-Reader')
      @writer = PipeProxy.new(writer, 'w', id + '-Writer')
      return
    end
  end

  # A single multiplexer that can process hundreds of clients in a single thread
  class Multiplexer
    LOOKUP_WAIT_READ = 1
    LOOKUP_WAIT_WRITE = 2
    LOOKUP_TIMER = 4
    LOOKUP_NOT_CONNECTION = 8
    LOOKUP_DEFER = 16
    LOOKUP_FORCE_READ = 32

    def initialize(options={})
      if options[:logger]
        @logger = IOMultiplex::Logger.create(options[:logger], multiplexer: @id)
      else
        @logger = IOMultiplex::Logger.create(Cabin::Channel.get(IOMultiplex), multiplexer: @id)
      end

      @id = options[:id] || object_id

      @nio = NIO::Selector.new
      @mutex = Mutex.new
      @timers = []
      @post_processing = []
      @lookup = Hash.new do |h, k|
        h[k] = 0
      end
      @shutdown = false
      @callbacks = Queue.new
      @connections = 0

      @controller = PipeForwarder.new('Controller')
      @controller.reader.forward self, :controller
      add @controller.reader, false
      return
    end

    def run
      until @shutdown do
        if @post_processing.length != 0
          next_timer = 0
        elsif @timers.length == 0
          next_timer = nil
        else
          next_timer = (@timers[0].time - Time.now).ceil
          next_timer = 0 if next_timer < 0
        end

        # Run deferred after we finish this loop
        # New defers then carry to next loop
        if @post_processing.length == 0
          post_processing = nil
        else
          post_processing = @post_processing
          @post_processing = []
        end

        start_time = Time.now

        @nio.select(next_timer) do |monitor|
          next unless @lookup.key?(monitor.value)
          if monitor.readable?
            sub_start_time = Time.now
            check_write = monitor.value.handle_read
            duration = ((Time.now - sub_start_time) * 1000).to_i
            @logger.warn 'Slow handle_write', :duration_ms => duration, :client => monitor.value.id if duration > 100

            # Skip if handle_read returned false
            next unless check_write
          end

          # Check we didn't remove the socket before we call monitor.writable?
          # otherwise it will throw a Java CancelledKeyException wrapped in
          # NativeException because we tried to access a removed monitor
          next unless @lookup.key?(monitor.value)
          if monitor.writable?
            sub_start_time = Time.now
            monitor.value.handle_write
            duration = ((Time.now - sub_start_time) * 1000).to_i
            @logger.warn 'Slow handle_write', :duration_ms => duration, :client => monitor.value.id if duration > 100
          end
        end

        duration = ((Time.now - start_time) * 1000).to_i
        if duration > 100
          now = Time.now
          if @timers.length == 0
            timer_due = 'None'
            timer_delay = 'N/A'
          else
            timer_due = @timers[0].time.to_f.ceil
            if now > @timers[0].time
              timer_delay = ((now - @timers[0].time) * 1000).to_i
            else
              timer_delay = 'None'
            end
          end
          @logger.warn 'Slow NIO select', :duration_ms => duration, :now => now.to_i, :timer_due => timer_due, :timer_delay => timer_delay, :connections => @connections
        end

        # Cannot use delete_if as we must remain compatible with JRuby 1.7.11
        # which corrupts an array if you break from a delete_if or reject!
        if @timers.length != 0
          now = Time.now
          while @timers.length != 0
            break if @timers[0].time > now
            timer = @timers.shift
            remove_timer timer
            timer.timer if timer.respond_to?(:timer)
          end
        end

        if post_processing
          post_processing.each do |client|
            next unless @lookup.has_key?(client)
            force_read = @lookup[client] & LOOKUP_FORCE_READ != 0
            @lookup[client] ^= LOOKUP_DEFER | LOOKUP_FORCE_READ
            start_time = Time.now
            if force_read
              client.handle_read
            else
              client.handle_data
            end
            duration = ((Time.now - start_time) * 1000).to_i
            @logger.warn 'Slow post processing', :duration_ms => duration, :client => client.id if duration > 100
          end
        end
      end

      @logger.debug 'Shutdown'

      # Forced shutdown
      @lookup.each_key do |client|
        # TODO: Timers? Timers need refactoring
        client.force_close if client.respond_to?(:force_close)
      end
      return
    end

    def add(client, increase_connection_count=true)
      fail ArgumentError, 'Client must be an instance of IOMultiplex::IOReactor' unless client.is_a?(IOReactor)
      fail ArgumentError, 'Client is already attached' if @lookup.has_key?(client)

      client.attached self, @logger
      @lookup[client] = 0
      wait_read client if client.mode == 'rw' or client.mode == 'r'
      wait_write client if client.can_write_immediately? and (client.mode == 'rw' or client.mode == 'w')

      if increase_connection_count
        @mutex.synchronize do
          @connections += 1
        end
      else
        @lookup[client] |= LOOKUP_NOT_CONNECTION
      end
      return
    end

    def remove(client)
      lookup = @lookup[client]
      return unless lookup

      if lookup & LOOKUP_NOT_CONNECTION == 0
        @mutex.synchronize do
          @connections -= 1
        end
      end

      client.detached self
      @nio.deregister client.io
      @post_processing.delete client
      if @lookup[client] & LOOKUP_TIMER != 0
        remove_timer client
      end
      @lookup.delete client
      return
    end

    def connections
      @mutex.synchronize do
        @connections
      end
    end

    # TODO NOW: Refactor
    def add_timer(timer, time)
      fail ArgumentError, 'Timer must be an instance of Timer' unless timer.is_a?(Timer)

      @logger.warn 'Add timer', :time => time.to_i if @debug_timers

      lookup = @lookup[timer]
      @timers.delete timer if lookup && lookup & LOOKUP_TIMER != 0
      j = -1
      @timers.each_index do |i|
        if @timers[i].time > time
          j = i
          break
        end
      end
      @timers.insert j, timer
      timer.time = time
      @lookup[timer] |= LOOKUP_TIMER
      return
    end

    def remove_timer(timer)
      lookup = @lookup[timer]
      return unless lookup & LOOKUP_TIMER != 0

      @lookup[timer] ^= LOOKUP_TIMER
      @timers.delete timer
      @lookup.delete timer if @lookup[timer] == 0
      return
    end

    def shutdown
      # Bypass write() and block
      # (we never registered a multiplexer to handle send)
      @controller.writer.io.write 'S'
      return
    end

    def callback(&block)
      @callbacks.push block
      # Bypass write() and block
      # (we never registered a multiplexer to handle send)
      @controller.writer.io.write 'C'
      return
    end

    def wait_read(client)
      lookup = @lookup[client]
      fail ArgumentError 'Client is not within the multiplexer' unless lookup

      return if lookup & LOOKUP_WAIT_READ != 0
      @lookup[client] |= LOOKUP_WAIT_READ
      register client, @lookup[client]
      return
    end

    def wait_write(client)
      lookup = @lookup[client]
      fail ArgumentError 'Client is not within the multiplexer' unless lookup

      return if lookup & LOOKUP_WAIT_WRITE != 0
      @lookup[client] |= LOOKUP_WAIT_WRITE
      register client, @lookup[client]
      return
    end

    def stop_read(client)
      lookup = @lookup[client]
      fail ArgumentError 'Client is not within the multiplexer' unless lookup

      return unless lookup & LOOKUP_WAIT_READ != 0
      @lookup[client] ^= LOOKUP_WAIT_READ
      register client, @lookup[client]
      return
    end

    def stop_write(client)
      lookup = @lookup[client]
      fail ArgumentError 'Client is not within the multiplexer' unless lookup

      return unless lookup & LOOKUP_WAIT_WRITE != 0
      @lookup[client] ^= LOOKUP_WAIT_WRITE
      register client, @lookup[client]
      return
    end

    def status(client)
      return @lookup[client]
    end

    def defer(client)
      post_process client, LOOKUP_DEFER
    end

    def force_read(client)
      post_process client, LOOKUP_FORCE_READ
    end

    private

    def controller
      10.times do
        case @controller.reader.read(1)
        when 'S'
          @logger.debug 'Multiplexer shutting down'
          @shutdown = true
        when 'C'
          length = @callbacks.length
          while length != 0
            @callbacks.pop.call
            length -= 1
          end
        end
      end
      return
    end

    def register(client, flags)
      @nio.deregister client.io
      if flags & LOOKUP_WAIT_READ == 0 and flags & LOOKUP_WAIT_WRITE == 0
        return
      elsif flags & LOOKUP_WAIT_READ == 0
        interests = :w
      elsif flags & LOOKUP_WAIT_WRITE == 0
        interests = :r
      else
        interests = :rw
      end

      monitor = @nio.register(client.io, interests)
      monitor.value = client
    end

    def post_process(client, flag)
      lookup = @lookup[client]
      fail ArgumentError 'Client is not within the multiplexer' unless lookup

      return if lookup & flag != 0
      @post_processing.push client
      @lookup[client] |= flag
      return
    end
  end

  class MultiplexerPool
    def initialize(options)
      ['parent', 'num_workers'].each do |k|
        fail ArgumentError, "Required option missing: #{k}" unless options[k.to_sym]
      end

      if options[:logger]
        @logger = IOMultiplex::Logger.create(options[:logger], multiplexer_pool: @id)
      else
        @logger = IOMultiplex::Logger.create(Cabin::Channel.get(IOMultiplex), multiplexer_pool: @id)
      end

      @id = options[:id] || object_id

      @parent = options[:parent]
      @workers = []
      @pipes = []
      @queues = []
      @threads = []

      options[:num_workers].times do |i|
        @workers[i] = Multiplexer.new(logger: @logger, id: "#{options[:id]}-Worker-#{i}")
        @pipes[i] = PipeForwarder.new("#{options[:id]}-Worker-#{i}")
        @pipes[i].reader.forward self, :process, i
        @workers[i].add @pipes[i].reader, false
        @parent.add @pipes[i].writer, false
        @queues[i] = Queue.new

        @threads[i] = Thread.new(@workers[i]) do |mutliplexer|
          mutliplexer.run
        end
      end
      return
    end

    def distribute(client)
      selected = [nil, nil]
      @workers.each_index do |i|
        connections = @workers[i].connections
        # TODO: Make customisable this maxmium
        next if connections >= 1000
        selected = [i, connections] if !selected[0] || selected[1] > connections
      end

      return false unless selected[0]

      @queues[selected[0]] << client
      @pipes[selected[0]].writer.write 'R'
      true
    end

    def shutdown
      # Raise shutdown in all client threads and join then
      @workers.each do |worker|
        worker.shutdown
      end

      @threads.each(&:join)
      return
    end

    private

    def process(i)
      loop do
        # Socket for the worker
        @pipes[i].reader.read 1
        length = @queues[i].length
        @logger.debug 'Receiving new sockets', length: length
        while length != 0 do
          @workers[i].add @queues[i].pop
          length -= 1
        end
      end
      return
    end
  end

  class TCPListener < IOReactor
    def initialize(address, port, pool=nil, &block)
      fail RuntimeError, 'connection_accepted not implemented', nil unless block_given? or respond_to?(:connection_accepted)
      super TCPServer.new(address, port), 'r'
      @io.listen 1024
      @pool = pool
      @block = block
    end

    def read_action
      10.times do
        start_time = Time.now
        socket = @io.accept_nonblock
        if @block
          client = @block.call(socket)
        else
          client = connection_accepted(socket)
        end
        unless client
          socket.close
          return
        end
        duration = ((Time.now - start_time) * 1000).to_i
        @logger.warn 'Slow connection accept', :duration_ms => duration, :client => client.id if duration > 100
        @logger.warn 'Connection accepted', client: client.id
        if @pool
          @pool.distribute client
        else
          @multiplexer.add client
        end
      end
    end
  end
end
