# frozen_string_literal: true

##
# A streaming multipart/form-data request body.
#
# This class is used instead of a String request body when
# Mechanize#streaming_multipart is enabled.  File content is read from disk
# or from a caller-supplied IO in chunks of at most CHUNK_SIZE bytes as the
# request is written to the socket, so uploading a large file does not
# buffer the whole file in memory or on disk.
#
# The body is built once per operation (Mechanize#post or Mechanize#submit)
# with a fixed boundary, and can be #rewind-ed and read again, producing
# byte-identical output.  This is what allows a 401 authentication resend to
# transmit exactly the same entity.
#
# The total #length is computed up front without reading any file content,
# so an accurate Content-Length is sent with the request.
#
# Files mechanize opens itself (from a file path) are closed by #close.
# Caller-supplied IO objects are read from the position they had when the
# body was built, are rewound to that position on #rewind and #close, and
# are never closed by mechanize.
#
# This class is an implementation detail and is subject to change at any
# time.

class Mechanize::StreamingMultipartBody

  ##
  # Maximum number of bytes read from an underlying file or IO in a single
  # read call.

  CHUNK_SIZE = 64 * 1024 # 64 KiB

  ##
  # Total length of the body in bytes.

  attr_reader :length

  alias size length

  ##
  # A segment of the body backed by a String.

  class StringSource # :nodoc:
    def initialize(string)
      @string = string
      @pos = 0
    end

    def length
      @string.bytesize
    end

    def read_chunk(maxlen)
      return nil if @pos >= @string.bytesize

      chunk = @string.byteslice(@pos, maxlen)
      @pos += chunk.bytesize
      chunk
    end

    def rewind
      @pos = 0
    end

    def close
    end
  end

  ##
  # A segment of the body backed by a file mechanize opens and owns.

  class FileSource # :nodoc:
    def initialize(path)
      @io = File.open(path, 'rb')
      @length = @io.size
      @pos = 0
    end

    attr_reader :length

    def read_chunk(maxlen)
      return nil if @pos >= @length

      read_from @io, [maxlen, @length - @pos].min
    end
    def rewind
      @io.seek 0
      @pos = 0
    end

    def close
      @io.close unless @io.closed?
    rescue IOError
      # already closed
    end

    private

    def read_from(io, maxlen)
      return String.new if maxlen.zero?

      data = io.read([maxlen, Mechanize::StreamingMultipartBody::CHUNK_SIZE].min)

      if data.nil? || data.bytesize.zero? then
        raise IOError, "file #{io.path.inspect} changed size while being read"
      end

      @pos += data.bytesize
      data
    end
  end

  ##
  # A segment of the body backed by a caller-supplied, seekable IO.  The IO
  # is read from the position it had when this source was created, and is
  # returned to that position by #rewind and #close.  It is never closed by
  # mechanize.

  class IOSource # :nodoc:
    def initialize(io)
      unless io.respond_to?(:read) && io.respond_to?(:pos) &&
             io.respond_to?(:seek) then
        raise Mechanize::Error,
              "streaming multipart upload requires a seekable IO, " \
              "got #{io.inspect}"
      end

      @io = io

      begin
        @start = io.pos
        io.seek 0, IO::SEEK_END
        @length = io.pos - @start
        io.seek @start, IO::SEEK_SET
      rescue SystemCallError, IOError => e
        raise Mechanize::Error,
              "streaming multipart upload requires a seekable IO with a " \
              "known length (#{e.class}: #{e.message})"
      end

      if @length.negative? then
        raise Mechanize::Error,
              "streaming multipart upload requires an IO whose position is " \
              "not past its end"
      end

      @pos = 0
    end

    attr_reader :length

    def read_chunk(maxlen)
      return nil if @pos >= @length
      return String.new if maxlen.zero?

      maxlen = [maxlen, @length - @pos,
                Mechanize::StreamingMultipartBody::CHUNK_SIZE].min

      data = @io.read(maxlen)

      if data.nil? || data.bytesize.zero? then
        raise IOError, "IO for multipart upload ended before its known length"
      end

      @pos += data.bytesize
      data
    end

    def rewind
      @io.seek @start, IO::SEEK_SET
      @pos = 0
    end

    def close
      # restore the caller's IO to the position it had when the body was
      # built; never close it
      @io.seek @start, IO::SEEK_SET
    rescue SystemCallError, IOError
      # the caller may have closed or broken the IO themselves; cleanup must
      # not raise
    end
  end

  ##
  # Creates a streaming body from +segments+, an array of:
  #
  # [:string, string]:: literal bytes
  # [:file, path]::     content of the file at +path+, opened and closed by
  #                     mechanize
  # [:io, io]::         content of +io+ from its current position, which
  #                     must be seekable and have a known length
  #
  # Raises Mechanize::Error before anything is sent if a segment is
  # unsupported.  If building fails, any file already opened is closed.

  def initialize(segments)
    @sources = []

    segments.each do |type, value|
      @sources <<
        case type
        when :string then StringSource.new(value)
        when :file   then FileSource.new(value)
        when :io     then IOSource.new(value)
        else
          raise Mechanize::Error,
                "unsupported streaming multipart segment: #{type.inspect}"
        end
    end

    @length = @sources.sum(&:length)
    @index = 0
    @closed = false
  rescue Exception
    close
    raise
  end

  ##
  # Reads and returns up to +maxlen+ bytes, or the rest of the body when
  # +maxlen+ is nil.  Returns nil at the end of the body.  Like IO#read, an
  # optional +outbuf+ may be given.

  def read(maxlen = nil, outbuf = nil)
    raise IOError, 'closed stream' if closed?

    if maxlen.nil? then
      buffer = String.new
      while (chunk = next_chunk(CHUNK_SIZE))
        buffer << chunk
      end
    elsif maxlen.zero? then
      buffer = String.new
    else
      raise ArgumentError, "negative length #{maxlen} given" if maxlen.negative?

      buffer = nil

      while buffer.nil? || buffer.bytesize < maxlen
        chunk = next_chunk(maxlen - (buffer ? buffer.bytesize : 0))
        break unless chunk
        buffer = buffer ? buffer << chunk : chunk.dup
      end

      return nil if buffer.nil?
    end

    if outbuf then
      outbuf.replace buffer
      outbuf
    else
      buffer
    end
  end

  ##
  # Reads and returns up to +maxlen+ bytes, raising EOFError at the end of
  # the body.  Like IO#readpartial, an optional +outbuf+ may be given.

  def readpartial(maxlen, outbuf = nil)
    raise IOError, 'closed stream' if closed?
    raise ArgumentError, "negative length #{maxlen} given" if maxlen.negative?

    chunk = maxlen.zero? ? String.new : next_chunk(maxlen)

    raise EOFError, 'end of file reached' unless chunk

    if outbuf then
      outbuf.replace chunk
      outbuf
    else
      chunk
    end
  end

  ##
  # Is the body exhausted?

  def eof?
    @index >= @sources.length
  end

  ##
  # Rewinds every segment so the body can be read again, byte-identically.
  # Caller-supplied IOs are returned to the position they had when the body
  # was built.

  def rewind
    raise IOError, 'closed stream' if closed?

    @sources.each(&:rewind)
    @index = 0

    0
  end

  ##
  # Closes files mechanize opened and restores caller-supplied IOs to the
  # positions they had when the body was built.  Caller-supplied IOs are
  # never closed.  Does nothing if the body is already closed.

  def close
    return if @closed

    @closed = true

    @sources.each do |source|
      begin
        source.close
      rescue StandardError
        # cleanup must not raise
      end
    end

    nil
  end

  ##
  # Is the body closed?

  def closed?
    @closed
  end

  def inspect # :nodoc:
    "#<#{self.class} length=#{length} sources=#{@sources.length}" \
      "#{closed? ? ' (closed)' : ''}>"
  end

  private

  def next_chunk(maxlen)
    while @index < @sources.length
      chunk = @sources[@index].read_chunk(maxlen)
      return chunk if chunk
      @index += 1
    end

    nil
  end

end
