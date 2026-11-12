# frozen_string_literal: true
require 'mechanize/test_case'
require 'digest'
require 'socket'

##
# Reads the streaming body off a request before the fake Net::HTTP#request
# in Mechanize::TestCase handles it, the same way Net::HTTP would read it
# off the wire (IO.copy_stream).  Disabled while a test talks to a real
# socket.

module MechanizeStreamingBodyCapture # :nodoc:
  class << self
    attr_accessor :enabled
  end
  self.enabled = true

  def request(req, *data, &block)
    if MechanizeStreamingBodyCapture.enabled && req.body_stream
      captured = StringIO.new.set_encoding(Encoding::BINARY)
      IO.copy_stream req.body_stream, captured
      req.body = captured.string
    end

    super
  end
end

class Net::HTTP # :nodoc:
  alias_method :harness_fake_request, :request
  alias_method :harness_fake_do_start, :do_start
end

Net::HTTP.prepend MechanizeStreamingBodyCapture

class TestMechanizeStreamingMultipart < Mechanize::TestCase

  ##
  # A StringIO that records the size of every read, can fail after serving
  # a number of bytes, and knows whether it was closed.

  class ControlledIO < StringIO
    attr_reader :read_sizes

    def initialize(string, fail_after_bytes: nil)
      super(string)
      @read_sizes = []
      @fail_after_bytes = fail_after_bytes
      @bytes_served = 0
      @closed = false
    end

    def read(maxlen = nil, outbuf = nil)
      @read_sizes << maxlen if maxlen

      if @fail_after_bytes && @bytes_served >= @fail_after_bytes
        raise IOError, 'controlled read failure'
      end

      result = maxlen ? super(maxlen, outbuf) : super()
      @bytes_served += result.bytesize if result
      result
    end

    def close
      @closed = true
      super
    end

    def closed?
      @closed
    end
  end

  ## A read-able object that is not seekable at all
  class UnseekableIO
    def read(*)
      ''
    end
  end

  ## An IO that claims to seek but raises ESPIPE, like a pipe or socket
  class EspipeIO < StringIO
    def seek(*)
      raise Errno::ESPIPE
    end
  end

  CHUNK = Mechanize::StreamingMultipartBody::CHUNK_SIZE

  def setup
    super

    @mech.streaming_multipart = true

    @file_content = Random.new(1234).bytes(200_000)
    @file_digest = Digest::SHA256.hexdigest(@file_content)

    @dir = Dir.mktmpdir
    @file_path = File.join(@dir, 'sample.bin')
    File.binwrite(@file_path, @file_content)

    # small enough that the echoed response stays in memory (under
    # max_file_buffer), keeping response tempfiles out of fd counting
    @small_path = File.join(@dir, 'small.bin')
    File.binwrite(@small_path, 'small file content'.b)
  end

  def teardown
    FileUtils.remove_entry @dir
    super
  end

  def multipart_form(action = 'http://localhost/file_upload', &block)
    html = <<-HTML
      <html><body>
      <form method="POST" enctype="multipart/form-data" action="#{action}">
        <input type="text" name="a" value="1">
        <input type="text" name="a" value="2">
        <input type="text" name="b" value="hello">
        <input type="file" name="userfile1">
        <input type="file" name="userfile2">
        <input type="submit" value="Upload">
      </form>
      </body></html>
    HTML

    form = html_page(html).forms.first
    yield form if block_given?
    form
  end

  def open_fd_count
    Dir.children('/proc/self/fd').count
  end

  def have_proc_fd?
    File.directory? '/proc/self/fd'
  end

  def request_body(request = requests.last)
    request.body.b
  end

  def boundary_of(request)
    request['content-type'][/boundary=(\S+)/, 1]
  end

  ##
  # Runs the block with the real Net::HTTP request/connection methods
  # instead of the Mechanize::TestCase fakes.

  def with_real_http
    Net::HTTP.class_eval do
      alias_method :request, :old_request
      alias_method :do_start, :old_do_start
    end
    MechanizeStreamingBodyCapture.enabled = false

    yield
  ensure
    Net::HTTP.class_eval do
      alias_method :request, :harness_fake_request
      alias_method :do_start, :harness_fake_do_start
    end
    MechanizeStreamingBodyCapture.enabled = true
  end

  ##
  # A minimal HTTP server on a real socket that records raw requests.

  def with_raw_server
    server = TCPServer.new '127.0.0.1', 0
    port = server.addr[1]
    received = []

    thread = Thread.new do
      loop do
        sock = server.accept

        request_line = sock.gets
        headers = {}
        while (line = sock.gets) && line != "\r\n"
          name, value = line.chomp.split(': ', 2)
          headers[name.downcase] = value
        end

        length = headers['content-length'].to_i
        body = String.new
        while body.bytesize < length
          chunk = sock.read(length - body.bytesize)
          break unless chunk
          body << chunk
        end

        received << [request_line, headers, body]

        sock.write "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n" \
                   "Content-Length: 2\r\nConnection: close\r\n\r\nok"
        sock.close
      end
    end

    yield port, received
  ensure
    thread.kill
    server.close
  end

  def test_default_is_off
    mech = Mechanize.new
    refute mech.streaming_multipart

    form = multipart_form { |f| f.file_uploads.first.file_name = @file_path }
    mech.submit form

    assert_nil requests.last.body_stream
    assert_kind_of String, requests.last.body
  end

  def test_streaming_matches_request_data_byte_for_byte
    io = StringIO.new('io content here'.b)

    build = lambda do
      multipart_form do |f|
        f.encoding = 'Shift_JIS'
        f['b'] = '日本語'
        f.file_uploads[0].file_name = @file_path
        f.file_uploads[1].file_data = io
      end
    end

    form = build.call
    srand 42
    expected = form.request_data

    io.rewind
    form = build.call
    srand 42
    body = form.streaming_request_data

    assert_equal expected.b, body.read
    assert_equal expected.bytesize, body.length
    assert_equal form.enctype, "multipart/form-data; boundary=#{boundary_of_fake(form)}"
  end

  def boundary_of_fake(form)
    form.enctype[/boundary=(\S+)/, 1]
  end

  def test_submit_file_path_digest_length_order
    form = multipart_form { |f| f.file_uploads.first.file_name = @file_path }
    @mech.submit form

    body = request_body
    boundary = boundary_of requests.last

    # Content-Length is accurate
    assert_equal body.bytesize.to_s, requests.last['content-length']

    # boundary delimiters
    assert body.start_with?("--#{boundary}\r\n")
    assert body.end_with?("--#{boundary}--\r\n")

    # file content arrived intact
    assert_includes body, @file_content
    part = body[/#{Regexp.escape("Content-Disposition: form-data; name=\"userfile1\"")}.*?\r\n\r\n(.*?)\r\n--#{boundary}/m, 1]
    assert_equal @file_digest, Digest::SHA256.hexdigest(part)

    # field order and duplicate values are preserved, files come after fields
    i1 = body.index("name=\"a\"\r\n\r\n1\r\n")
    i2 = body.index("name=\"a\"\r\n\r\n2\r\n")
    i3 = body.index("name=\"b\"\r\n\r\nhello\r\n")
    i4 = body.index('name="userfile1"')
    assert i1 && i2 && i3 && i4, 'all parts present'
    assert i1 < i2 && i2 < i3 && i3 < i4, 'parts in order'
  end

  def test_file_read_upper_bound_and_offset
    offset = 100
    io = ControlledIO.new('x' * offset + @file_content)
    io.read offset # caller's position

    form = multipart_form { |f| f.file_uploads[0].file_data = io }
    @mech.submit form

    # every read from the IO was at most 64 KiB
    assert_operator io.read_sizes.max, :<=, CHUNK
    assert_operator io.read_sizes.length, :>=, 3 # 200_000 bytes in 64 KiB reads

    # only bytes after the caller's position were sent
    body = request_body
    assert_includes body, @file_content
    refute_includes body, 'x' * offset
    assert_equal body.bytesize.to_s, requests.last['content-length']
  end

  def test_caller_io_restored_and_not_closed
    io = ControlledIO.new('prefix'.b + @file_content)
    io.read 6

    form = multipart_form { |f| f.file_uploads[0].file_data = io }
    @mech.submit form

    assert_equal 6, io.pos
    refute io.closed?
  end

  def test_library_file_closed_after_submit
    skip 'needs /proc/self/fd' unless have_proc_fd?

    fds_before = open_fd_count

    form = multipart_form { |f| f.file_uploads.first.file_name = @small_path }
    @mech.submit form

    assert_equal fds_before, open_fd_count
  end

  def test_multi_file_upload
    other_content = Random.new(99).bytes(70_000)
    io = ControlledIO.new(other_content)

    form = multipart_form do |f|
      f.file_uploads[0].file_name = @file_path
      f.file_uploads[1].file_data = io
    end
    @mech.submit form

    body = request_body

    i1 = body.index('name="userfile1"')
    i2 = body.index('name="userfile2"')
    assert i1 < i2, 'file parts in DOM order'

    assert_includes body, @file_content
    assert_includes body, other_content
    assert_equal body.bytesize.to_s, requests.last['content-length']
    assert_operator io.read_sizes.max, :<=, CHUNK
  end

  def test_post_with_io_streams
    file = File.open(@file_path, 'rb')
    read_sizes = []
    file.define_singleton_method(:read) do |maxlen = nil, outbuf = nil|
      read_sizes << maxlen if maxlen
      maxlen ? super(maxlen, outbuf) : super()
    end

    page = @mech.post('http://localhost/file_upload',
                      'name' => 'Some file', 'userfile1' => file)

    assert_includes page.body.b, @file_content
    assert_operator read_sizes.max, :<=, CHUNK
    assert_equal 0, file.pos
    refute file.closed?
  ensure
    file.close unless file.closed?
  end

  def test_post_with_file_upload_object
    upload = Mechanize::Form::FileUpload.new({ 'name' => 'userfile1' }, @file_path)

    page = @mech.post('http://localhost/file_upload',
                      'name' => 'Some file', 'userfile1' => upload)

    assert_includes page.body.b, @file_content
    assert_nil upload.file_data # the file was never buffered onto the field
  end

  def test_auth_resend_byte_identical
    @mech.add_auth 'http://localhost/basic_auth', 'user', 'pass'

    form = multipart_form('http://localhost/basic_auth') do |f|
      f.file_uploads.first.file_name = @file_path
    end
    page = @mech.submit form

    assert_equal 'You are authenticated', page.body
    assert_equal 2, requests.length

    first, second = requests
    assert_equal boundary_of(first), boundary_of(second)
    assert_equal first.body.b, second.body.b
    assert_equal first['content-length'], second['content-length']
    assert_includes first.body.b, @file_content
  end

  def test_duplicate_submit
    io = ControlledIO.new(@file_content)

    form = multipart_form { |f| f.file_uploads[0].file_data = io }

    2.times do
      @mech.submit form
      assert_includes request_body, @file_content
      assert_equal 0, io.pos
      refute io.closed?
    end

    assert_equal 2, requests.length
  end

  def test_pre_connect_hook_exception_cleans_up
    skip 'needs /proc/self/fd' unless have_proc_fd?

    io = ControlledIO.new(@file_content)
    fds_before = open_fd_count

    @mech.pre_connect_hooks << ->(_agent, _req) { raise 'hook boom' }

    form = multipart_form do |f|
      f.file_uploads[0].file_name = @file_path
      f.file_uploads[1].file_data = io
    end

    assert_raises(RuntimeError) { @mech.submit form }

    assert_equal 0, io.pos
    refute io.closed?
    assert_equal fds_before, open_fd_count
  end

  def test_post_connect_hook_exception_cleans_up
    io = ControlledIO.new(@file_content)

    @mech.post_connect_hooks << ->(*) { raise 'hook boom' }

    form = multipart_form { |f| f.file_uploads[0].file_data = io }

    assert_raises(RuntimeError) { @mech.submit form }

    assert_equal 0, io.pos
    refute io.closed?
  end

  def test_read_exception_cleans_up
    io = ControlledIO.new(@file_content, fail_after_bytes: 1000)

    form = multipart_form { |f| f.file_uploads[0].file_data = io }

    assert_raises(IOError) { @mech.submit form }

    assert_equal 0, io.pos
    refute io.closed?
  end

  def test_network_exception_cleans_up
    skip 'needs /proc/self/fd' unless have_proc_fd?

    io = ControlledIO.new(@file_content)
    fds_before = open_fd_count

    # find a closed port
    probe = TCPServer.new('127.0.0.1', 0)
    port = probe.addr[1]
    probe.close

    form = multipart_form("http://127.0.0.1:#{port}/upload") do |f|
      f.file_uploads[0].file_name = @small_path
      f.file_uploads[1].file_data = io
    end

    with_real_http do
      assert_raises(Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EPIPE) do
        @mech.submit form
      end
    end

    assert_equal 0, io.pos
    refute io.closed?
    assert_equal fds_before, open_fd_count
  end

  def test_unsupported_io_raises_before_request
    form = multipart_form { |f| f.file_uploads[0].file_data = UnseekableIO.new }

    e = assert_raises(Mechanize::Error) { @mech.submit form }
    assert_match(/seekable/, e.message)
    assert_empty requests
  end

  def test_unseekable_io_raises_before_request
    form = multipart_form { |f| f.file_uploads[0].file_data = EspipeIO.new('x') }

    e = assert_raises(Mechanize::Error) { @mech.submit form }
    assert_match(/seekable/, e.message)
    assert_empty requests
  end

  def test_failed_build_closes_library_files
    skip 'needs /proc/self/fd' unless have_proc_fd?

    fds_before = open_fd_count

    assert_raises(Mechanize::Error) do
      Mechanize::StreamingMultipartBody.new [
        [:file, @file_path],
        [:io, UnseekableIO.new],
      ]
    end

    assert_equal fds_before, open_fd_count
  end

  def test_request_data_still_buffered
    form = multipart_form { |f| f.file_uploads.first.file_name = @file_path }

    data = form.request_data

    assert_kind_of String, data
    assert_includes data.b, @file_content
    assert_kind_of String, form.file_uploads.first.file_data
  end

  def test_raw_socket_content_length_and_bytes
    form_action = nil

    with_raw_server do |port, received|
      form_action = "http://127.0.0.1:#{port}/upload"

      with_real_http do
        form = multipart_form(form_action) do |f|
          f.file_uploads.first.file_name = @file_path
        end
        @mech.submit form
      end

      assert_equal 1, received.length
      _request_line, headers, body = received.first

      assert_equal body.bytesize.to_s, headers['content-length']
      boundary = headers['content-type'][/boundary=(\S+)/, 1]
      assert body.start_with?("--#{boundary}\r\n")
      assert body.end_with?("--#{boundary}--\r\n")
      assert_includes body, @file_content
      assert_equal @file_digest,
                   Digest::SHA256.hexdigest(@file_content)
    end
  end

  def test_body_read_readpartial_rewind_close
    body = Mechanize::StreamingMultipartBody.new [
      [:string, 'hello '],
      [:string, 'world'],
    ]

    assert_equal 11, body.length
    assert_equal 11, body.size

    assert_equal 'he', body.read(2)
    assert_equal 'llo ', body.readpartial(4)
    assert_equal 'world', body.read
    assert_nil body.read(1)
    assert_raises(EOFError) { body.readpartial(1) }

    body.rewind
    assert_equal 'hello world', body.read

    body.rewind
    out = String.new
    assert_same out, body.read(5, out)
    assert_equal 'hello', out

    body.close
    assert body.closed?
    assert_raises(IOError) { body.read }
    assert_raises(IOError) { body.rewind }

    body.close # idempotent
  end

  def test_body_rewind_byte_identical
    io = ControlledIO.new(@file_content)

    body = Mechanize::StreamingMultipartBody.new [
      [:string, "header\r\n"],
      [:io, io],
      [:string, "\r\n"],
    ]

    first_pass = body.read
    body.rewind
    second_pass = body.read

    assert_equal first_pass, second_pass
    assert_includes first_pass, @file_content

    body.close
    assert_equal 0, io.pos
    refute io.closed?
  end

end
