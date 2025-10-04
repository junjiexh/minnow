#include "byte_stream.hh"
#include "debug.hh"

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity + 1 ), buffer_( capacity + 1 ) {}

// Push data to stream, but only as much as available capacity allows.
void Writer::push( string data )
{
  uint64_t old_writer_index = writer_index_;
  for ( char c : data ) {
    if ( ( writer_index_ + 1 ) % capacity_ != reader_index_ ) {
      buffer_[writer_index_] = c;
      writer_index_ = ( writer_index_ + 1 ) % capacity_;
    } else {
      break; // No more capacity
    }
  }
  bytes_pushed_ += ( writer_index_ + capacity_ - old_writer_index ) % capacity_;
}

// Signal that the stream has reached its ending. Nothing more will be written.
void Writer::close()
{
  closed_ = true;
}

// Has the stream been closed?
bool Writer::is_closed() const
{
  return closed_;
}

// How many bytes can be pushed to the stream right now?
uint64_t Writer::available_capacity() const
{
  return ( capacity_ - writer_index_ + reader_index_ - 1 ) % capacity_;
}

// Total number of bytes cumulatively pushed to the stream
uint64_t Writer::bytes_pushed() const
{
  return bytes_pushed_;
}

// Peek at the next bytes in the buffer -- ideally as many as possible.
// It's not required to return a string_view of the *whole* buffer, but
// if the peeked string_view is only one byte at a time, it will probably force
// the caller to do a lot of extra work.
string_view Reader::peek() const
{
  uint64_t len = ( writer_index_ + capacity_ - reader_index_ ) % capacity_;
  if ( len == 0 ) {
    return {};
  }
  uint64_t start = reader_index_;
  uint64_t contig = min( len, capacity_ - start );
  return { &buffer_[start], contig };
}

// Remove `len` bytes from the buffer.
void Reader::pop( uint64_t len )
{
  if ( len > ( writer_index_ + capacity_ - reader_index_ ) % capacity_ ) {
    set_error();
    return;
  }
  reader_index_ = ( reader_index_ + len ) % capacity_;
  bytes_popped_ += len;
}

// Is the stream finished (closed and fully popped)?
bool Reader::is_finished() const
{
  return closed_ && ( reader_index_ == writer_index_ );
}

// Number of bytes currently buffered (pushed and not popped)
uint64_t Reader::bytes_buffered() const
{
  return ( writer_index_ + capacity_ - reader_index_ ) % capacity_;
}

// Total number of bytes cumulatively popped from stream
uint64_t Reader::bytes_popped() const
{
  return bytes_popped_;
}
