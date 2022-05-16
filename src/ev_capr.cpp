#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <time.h>
#include <capr/ev_capr.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <raikv/timer_queue.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>
#include <raimd/json_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/rv_msg.h>

using namespace rai;
using namespace capr;
using namespace kv;
using namespace md;

/*
 * CAPR (CAche PRotocol) message frame:
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |1 1 0 0 1 0 1 0|    MSG CODE   |    MSG ENC    |I|S|P|R|N|x|0 1|
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |                        DATA LENGTH                            |
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |                        SUBJECT HASH                           |
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |                    SUBJECT   ...                              .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ INBOX ADDRESS ] -- optional 12 byte reply addr:    I set .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ SESSION ID ]    -- optional 8 byte session id:     S set .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ PUBTIME ]       -- optional 8 byte pub time:       P set .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ ROUTETIME ]     -- optional 8 byte route time:     R set .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ COUNTER ]       -- optional 4 byte update counter: N set .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     .    [ MESSAGE DATA ]  -- the rest up to DATA LENGTH + 12 bytes .
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

       SUBJECT must be present, the coding is what RaiSubject uses:
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    N SEGS     |   SEG1 LEN    |  SEG1 string, null terminated |
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     |    SEG2 LEN   |   SEG2 string, null terminated                |
     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

 * MSG CODE |   what        |  description
 * ---------+---------------+--------------------
 *    +     | SESSION_START | first message published
 *    !     | SESSION_INFO  | info message every 60 seconds
 *    -     | SESSION_STOP  | last message published if shutdown
 *      It doesn't matter which side sends SESSION_START first,
 *      proto is designed for multiplexing sessions over multicast
 *      { "sub-listen" : "wildcard", "bcast-feed" : "wildcard",
 *        "inter-feed" : "wildcard", "casca-feed" : "wildcard",
 *        "user" : "name", "host" : "hostname", "app" : "application",
 *        "date" : "stamp", "time" : seconds.ns, "uptime" : seconds.ms,
 *        "subcnt" : int, "msg-sent" : msg-cnt, "msg-recv" : msg-recv,
 *        ... more stats ... }
 *
 *    #    | SESSION_PUB    | notify publish start, subs on different nets
 *    ?    | PING           | test transport link for subject
 *    1    | ACK            | respond to ping
 *
 *    C    | CANCEL         | stop subscription
 *    L    | LISTEN         | start subscription
 *    S    | SUBSCRIBE      | start subscription w/initial
 *    X    | SNAP_REQUEST   | snapshot request
 *    Z    | DICT_REQUEST   | dict request
 *
 *    d    | DROP           | drop subjsct, publisher stop
 *    i    | INITIAL        | initial message
 *    p    | PUBLISH        | publish message (without ini/upd attr)
 *    r    | RECAP          | repeat previous state, could have seen before
 *    s    | STATUS         | status message
 *    u    | UPDATE         | update message
 *    v    | VERIFY         | verify message
 *    x    | SNAP_REPLY     | response to a snap request
 *    z    | DICT_REPLY     | response to a dict request
 */

static uint64_t uptime_stamp;

EvCaprListen::EvCaprListen( EvPoll &p ) noexcept
  : EvTcpListen( p, "capr_listen", "capr_sock" )
{
  if ( uptime_stamp == 0 )
    uptime_stamp = kv_current_realtime_ns();
  md_init_auto_unpack();
}

bool
EvCaprListen::accept( void ) noexcept
{
  EvCaprService *c = 
    this->poll.get_free_list<EvCaprService>( this->accept_sock_type );
  if ( c == NULL )
    return false;
  if ( ! this->accept2( *c, "capr" ) )
    return false;

  if ( this->sess == NULL ) {
    uint64_t h1;
    const char * user = ::getenv( "USER" );
    char host[ 256 ];
    h1 = this->poll.create_ns();
    ::gethostname( host, sizeof( host ) );
    this->sess = CaprSession::create( "localhost", user, host, "ds", h1 );
  }
  c->initialize_state( ++this->timer_id );
  c->sess = this->sess->copy();
  c->pub_session( CAPR_SESSION_START );
  c->idle_push( EV_WRITE_HI );
  this->poll.timer.add_timer_seconds( c->fd, CAPR_SESSION_IVAL,
                                      c->timer_id, 0 );
  return true;
}

static void
print_rec( CaprMsgIn &rec )
{
  char subj[ CAPR_MAX_SUBJ_LEN ];
  printf( "rec %c\n", rec.code );
  rec.get_subject( subj );
  printf( "subj %s\n", subj );
}

void
EvCaprService::process( void ) noexcept
{
  CaprMsgIn rec;
  size_t    buflen;
  int32_t   status = 0;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;
    status = rec.decode( (uint8_t *) &this->recv[ this->off ], buflen );
    if ( status != 0 )
      goto break_loop;

    if ( this->inboxlen == 0 ) {
      if ( ( rec.flags & CAPR_SID_PRESENT ) != 0 ) {
        this->sid = rec.sid;
        this->inboxlen = rec.get_inbox( this->inbox );
        this->add_subscription( this->inbox, this->inboxlen, NULL, 0, true );
      }
    }
    if ( is_capr_pub( rec.code ) ) {
      this->fwd_pub( rec );
    }
    else if ( is_capr_sess( rec.code ) ) {
      if ( rec.code == CAPR_SESSION_SUBS )
        this->reassert_subs( rec );
      /*print_rec( rec );*/
    }
    else if ( is_capr_sub( rec.code ) ) {
      /*print_rec( rec );*/
      if ( rec.code == CAPR_LISTEN || rec.code == CAPR_SUBSCRIBE )
        this->add_sub( rec );
      else if ( rec.code == CAPR_CANCEL )
        this->rem_sub( rec );
    }
    else if ( is_capr_meta( rec.code ) ) {
      print_rec( rec );
    }
    else {
      print_rec( rec );
    }
    this->off += rec.data_len + (uint32_t) CAPR_HDR_SIZE;
    this->br  += rec.data_len + (uint32_t) CAPR_HDR_SIZE;
    this->mr++;
  }
break_loop:;
  this->pop( EV_PROCESS );
  this->push_write();
  if ( status < 0 ) {
    fprintf( stderr, "capr status %d, closing\n", status );
    this->push( EV_CLOSE );
  }
}

bool
EvCaprService::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  if ( this->timer_id != tid )
    return false;
  this->pub_session( CAPR_SESSION_INFO );
  this->idle_push_write();
  return true;
}

uint8_t
EvCaprService::is_subscribed( const NotifySub &sub ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  if ( this->sub_tab.find( sub.subj_hash, sub.subject, sub.subject_len,
                           coll ) == CAPR_SUB_OK )
    v |= EV_SUBSCRIBED;
  else
    v |= EV_NOT_SUBSCRIBED;
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

uint8_t
EvCaprService::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  const PatternCvt & cvt = pat.cvt;
  CaprPatternRoute * rt;
  if ( this->pat_tab.find( pat.prefix_hash, pat.pattern, cvt.prefixlen,
                           rt, coll ) == CAPR_SUB_OK ) {
    CaprWildMatch *m;
    for ( m = rt->list.hd; m != NULL; m = m->next ) {
      if ( m->len == pat.pattern_len &&
           ::memcmp( pat.pattern, m->value, m->len ) == 0 ) {
        v |= EV_SUBSCRIBED;
        break;
      }
    }
    if ( m == NULL )
      v |= EV_NOT_SUBSCRIBED | EV_COLLISION;
    else if ( rt->count > 1 )
      v |= EV_COLLISION;
  }
  else {
    v |= EV_NOT_SUBSCRIBED;
  }
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

void
EvCaprService::reassert_subs( CaprMsgIn &rec ) noexcept
{
  MDMsgMem  mem;
  MDMsg   * m = MDMsg::unpack( rec.msg_data, 0, rec.msg_data_len, 0, NULL,
                               &mem );
  MDFieldIter * iter;

  if ( m != NULL && m->get_field_iter( iter ) == 0 ) {
    if ( iter->first() == 0 ) {
      do {
        MDName      name;
        MDReference mref;
        if ( iter->get_name( name ) == 0 ) {
          if ( name.fnamelen == 2 &&
               ( name.fname[ 0 ] == (char) CAPR_SUBSCRIBE ||
                 name.fname[ 0 ] == (char) CAPR_LISTEN ) ) {
            if ( iter->get_reference( mref ) == 0 ) {
              if ( mref.ftype == MD_STRING &&
                   mref.fsize > 0 && mref.fsize < CAPR_MAX_SUBJ_LEN ) {
                const char * sub     = (char *) mref.fptr;
                size_t       len     = mref.fsize;
                bool         is_wild = false;

                while ( len > 0 && sub[ len - 1 ] == '\0' )
                  len--;
                if ( len > 0 ) {
                  if ( sub[ len - 1 ] == '*' || sub[ len - 1 ] == '>' ) {
                    if ( len == 1 )
                      is_wild = true;
                    else if ( sub[ len - 2 ] == '.' )
                      is_wild = true;
                  }
                  if ( ! is_wild ) { /* look for *. .*. .* */
                    const char * p = (const char *) ::memchr( sub, '*', len );
                    while ( p != NULL ) {
                      if ( ( p == sub || p[ -1 ] == '.' ) &&
                           ( p == &sub[ len - 1 ] || p[ 1 ] == '.' ) ) {
                        is_wild = true;
                        break;
                      }
                      p = (const char *)
                          ::memchr( p+1, '*', &sub[ len ] - (p+1) );
                    }
                  }
                  this->add_subscription( sub, len, NULL, 0, is_wild );
                }
              }
            }
          }
        }
      } while ( iter->next() == 0 );
    }
  }
}

static inline char
to_hex_char( uint8_t b ) {
  return ( b < 0xa ) ? ( b + '0' ) : ( b - 0xa + 'A' );
}

static inline uint32_t
from_hex_char( char b ) {
  return ( b <= '9' ) ? ( b - '0' ) : ( b - 'A' + 0xa );
}

void
EvCaprService::add_sub( CaprMsgIn &rec ) noexcept
{
  char     sub[ CAPR_MAX_SUBJ_LEN ],
           reply[ CAPR_MAX_SUBJ_LEN * 2 ];
  bool     is_wild;
  uint32_t len  = rec.get_subscription( sub, is_wild ),
           rlen = 0;

  if ( ! is_wild && ( rec.flags & CAPR_IBX_PRESENT ) != 0 ) {
    if ( this->inboxlen > 0 ) {
      rlen = this->inboxlen - 1;
      ::memcpy( reply, this->inbox, rlen );
      for ( uint32_t k = 8; k < 12; k++ ) {
        reply[ rlen ]   = to_hex_char( ( rec.addr[ k ] >> 4 ) & 0xfU );
        reply[ rlen+1 ] = to_hex_char( rec.addr[ k ] & 0xfU );
        rlen += 2;
      }
      reply[ rlen++ ] = '.';
      ::strcpy( &reply[ rlen ], sub );
      rlen += len;
    }
  }
  this->add_subscription( sub, len, reply, rlen, is_wild );
}

void
EvCaprService::add_subscription( const char *sub,  size_t len,
                                 const char *reply,  size_t replylen,
                                 bool is_wild ) noexcept
{
    uint32_t h;
    bool     coll = false;

  if ( ! is_wild ) {
    h    = kv_crc_c( sub, len, 0 );
    if ( this->sub_tab.put( h, sub, len, coll ) == CAPR_SUB_OK ) {
      NotifySub nsub( sub, len, reply, replylen, h, this->fd, coll, 'C' );
      this->sub_route.add_sub( nsub );
    }
  }
  else {
    CaprPatternRoute * rt;
    PatternCvt cvt;
    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.put( h, sub, cvt.prefixlen, rt,
                              coll ) != CAPR_SUB_NOT_FOUND ){
        CaprWildMatch * m;
        for ( m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == len && ::memcmp( sub, m->value, len ) == 0 )
            break;
        }
        if ( m == NULL ) {
          pcre2_real_code_8       * re = NULL;
          pcre2_real_match_data_8 * md = NULL;
          size_t erroff;
          int    error;
          bool   pattern_success = false;
          /* if prefix matches, no need for pcre2 */
          if ( cvt.prefixlen + 1 == len && sub[ cvt.prefixlen ] == '>' )
            pattern_success = true;
          else {
            re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error,
                                &erroff, 0 );
            if ( re == NULL ) {
              fprintf( stderr, "re failed\n" );
            }
            else {
              md = pcre2_match_data_create_from_pattern( re, NULL );
              if ( md == NULL )
                fprintf( stderr, "md failed\n" );
              else
                pattern_success = true;
            }
          }
          if ( pattern_success &&
               (m = CaprWildMatch::create( len, sub, re, md )) != NULL ) {
            rt->list.push_hd( m );
            if ( rt->count++ > 0 )
              coll = true;
            this->pat_tab.sub_count++;
            NotifyPattern npat( cvt, sub, len, reply, replylen, h, this->fd,
                                coll, 'C' );
            this->sub_route.add_pat( npat );
          }
          else {
            fprintf( stderr, "wildcard failed\n" );
            if ( rt->count == 0 )
              this->pat_tab.tab.remove( h, sub, len );
            if ( md != NULL )
              pcre2_match_data_free( md );
            if ( re != NULL )
              pcre2_code_free( re );
          }
        }
      }
    }
  }
}

void
EvCaprService::rem_sub( CaprMsgIn &rec ) noexcept
{
  char     sub[ CAPR_MAX_SUBJ_LEN ];
  bool     is_wild;
  uint32_t len = rec.get_subscription( sub, is_wild );
  uint32_t h;
  bool     coll = false;

  if ( ! is_wild ) {
    h = kv_crc_c( sub, len, 0 );
    if ( this->sub_tab.rem( h, sub, len, coll ) == CAPR_SUB_OK ) {
      NotifySub nsub( sub, len, h, this->fd, coll, 'C' );
      this->sub_route.del_sub( nsub );
    }
  }
  else {
    PatternCvt         cvt;
    RouteLoc           loc;
    CaprPatternRoute * rt;
    uint32_t           h;

    if ( cvt.convert_rv( sub, len ) == 0 ) {
      h = kv_crc_c( sub, cvt.prefixlen,
                    this->sub_route.prefix_seed( cvt.prefixlen ) );
      if ( this->pat_tab.find( h, sub, cvt.prefixlen, loc, rt,
                               coll ) == CAPR_SUB_OK ) {
        for ( CaprWildMatch *m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->len == len && ::memcmp( m->value, sub, len ) == 0 ) {
            if ( m->md != NULL ) {
              pcre2_match_data_free( m->md );
              m->md = NULL;
            }
            if ( m->re != NULL ) {
              pcre2_code_free( m->re );
              m->re = NULL;
            }
            rt->list.pop( m );
            if ( --rt->count > 0 )
              coll = true;
            delete m;
            this->pat_tab.sub_count--;
            if ( rt->count == 0 )
              this->pat_tab.tab.remove( loc );

            NotifyPattern npat( cvt, sub, len, h, this->fd, coll, 'C' );
            this->sub_route.del_pat( npat );
            break;
          }
        }
      }
    }
  }
}

void
EvCaprService::rem_all_sub( void ) noexcept
{
  CaprSubRoutePos     pos;
  CaprPatternRoutePos ppos;

  if ( this->sub_tab.first( pos ) ) {
    do {
      bool coll = this->sub_tab.rem_collision( pos.rt );
      NotifySub nsub( pos.rt->value, pos.rt->len, pos.rt->hash,
                      this->fd, coll, 'C' );
      this->sub_route.del_sub( nsub );
    } while ( this->sub_tab.next( pos ) );
  }
  if ( this->pat_tab.first( ppos ) ) {
    do {
      for ( CaprWildMatch *m = ppos.rt->list.hd; m != NULL; m = m->next ) {
        PatternCvt cvt;
        if ( cvt.convert_rv( m->value, m->len ) == 0 ) {
          bool coll = this->pat_tab.rem_collision( ppos.rt, m );
          NotifyPattern npat( cvt, m->value, m->len, ppos.rt->hash,
                              this->fd, coll, 'C' );
          this->sub_route.del_pat( npat );
        }
      }
    } while ( this->pat_tab.next( ppos ) );
  }
}

bool
EvCaprService::fwd_pub( CaprMsgIn &rec ) noexcept
{
  char     sub[ CAPR_MAX_SUBJ_LEN ];
  uint32_t len = rec.get_subject( sub ),
           h   = kv_crc_c( sub, len, 0 );
  EvPublish pub( sub, len, NULL, 0, rec.msg_data, rec.msg_data_len,
                 this->sub_route, this->fd, h, rec.msg_enc, rec.code );
  return this->sub_route.forward_msg( pub );
}

bool
EvCaprService::on_msg( EvPublish &pub ) noexcept
{
  uint32_t pub_cnt = 0;
  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    CaprSubStatus ret;
    if ( pub.subj_hash == pub.hash[ cnt ] ) {
      ret = this->sub_tab.updcnt( pub.subj_hash, pub.subject, pub.subject_len );
      if ( ret == CAPR_SUB_OK ) {
        if ( pub_cnt == 0 )
          this->fwd_msg( pub, NULL, 0 );
        pub_cnt++;
      }
    }
    else {
      CaprPatternRoute * rt = NULL;
      ret = this->pat_tab.find( pub.hash[ cnt ], pub.subject, pub.prefix[ cnt ],
                                rt );
      if ( ret == CAPR_SUB_OK ) {
        for ( CaprWildMatch *m = rt->list.hd; m != NULL; m = m->next ) {
          if ( m->re == NULL ||
               pcre2_match( m->re, (const uint8_t *) pub.subject,
                            pub.subject_len, 0, 0, m->md, 0 ) == 1 ) {
            if ( pub.subject[ 0 ] == '_' &&
                 pub.subject_len > this->inboxlen &&
                 ::memcmp( pub.subject, this->inbox, this->inboxlen-1 ) == 0 ) {
              m->msg_cnt++;
              this->fwd_inbox( pub );
            }
            else {
              m->msg_cnt++;
              if ( pub_cnt == 0 )
                this->fwd_msg( pub, NULL, 0 );
            }
            pub_cnt++;
          }
        }
      }
    }
  }
  return true;
}

bool
EvCaprService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  RouteLoc       loc;
  CaprSubRoute * rt = this->sub_tab.tab.find_by_hash( h, loc );
  if ( rt == NULL )
    return false;
  keylen = rt->len;
  ::memcpy( key, rt->value, keylen );
  return true;
}

void
EvCaprService::send( CaprMsgOut &rec,  size_t off,   const void *data,
                     size_t data_len ) noexcept
{
  this->append2( &rec, off, data, data_len );
  this->bs += off + data_len;
  this->ms++;
}

bool
EvCaprService::fwd_msg( EvPublish &pub,  const void *,  size_t ) noexcept
{
  CaprMsgOut rec;
  size_t off = rec.encode_publish( *this->sess, 0, pub.subject, pub.pub_type,
                                   pub.msg_len, pub.msg_enc );
  this->send( rec, off, pub.msg, pub.msg_len );
  return this->idle_push_write();
}

void
EvCaprService::get_inbox_addr( EvPublish &pub,  const char *&subj,
                               uint8_t *addr ) noexcept
{
  const char * i = &pub.subject[ this->inboxlen - 1 ];
  ::memcpy( addr, &this->sid, 8 );
  addr[ 8 ]  = ( from_hex_char( i[ 0 ] ) << 4 ) | from_hex_char( i[ 1 ] );
  addr[ 9 ]  = ( from_hex_char( i[ 2 ] ) << 4 ) | from_hex_char( i[ 3 ] );
  addr[ 10 ] = ( from_hex_char( i[ 4 ] ) << 4 ) | from_hex_char( i[ 5 ] );
  addr[ 11 ] = ( from_hex_char( i[ 6 ] ) << 4 ) | from_hex_char( i[ 7 ] );
  subj = &i[ 9 ];
}

bool
EvCaprService::fwd_inbox( EvPublish &pub ) noexcept
{
  CaprMsgOut rec;
  uint8_t addr[ 12 ];
  const char *subj;
  this->get_inbox_addr( pub, subj, addr );
  size_t off = rec.encode_publish( *this->sess, addr, subj, pub.pub_type,
                                   pub.msg_len, pub.msg_enc );
  this->send( rec, off, pub.msg, pub.msg_len );
  return this->idle_push_write();
}

void
CaprPatternMap::release( void ) noexcept
{
  CaprPatternRoutePos ppos;

  if ( this->first( ppos ) ) {
    do {
      CaprWildMatch *next;
      for ( CaprWildMatch *m = ppos.rt->list.hd; m != NULL; m = next ) {
        next = m->next;
        if ( m->md != NULL ) {
          pcre2_match_data_free( m->md );
          m->md = NULL;
        }
        if ( m->re != NULL ) {
          pcre2_code_free( m->re );
          m->re = NULL;
        }
        delete m;
      }
    } while ( this->next( ppos ) );
  }
  this->tab.release();
}

void
EvCaprService::release( void ) noexcept
{
  if ( this->sess != NULL )
    delete this->sess;
  this->rem_all_sub();
  this->sub_tab.release();
  this->pat_tab.release();
  this->EvConnection::release_buffers();
}

#ifdef _MSC_VER
static inline void ca_localtime( time_t t, struct tm &tmbuf ) {
  ::localtime_s( &tmbuf, &t );
}
#else
static inline void ca_localtime( time_t t, struct tm &tmbuf ) {
  ::localtime_r( &t, &tmbuf );
}
#endif

void
EvCaprService::pub_session( uint8_t code ) noexcept
{
  static const char listen_str[]        = "sub-listen",
                    bcast_str[]         = "bcast-feed",
                    state_str[]         = "state",
                    gt_str[]            = ">",
                    primary_str[]       = "primary",
                    date_str[]          = "date",
                    time_str[]          = "time",
                    uptime_str[]        = "uptime",
                    msg_sent_str[]      = "msg-sent",
                    byt_sent_str[]      = "byt-sent",
                    msg_recv_str[]      = "msg-recv",
                    byt_recv_str[]      = "byt-recv",
                    pkt_sent_str[]      = "pkt-sent",
                    pkt_recv_str[]      = "pkt-recv",
                    pkt_rxmt_str[]      = "pkt-rxmt",
                    pkt_miss_str[]      = "pkt-miss",
                    inb_loss_str[]      = "inb-loss",
                    out_loss_str[]      = "out-loss",
                    transport_str[]     = "transport",
                    session_info_sub[]  = "_CAPR.SESS.INFO",
                    session_start_sub[] = "_CAPR.SESS.START",
                    session_stop_sub[]  = "_CAPR.SESS.STOP",
                    pub_notify_sub[]    = "_CAPR.PUB.NOTIFY";
  char         buf[ 8192 ],
               date_buf[ 80 ];
  TibMsgWriter tmw( buf, sizeof( buf ) );

  tmw.append_string( listen_str, sizeof( listen_str ),
                     gt_str, sizeof( gt_str ) );
  tmw.append_string( bcast_str, sizeof( bcast_str ),
                     gt_str, sizeof( gt_str ) );
  tmw.append_string( state_str, sizeof( state_str ),
                     primary_str, sizeof( primary_str ) );

  uint64_t ns = kv_current_realtime_ns();
  time_t   t  = (time_t) ( ns / (uint64_t) 1e9 );
  struct tm tim;
  ca_localtime( t, tim );
  ::strftime( date_buf, sizeof( date_buf ), "%Y-%m-%d %H:%M:%S", &tim );

  tmw.append_string( date_str, sizeof( date_str ),
                     date_buf, ::strlen( date_buf ) + 1 );
  tmw.append_real( time_str, sizeof( time_str ), (double) ns / 1e9 );
  tmw.append_real( uptime_str, sizeof( uptime_str ),
                   (double) ( ns - uptime_stamp ) / 1e9 );
  tmw.append_int( msg_sent_str, sizeof( msg_sent_str ), this->ms );
  tmw.append_int( byt_sent_str, sizeof( byt_sent_str ), this->bs );
  tmw.append_int( msg_recv_str, sizeof( msg_recv_str ), this->mr );
  tmw.append_int( byt_recv_str, sizeof( byt_recv_str ), this->br );
  tmw.append_int( pkt_sent_str, sizeof( pkt_sent_str ), this->ms );
  tmw.append_int( pkt_recv_str, sizeof( pkt_recv_str ), this->mr );
  tmw.append_int( pkt_rxmt_str, sizeof( pkt_rxmt_str ), (uint8_t) 0 );
  tmw.append_int( pkt_miss_str, sizeof( pkt_miss_str ), (uint8_t) 0 );
  tmw.append_int( inb_loss_str, sizeof( inb_loss_str ), (uint8_t) 0 );
  tmw.append_int( out_loss_str, sizeof( out_loss_str ), (uint8_t) 0 );
  tmw.append_string( transport_str, sizeof( transport_str ), this->sess->addr,
                     this->sess->addr_len );

  CaprMsgOut rec;
  size_t off, len = tmw.update_hdr();
  const char *subj = ( code == CAPR_SESSION_INFO  ? session_info_sub :
                       code == CAPR_SESSION_START ? session_start_sub :
                       code == CAPR_SESSION_STOP  ? session_stop_sub :
                                                    pub_notify_sub );
  off = rec.encode_publish( *this->sess, 0, subj, code, len,
                            (uint8_t) RAIMSG_TYPE_ID );
  this->send( rec, off, buf, len );
  if ( code == CAPR_SESSION_INFO )
    this->sess->info_sent++;
}

static const char addr_str[] = "addr",
                  user_str[] = "user",
                  host_str[] = "host",
                  app_str[]  = "app";

CaprSession *
CaprSession::create( const char *addr,  const char *user,  const char *host,
                     const char *app,  uint64_t sid ) noexcept
{
  char          buf[ 8192 ];
  CaprSession * s = new ( buf ) CaprSession();
  TibMsgWriter  tmw( s->id, sizeof( buf ) - sizeof( *s ) );
  size_t        len;

  len = ::strlen( addr ) + 1;
  tmw.append_string( addr_str, sizeof( addr_str ), addr, len );
  len = ::strlen( user ) + 1;
  tmw.append_string( user_str, sizeof( host_str ), user, len );
  len = ::strlen( host ) + 1;
  tmw.append_string( host_str, sizeof( host_str ), host, len );
  len = ::strlen( app ) + 1;
  tmw.append_string( app_str, sizeof( app_str ), app, len );
  len = tmw.update_hdr();

  s->sid    = sid;
  s->stime  = kv_current_realtime_ns();
  s->id_len = (uint32_t) len;

  return s->copy();
}

CaprSession *
CaprSession::copy( void ) const noexcept
{
  MDMsgMem      mem;
  MDReference   mref;
  MDFieldIter * fld;
  size_t        len = sizeof( CaprSession ) + this->id_len;
  CaprSession * s = (CaprSession *) ::malloc( len );
  if ( s == NULL )
    return NULL;

  ::memcpy( s, this, len );
  TibMsg * msg = TibMsg::unpack( s->id, 0, s->id_len, 0, NULL, &mem );

  msg->get_field_iter( fld );
  fld->find( addr_str, sizeof( addr_str ), mref );
  msg->get_string( mref, s->addr, len ); s->addr_len = (uint8_t) len;
  fld->find( user_str, sizeof( user_str ), mref );
  msg->get_string( mref, s->user, len ); s->user_len = (uint8_t) len;
  fld->find( host_str, sizeof( host_str ), mref );
  msg->get_string( mref, s->host, len ); s->host_len = (uint8_t) len;
  fld->find( app_str, sizeof( app_str ), mref );
  msg->get_string( mref, s->app, len );  s->app_len = (uint8_t) len;

  return s;
}

static inline uint32_t
copy_subj_out( const char *subj,  uint8_t *buf,  uint32_t &hash )
{
  uint32_t i = 2, j = 1, segs = 1;
  for ( ; *subj != '\0'; subj++ ) {
    if ( *subj == '.' || i - j == 0xff ) {
      buf[ i++ ] = 0;
      buf[ j ]   = (uint8_t) ( i - j );
      j = i++;
      if ( ++segs == 0xff )
        break;
    }
    else {
      buf[ i++ ] = *subj;
    }
    if ( i > 1029 )
      break;
  }
  buf[ i++ ] = 0;
  buf[ j ]   = (uint8_t) ( i - j );
  buf[ 0 ]   = (uint8_t) segs;
  hash       = kv_crc_c( buf, i, 0 );
  return i;
}

static inline uint32_t
copy_subj_in( const uint8_t *buf,  char *subj,  bool &is_wild )
{
  uint8_t segs = buf[ 0 ];
  uint32_t i, j = 1, k = 0;

  is_wild = false;
  if ( segs > 0 ) {
    for (;;) {
      i  = j + 1;
      j += buf[ j ];
      if ( k + j - i >= CAPR_MAX_SUBJ_LEN - 2 )
        break;
      if ( i + 2 == j ) {
        if ( buf[ i ] == '*' ||
             ( buf[ i ] == '>' && segs == 1 ) )
          is_wild = true;
      }
      while ( i + 1 < j )
        subj[ k++ ] = (char) buf[ i++ ];
      if ( --segs > 0 )
        subj[ k++ ] = '.';
      else
        break;
    } 
  } 
  subj[ k ] = '\0';
  return k;
}

uint32_t
CaprMsgIn::get_subscription( char *s,  bool &is_wild ) noexcept
{
  return copy_subj_in( this->subj, s, is_wild );
}

static inline uint32_t
copy_subj_in2( const uint8_t *buf,  char *subj )
{
  uint8_t segs = buf[ 0 ];
  uint32_t i, j = 1, k = 0;

  if ( segs > 0 ) {
    for (;;) {
      i  = j + 1;
      j += buf[ j ];
      if ( k + j - i >= CAPR_MAX_SUBJ_LEN - 2 )
        break;
      while ( i + 1 < j )
        subj[ k++ ] = (char) buf[ i++ ];
      if ( --segs > 0 )
        subj[ k++ ] = '.';
      else
        break;
    } 
  } 
  subj[ k ] = '\0';
  return k;
}

uint32_t
CaprMsgIn::get_subject( char *s ) noexcept
{
  return copy_subj_in2( this->subj, s );
}

uint32_t
CaprMsgIn::get_inbox( char *buf ) noexcept
{
  uint8_t * byt = (uint8_t *) (void *) &this->sid;
  uint32_t j = 7, k;

  ::memcpy( buf, "_INBOX.", 7 );
  for ( k = 0; k < 4; k++ ) {
    if ( byt[ k ] >= 100 )
      buf[ j++ ] = ( byt[ k ] / 100 ) + '0';
    if ( byt[ k ] >= 10 )
      buf[ j++ ] = ( byt[ k ] / 10 ) % 10 + '0';
    buf[ j++ ] = byt[ k ] % 10 + '0';
    buf[ j++ ] = '-';
  }
  if ( buf[ j - 1 ] == '-' )
    buf[ j - 1 ] = '.';
  for ( ; k < 8; k++ ) {
    buf[ j ]     = to_hex_char( ( byt[ k ] >> 4 ) & 0xfU );
    buf[ j + 1 ] = to_hex_char( byt[ k ] & 0xfU );
    j += 2;
  }
  buf[ j ] = '.';
  buf[ j + 1 ] = '>';
  buf[ j + 2 ] = '\0';
  return j + 2;
}

uint32_t
CaprMsgOut::encode_publish( CaprSession &sess,  const uint8_t *addr,
                            const char *subj,  uint8_t code,
                            size_t msg_len,  uint32_t msg_enc ) noexcept
{
  uint32_t off;

  this->capr_byte = CAPR_MAGIC;
  this->code      = code;
  this->msg_enc   = msg_enc;
  this->flags     = CAPR_VERSION;

  off = copy_subj_out( subj, this->buf, this->subj_hash );
  if ( addr != NULL ) {
    ::memcpy( &this->buf[ off ], addr, CAPR_IBX_SIZE );
    off += CAPR_IBX_SIZE;
    this->flags |= CAPR_IBX_PRESENT;
  }
  ::memcpy( &this->buf[ off ], &sess.sid, CAPR_SID_SIZE );
  off += CAPR_SID_SIZE;

  this->flags    |= CAPR_SID_PRESENT;
  this->data_len  = (uint32_t) ( msg_len + off );
  this->subj_hash = get_u32<MD_BIG>( &this->subj_hash ); /* flip */
  this->data_len  = get_u32<MD_BIG>( &this->data_len );

  return off + (uint32_t) CAPR_HDR_SIZE;
}

int32_t
CaprMsgIn::decode( uint8_t *capr_pkt,  size_t pkt_size ) noexcept
{
  size_t off;
  uint8_t n;

  if ( pkt_size < CAPR_HDR_SIZE )
    return CAPR_HDR_SIZE;
  ::memcpy( &this->capr_byte, capr_pkt, CAPR_HDR_SIZE );
  this->data_len  = get_u32<MD_BIG>( &this->data_len );
  this->subj_hash = get_u32<MD_BIG>( &this->subj_hash );

  if ( this->capr_byte != CAPR_MAGIC )
    return ERR_BAD_PROTO_MAGIC;
  if ( ( this->flags & CAPR_VERSION_MASK ) != CAPR_VERSION )
    return ERR_BAD_PROTO_VERSION;

  if ( this->data_len + CAPR_HDR_SIZE > pkt_size )
    return (int32_t) ( this->data_len + (uint32_t) CAPR_HDR_SIZE );

  capr_pkt  = &capr_pkt[ CAPR_HDR_SIZE ];
  pkt_size -= CAPR_HDR_SIZE;

  if ( pkt_size == 0 || capr_pkt[ 0 ] == 0 )
    return ERR_MISSING_SUBJECT;
  off = 1;
  for ( n = 0; n < capr_pkt[ 0 ]; n++ ) {
    if ( capr_pkt[ off ] < 3 )
      return ERR_BAD_SUBJECT_SEG;
    off += (unsigned int) capr_pkt[ off ];
    if ( off > pkt_size  )
      return ERR_TRUNCATED_SUBJECT;
  }
  this->subj        = capr_pkt;
  this->subj_len    = (uint32_t) off;
  this->addr        = NULL; /* optional values, test flags for presence */
  this->sid         = 0;
  this->ptime       = 0;
  this->rtime       = 0;
  this->counter     = 0;

  if ( ( this->flags & ( CAPR_IBX_PRESENT | CAPR_SID_PRESENT ) ) != 0 ) {
    if ( ( this->flags & CAPR_IBX_PRESENT ) != 0 ) {
      this->addr = &capr_pkt[ off ];
      off += CAPR_IBX_SIZE;
      if ( ( this->flags & CAPR_SID_PRESENT ) != 0 )
        goto sid_present;
    }
    else { /* sid must be present */
    sid_present:;
      ::memcpy( &this->sid, &capr_pkt[ off ], sizeof( this->sid ) );
      off += CAPR_SID_SIZE;
    }
  }
  if ( ( this->flags & ( CAPR_PTM_PRESENT | CAPR_RTM_PRESENT |
                         CAPR_CTR_PRESENT ) ) != 0 ) {
    if ( ( this->flags & CAPR_PTM_PRESENT ) != 0 ) {
      this->ptime = get_u64<MD_BIG>( &capr_pkt[ off ] );
      off += CAPR_PTM_SIZE;
    }
    if ( ( this->flags & CAPR_RTM_PRESENT ) != 0 ) {
      this->rtime = get_u64<MD_BIG>( &capr_pkt[ off ] );
      off += CAPR_RTM_SIZE;
    }
    if ( ( this->flags & CAPR_CTR_PRESENT ) != 0 ) {
      this->counter = get_u32<MD_BIG>( &capr_pkt[ off ] );
      off += CAPR_CTR_SIZE;
    }
  }
  this->msg_data = &capr_pkt[ off ];
  if ( this->data_len < off )
    return ERR_TRUNCATED_MESSAGE;
  this->msg_data_len = (uint32_t) ( this->data_len - off );
  return DECODE_OK;
}
