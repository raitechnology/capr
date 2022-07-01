#ifndef __rai_capr__ev_capr_h__
#define __rai_capr__ev_capr_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raikv/ev_tcp.h>
#include <raikv/route_ht.h>

namespace rai {
namespace capr {

struct CaprSession;

struct EvCaprListen : public kv::EvTcpListen {
  CaprSession * sess;
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvCaprListen( kv::EvPoll &p ) noexcept;
  virtual EvSocket *accept( void ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept {
    return this->kv::EvTcpListen::listen2( ip, port, opts, "capr_listen", -1 );
  }
};

struct EvPrefetchQueue;
struct CaprMsgOut;
struct CaprMsgIn;

struct CaprSubRoute {
  uint32_t hash;
  uint32_t msg_cnt;
  uint16_t len;
  char     value[ 2 ];
};

enum CaprSubStatus {
  CAPR_SUB_OK        = 0,
  CAPR_SUB_EXISTS    = 1,
  CAPR_SUB_NOT_FOUND = 2
};

struct CaprSubRoutePos {
  CaprSubRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct CaprSubMap {
  kv::RouteVec<CaprSubRoute> tab;

  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }

  size_t sub_count( void ) const {
    return this->tab.pop_count();
  }
  void release( void ) {
    this->tab.release();
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  CaprSubStatus put( uint32_t h,  const char *sub,  size_t len,
                     bool &collision ) {
    kv::RouteLoc   loc;
    uint32_t       hcnt;
    CaprSubRoute * rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->msg_cnt = 0;
      return CAPR_SUB_OK;
    }
    return CAPR_SUB_EXISTS;
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  CaprSubStatus updcnt( uint32_t h,  const char *sub,  size_t len ) const {
    CaprSubRoute * rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    rt->msg_cnt++;
    return CAPR_SUB_OK;
  }
  /* find tab[ sub ] */
  CaprSubStatus find( uint32_t h,  const char *sub,  size_t len,
                      bool &collision ) {
    kv::RouteLoc   loc;
    uint32_t       hcnt;
    CaprSubRoute * rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return CAPR_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return CAPR_SUB_OK;
  }
  /* remove tab[ sub ] */
  CaprSubStatus rem( uint32_t h,  const char *sub,  size_t len,
                     bool &collision ) {
    kv::RouteLoc   loc;
    uint32_t       hcnt;
    CaprSubRoute * rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    collision = ( hcnt > 1 );
    this->tab.remove( loc );
    return CAPR_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( CaprSubRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( CaprSubRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  bool rem_collision( CaprSubRoute *rt ) {
    kv::RouteLoc loc;
    CaprSubRoute * rt2;
    rt->msg_cnt = ~(uint32_t) 0;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        if ( rt2->msg_cnt != ~(uint32_t) 0 )
          return true;
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};

struct CaprWildMatch {
  CaprWildMatch           * next,
                          * back;
  pcre2_real_code_8       * re;
  pcre2_real_match_data_8 * md;
  uint32_t                  msg_cnt;
  uint16_t                  len;
  char                      value[ 2 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  CaprWildMatch( size_t patlen,  const char *pat,  pcre2_real_code_8 *r,
               pcre2_real_match_data_8 *m )
    : next( 0 ), back( 0 ), re( r ), md( m ), msg_cnt( 0 ),
      len( (uint16_t) patlen ) {
    ::memcpy( this->value, pat, patlen );
    this->value[ patlen ] = '\0';
  }
  static CaprWildMatch *create( size_t patlen,  const char *pat,
                           pcre2_real_code_8 *r, pcre2_real_match_data_8 *m ) {
    size_t sz = sizeof( CaprWildMatch ) + patlen - 2;
    void * p  = ::malloc( sz );
    if ( p == NULL ) return NULL;
    return new ( p ) CaprWildMatch( patlen, pat, r, m );
  }
};

struct CaprPatternRoute {
  uint32_t                     hash,       /* hash of the pattern prefix */
                               count;
  kv::DLinkList<CaprWildMatch> list;
  uint16_t                     len;        /* length of the pattern subject */
  char                         value[ 2 ]; /* the pattern subject */
};

struct CaprPatternRoutePos {
  CaprPatternRoute * rt;
  uint32_t v;
  uint16_t off;
};

struct CaprPatternMap {
  kv::RouteVec<CaprPatternRoute> tab;
  size_t sub_count;

  CaprPatternMap() : sub_count( 0 ) {}
  bool is_null( void ) const {
    return this->tab.vec_size == 0;
  }
  void release( void ) noexcept;
  /* put in new sub
   * tab[ sub ] => {cnt} */
  CaprSubStatus put( uint32_t h,  const char *sub,  size_t len,
                   CaprPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    rt = this->tab.upsert2( h, sub, len, loc, hcnt );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    collision = ( hcnt > 0 );
    if ( loc.is_new ) {
      rt->count = 0;
      rt->list.init();
      return CAPR_SUB_OK;
    }
    return CAPR_SUB_EXISTS;
  }

  CaprSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    CaprPatternRoute *&rt ) {
    rt = this->tab.find( h, sub, len );
    if ( rt == NULL )
      return CAPR_SUB_NOT_FOUND;
    return CAPR_SUB_OK;
  }
  CaprSubStatus find( uint32_t h,  const char *sub,  size_t len,
                    CaprPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    return this->find( h, sub, len, loc, rt, collision );
  }
  CaprSubStatus find( uint32_t h,  const char *sub,  size_t len,
                  kv::RouteLoc &loc, CaprPatternRoute *&rt,  bool &collision ) {
    uint32_t hcnt;
    rt = this->tab.find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return CAPR_SUB_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return CAPR_SUB_OK;
  }
  /* iterate first tab[ sub ] */
  bool first( CaprPatternRoutePos &pos ) {
    pos.rt = this->tab.first( pos.v, pos.off );
    return pos.rt != NULL;
  }
  /* iterate next tab[ sub ] */
  bool next( CaprPatternRoutePos &pos ) {
    pos.rt = this->tab.next( pos.v, pos.off );
    return pos.rt != NULL;
  }
  bool rem_collision( CaprPatternRoute *rt,  CaprWildMatch *m ) {
    kv::RouteLoc       loc;
    CaprPatternRoute * rt2;
    CaprWildMatch    * m2;
    m->msg_cnt = ~(uint32_t) 0;
    if ( (rt2 = this->tab.find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        for ( m2 = rt2->list.tl; m2 != NULL; m2 = m2->back ) {
          if ( m2->msg_cnt != ~(uint32_t) 0 )
            return true;
        }
      } while ( (rt2 = this->tab.find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
};

struct EvCaprService : public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }

  kv::RoutePublish & sub_route;
  CaprSubMap     sub_tab;
  CaprPatternMap pat_tab;
  CaprSession  * sess;
  uint64_t       ms, bs,  /* msgs sent, bytes sent */
                 mr, br,  /* msgs recv, bytes recv */
                 timer_id;
  char           inbox[ 32 + 4 ]; /* _INBOX.127-000-000-001.6EB8C0CB.> */
  uint32_t       inboxlen;
  uint64_t       sid;

  EvCaprService( kv::EvPoll &p,  const uint8_t t )
    : kv::EvConnection( p, t ), sub_route( p.sub_route ) {}
  void initialize_state( uint64_t id ) {
    this->sess = NULL;
    this->ms = this->bs = 0;
    this->mr = this->br = 0;
    this->timer_id = id;
    this->inboxlen = 0;
    this->sid = 0;
  }
  void send( CaprMsgOut &rec,  size_t off,  const void *data,
             size_t data_len ) noexcept;
  void reassert_subs( CaprMsgIn &rec ) noexcept;
  void add_sub( CaprMsgIn &rec ) noexcept;
  void add_subscription( const char *sub,  size_t len,  const char *reply,
                         size_t replylen,  bool is_wild ) noexcept;
  void rem_sub( CaprMsgIn &rec ) noexcept;
  void rem_all_sub( void ) noexcept;
  bool fwd_pub( CaprMsgIn &rec ) noexcept;
  bool fwd_msg( kv::EvPublish &pub,  const void *sid,  size_t sid_len ) noexcept;
  bool fwd_inbox( kv::EvPublish &pub ) noexcept;
  void get_inbox_addr( kv::EvPublish &pub,  const char *&subj,
                       uint8_t *addr ) noexcept;
  void pub_session( uint8_t code ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept;
};

static inline bool is_rng( uint8_t c, uint8_t x, uint8_t y ) {
  return c >= x && c <= y;
}
static inline bool is_capr_pub( uint8_t c )  { return is_rng( c, 'a', 'z' ); }
static inline bool is_capr_sess( uint8_t c ) { return is_rng( c, ' ', '/' ); }
static inline bool is_capr_sub( uint8_t c )  { return is_rng( c, 'A', 'Z' ); }
static inline bool is_capr_meta( uint8_t c ) { return is_rng( c, '0', '@' ); }

static const uint8_t
  /* session ' ' -> '/' */
  CAPR_SESSION_INFO  = '!', /* session info are sent every 60 seconds w/stats */
  CAPR_SESSION_START = '+', /* sent at the start of a session */
  CAPR_SESSION_STOP  = '-', /* sent when the session ends */
  CAPR_SESSION_SUBS  = '/', /* sent to publishers by clients to notify */
                              /* subject interest when out of sync (reassert) */
  CAPR_SESSION_PUB   = '#', /* sent to subscribers to notify start/stop pub */

  /* meta '0' -> '@' */
  CAPR_REJECT   = '0',
  CAPR_ACK      = '1',
  CAPR_QUALITY  = '@',
  CAPR_PING     = '?',

  /* subs 'A' -> 'Z' */
  CAPR_CANCEL    = 'C', /* cancel a listen or a subscribe */
  CAPR_LISTEN    = 'L', /* listen does not expect an initial value */
  CAPR_SUBSCRIBE = 'S', /* subscribe client expects initial value or status */
  CAPR_SNAP_REQ  = 'X', /* one shot snapshot request */
  CAPR_DICT_REQ  = 'Z', /* one shot dictionary request */

  /* pubs 'a' -> 'z' */
  CAPR_DROP       = 'd', /* when publishers stop sending updates to a subject */
  CAPR_INITIAL    = 'i', /* initial subject message */
  CAPR_PUBLISH    = 'p', /* generic publish, any kind of msg, not categorized */
  CAPR_RECAP      = 'r', /* recap, generated by publisher for current state */
  CAPR_STATUS     = 's', /* a status message, not data */
  CAPR_UPDATE     = 'u', /* delta update */
  CAPR_VERIFY     = 'v', /* verify is delta that initializes if not cached */
  CAPR_SNAP_REPLY = 'x', /* one shot reply to a snapshot request */
  CAPR_DICT_REPLY = 'z'; /* one shot reply to a dictionary request */

static const uint32_t
  CAPR_SESSION_IVAL = 60;

struct CaprSession {
  uint64_t sid,           /* session id (unique endpoint) */
           stime;         /* start time */
  uint32_t start_count,   /* count of session start recvd */
           stop_count,    /* count of session stop recvd */
           info_count,    /* count of session info recvd */
           reassert_sent, /* count of reasserts sent */
           info_sent,     /* count of session info sent */
           id_len;        /* len msg in this->id */
  char   * addr,          /* addr of session */
         * user,          /* user of session */
         * host,          /* host of session */
         * app;           /* app  of session */
  uint8_t  addr_len,
           user_len,
           host_len,
           app_len;
  char     id[ 4 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  CaprSession() : sid( 0 ), stime( 0 ),
                  start_count( 0 ), stop_count( 0 ),
                  info_count( 0 ), reassert_sent( 0 ), info_sent( 0 ),
                  id_len( 0 ), addr( 0 ), user( 0 ), host( 0 ), app( 0 ),
                  addr_len( 0 ), user_len( 0 ), host_len( 0 ), app_len( 0 ) {}

  static CaprSession *create( const char *addr,
                              const char *user,
                              const char *host,
                              const char *app,
                              uint64_t sid ) noexcept;
  CaprSession *copy( void ) const noexcept;
};

static const uint8_t CAPR_MAGIC         = 0xca, /* hdr magic, first byte */
                     CAPR_IBX_PRESENT   = 0x80, /* hdr flags, fld present */
                     CAPR_SID_PRESENT   = 0x40, /* session id present */
                     CAPR_PTM_PRESENT   = 0x20, /* pub time present */
                     CAPR_RTM_PRESENT   = 0x10, /* route time present */
                     CAPR_CTR_PRESENT   = 0x08, /* update counter present */
                     CAPR_VERSION       = 0x01, /* hdr version number 1 */
                     CAPR_VERSION_MASK  = 0x03, /* 4 versions numbers */
                     CAPR_HDR_SIZE      = 12, /* capr frame header */
                     CAPR_SID_SIZE      = 8,  /* session id size */
                     CAPR_IBX_SIZE      = 12, /* inbox id size */
                     CAPR_PTM_SIZE      = 8,  /* publish time size */
                     CAPR_RTM_SIZE      = 8,  /* cache time size */
                     CAPR_CTR_SIZE      = 4;  /* update counter size */
static const size_t  CAPR_MAX_SUBJ_LEN  = 1032; /* 4 segs, 1024 encoded */

struct CaprMsgOut {
  uint8_t  capr_byte,       /* CAPR_MAGIC */
           code,            /* CAPR_LISTEN, CAPR_PING, etc */
           msg_enc,         /* (uint8_t) RAIMSG_TYPE_ID, or zero for no type */
           flags;           /* CAPR_xxx_PRESENT bits */
  uint32_t data_len,        /* length of data payload */
           subj_hash;       /* crc_c hash of subject */
  uint8_t  buf[ 2 * 1024 ]; /* space for subject, inbox, session */

  /* publish without publish time / create time */
  uint32_t encode_publish( CaprSession &sess,  const uint8_t *addr,
                           const char *subj,  uint8_t code,
                           size_t msg_len,  uint32_t msg_enc ) noexcept;
#if 0
  /* publish with pub time from link and create time from source */
  uint32_t encode_publish_time( CaprSession &sess,  const uint8_t *addr,
                                const char *subj,  uint8_t code,
                                size_t msg_len,  uint32_t msg_enc,
                                uint64_t ptim,  uint64_t ctim,
                                uint32_t *counter ) noexcept;
  /* request with inbox reply */
  uint32_t encode_request( CaprSession &sess,  uint32_t inbox_id,
                           const char *subj,  uint8_t code ) noexcept;
  /* cancel sub or other non-inbox requests */
  uint32_t encode_cancel( CaprSession &sess,  const char *subj,
                          uint8_t code ) noexcept;
  /* request with inbox reply and message */
  uint32_t encode_request_msg( CaprSession &sess,  uint32_t inbox_id,
                               const char *subj,  uint8_t code,
                               size_t msg_len,  uint8_t msg_enc ) noexcept;
#endif
};

enum CaprDecodeStatus {
  DECODE_OK = 0,
  ERR_MISSING_SUBJECT   = -1,
  ERR_BAD_SUBJECT_SEG   = -2,
  ERR_TRUNCATED_SUBJECT = -3,
  ERR_TRUNCATED_MESSAGE = -4,
  ERR_BAD_PROTO_MAGIC   = -5,
  ERR_BAD_PROTO_VERSION = -6
};

struct CaprMsgIn {
  uint8_t   capr_byte, /* +--  CAPR_MAGIC */
            code,      /* |    CAPR_SNAP_REQ */
            msg_enc,   /* |    msg encoding */
            flags;     /* |    CAPR_xxx_PRESENT */
  uint32_t  data_len,  /* |    payload length */
            subj_hash, /* +-- 12 byte header always present + subject */
            msg_data_len,
            counter,
            subj_len;
  uint64_t  sid,
            ptime,
            rtime;
  uint8_t * subj,
          * msg_data,
          * addr;

  /* decode a message, return = 0 success, < 0 error, > 0 bytes needed */
  int32_t decode( uint8_t *capr_pkt,  size_t pkt_size ) noexcept;
  uint32_t get_subscription( char *s,  bool &is_wild ) noexcept;
  uint32_t get_inbox( char *buf ) noexcept;
  uint32_t get_subject( char *s ) noexcept;
};

}
}
#endif
