#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#else
#include <raikv/win.h>
#endif
#include <capr/ev_capr.h>
#include <raikv/mainloop.h>

using namespace rai;
using namespace capr;
using namespace kv;

struct Args : public MainLoopVars { /* argv[] parsed args */
  int capr_port;
  Args() : capr_port( 0 ) {}
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  size_t num,  bool (*ini)( void * ) ) :
    MainLoop<Args>( m, args, num, ini ) {}

  EvCaprListen * capr_sv;
  bool capr_init( void ) {
    return Listen<EvCaprListen>( 0, this->r.capr_port, this->capr_sv,
                                 this->r.tcp_opts ); }
  bool init( void ) {
    if ( this->thr_num == 0 )
      printf( "capr:                 %d\n", this->r.capr_port );
    int cnt = this->capr_init();
    if ( this->thr_num == 0 )
      fflush( stdout );
    return cnt > 0;
  }
  static bool initialize( void *me ) noexcept {
    return ((Loop *) me)->init();
  }
};

int
main( int argc, const char *argv[] )
{
  EvShm shm( "capr_server" );
  Args  r;

  r.no_threads   = true;
  r.no_reuseport = true;
  r.no_map       = true;
  r.no_default   = true;
  r.all          = true;
  r.add_desc( "  -c capr  = listen capr port      (8866)\n" );
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  printf( "capr_version:         " kv_stringify( CAPR_VER ) "\n" );
  shm.print();
  r.capr_port = r.parse_port( argc, argv, "-c", "8866" );
  Runner<Args, Loop> runner( r, shm, Loop::initialize );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
