#include <assert.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <unistd.h>
#include <stdlib.h>
#include <err.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <event.h>
#include <evhttp.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <limits.h>
#include <sysexits.h>

#include <libmemcached/memcached.h>

#include "erl_interface.h"
#include "ei.h"

#define BUFSIZE 1024
#define MAXUSERS 65536

static void settings_init(void);

struct settings {
    int port;
    char *inter;
    int verbose;
    int maxconns;
    int memcached_binary;
    char *memcached_pool;
} settings;

static void settings_init(void) {
    settings.port = 8080;
    settings.inter = "0.0.0.0";
    settings.maxconns = 65536;
    settings.verbose = 0;
    settings.memcached_pool = "127.0.0.1:11211";
    settings.memcached_binary = 1;
}

struct evhttp_request * clients[MAXUSERS+1];

int slots[MAXUSERS+1];

memcached_st *memc;

void cleanup(struct evhttp_connection *evcon, void *arg)
{
    int *uidp = (int *) arg;
    fprintf(stderr, "disconnected uid %d\n", *uidp);
    clients[*uidp] = NULL;
}

char *fetch_memcached(const char *rkey)
{
    memcached_return rc;
    size_t cached_length;
    uint32_t flags;
    return memcached_get(memc, rkey, strlen(rkey), &cached_length, &flags, &rc);
}

void request_handler(struct evhttp_request *req, void *arg)
{
    const char *uri;
    uri = evhttp_request_uri(req);

    if(strncmp(uri, "/updates/", 9) != 0){
        evhttp_send_error(req, HTTP_NOTFOUND, "Not Found");
        if (settings.verbose > 0)
            fprintf(stderr, "URL not found.. needs to be /updates/... but was %s\n", uri);
        return;
    }
    
    const char *rkey;
    struct evkeyvalq args;
    TAILQ_INIT(&args);

    evhttp_parse_query(uri, &args);
    
    rkey = evhttp_find_header(&args, "rkey");
    
    if (NULL == rkey) {
        evhttp_send_error(req, HTTP_BADREQUEST, "Bad Request");
        if (settings.verbose > 0)
            fprintf(stderr, "RKey param not found in request URI %s\n", uri);
        evhttp_clear_headers(&args);
        return;
    }
    
    fprintf(stderr, "Using RKey: %s\n", rkey);
    
    char *cached = fetch_memcached(rkey);
    
    if (NULL == cached) {
        evhttp_send_error(req, HTTP_BADREQUEST, "Bad Request");
        fprintf(stderr, "RKey %s not found in Memcache!\n", rkey);
        evhttp_clear_headers(&args);
        return;
    }
    
    int uid = atoi(cached);

    fprintf(stderr, "Great, found RKey in Memcached: %s = %s and now UID %d\n", rkey, cached, uid);
    
	struct evbuffer *buf = evbuffer_new();
    
    evhttp_add_header(req->output_headers, "Content-Type", "text/html; charset=UTF-8");
    evhttp_add_header(req->output_headers, "Connection", "keep-alive");
    evhttp_add_header(req->output_headers, "Cache-Control", "no-cache");
    
    evhttp_send_reply_start(req, HTTP_OK, "OK");
    evbuffer_add_printf(buf, "Welcome, RKey: ‘%s’\n", rkey);
    evhttp_send_reply_chunk(req, buf);
    evbuffer_free(buf);

    clients[uid] = req;
    evhttp_clear_headers(&args);
    free(cached);
    
    evhttp_connection_set_closecb( req->evcon, cleanup, &slots[uid] );
}

void *cnode_run()
{
    int fd;                                  /* fd to Erlang node */
    int got;                                 /* Result of receive */
    unsigned char buf[BUFSIZE];              /* Buffer for incoming message */
    ErlMessage emsg;                         /* Incoming message */
 
    ETERM *uid, *msg;
 
    erl_init(NULL, 0);
 
    if (erl_connect_init(1, "secretcookie", 0) == -1)
        erl_err_quit("erl_connect_init");
 
    if ((fd = erl_connect("httpdmaster@localhost")) < 0)
        erl_err_quit("erl_connect");
 
    fprintf(stderr, "Connected to httpdmaster@localhost\n\r");
 
    struct evbuffer *evbuf;
 
    while (1) {
        got = erl_receive_msg(fd, buf, BUFSIZE, &emsg);
        if (got == ERL_TICK) {
            continue;
        } else if (got == ERL_ERROR) {
            fprintf(stderr, "ERL_ERROR from erl_receive_msg.\n");
            break;
        } else {
            if (emsg.type == ERL_REG_SEND) {
                fprintf(stderr, "type of emsg is ERL_REG_SEND\n");
                // get uid and body data from eg: {123, <<"Hello">>}
                uid = erl_element(1, emsg.msg);
                msg = erl_element(2, emsg.msg);
                char *token = (char *) ERL_BIN_PTR(uid);
                char *body = (char *) ERL_BIN_PTR(msg);
                int body_len = ERL_BIN_SIZE(msg);
                char *cached = fetch_memcached(token);
                int userid = atoi(cached);
                fprintf(stderr, "memc: %d\n\r", userid);
                if(clients[userid]){
                    fprintf(stderr, "Sending %d bytes to token %s\n", body_len, token);                
                    evbuf = evbuffer_new();
                    evbuffer_add(evbuf, (const void*)body, (size_t) body_len);
                    evhttp_send_reply_chunk(clients[userid], evbuf);
                    evhttp_send_reply_end(clients[userid]);
                    evbuffer_free(evbuf);
                }else{
                    fprintf(stderr, "Discarding %d bytes to uid %d - user not connected\n",
                            body_len, userid);                
                }
                free(cached);
                erl_free_term(emsg.msg);
                erl_free_term(uid);
                erl_free_term(msg);
            }
        }
        fprintf(stderr, ".");
    }
    pthread_exit(0);
}

void init_memcached()
{
    memcached_return rc;
    memcached_server_st *server_pool;
    memc = memcached_create(NULL);
    assert(memc);
    
    server_pool = memcached_servers_parse(settings.memcached_pool);
    assert(server_pool);

    rc = memcached_server_push(memc, server_pool);
    assert(rc == MEMCACHED_SUCCESS);
    
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, settings.memcached_binary);
    assert(rc == MEMCACHED_SUCCESS);
    
    if (settings.verbose > 0)
        fprintf(stderr, "connected to Memcached server pool: %s. Total hosts: %d. Binary setting: %d\n", settings.memcached_pool, memc->number_of_hosts, settings.memcached_binary);
    
    memcached_server_list_free(server_pool);
}

static void usage(void) {
    printf("-p <num>      TCP port number to listen on (default: 11211)\n"
           "-l <ip_addr>  interface to listen on, default is INADDR_ANY\n"
           "-m <servers>  Memcached servers comma-seperated (default: 127.0.0.1:11211)\n"
           "-c <num>      max simultaneous connections, default is 1024\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           );
    return;
}
int main(int argc, char **argv)
{
    settings_init();
    
    struct rlimit rlim;
    int c;
        
    while (-1 != (c = getopt(argc, argv,
        "c:"  /* max simultaneous connections */
        "p:"  /* TCP port number to listen on */
        "l:"  /* interface to listen on, default is INADDR_ANY */
        "m:"  /* Memcached servers */
        "v"   /* verbose */
        "h"
        ))) {
        switch (c) {
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'p':
            settings.port = atoi(optarg);
            break;
        case 'l':
            settings.inter = optarg;
            break;
        case 'm':
            settings.memcached_pool = optarg;
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'v':
            settings.verbose++;
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        int maxfiles = settings.maxconns;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles + 3;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            fprintf(stderr, "failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }
    
    if (settings.verbose > 0)
        fprintf(stderr, "maxconns set to: %d\n", settings.maxconns);
    
    init_memcached();

    pthread_attr_t tattr;
    pthread_t helper;
    pthread_create(&helper, NULL, cnode_run, NULL);
    
    int i;
    for(i=0;i<=MAXUSERS;i++) slots[i]=i;
    
    struct evhttp *httpd;
    event_init();
    httpd = evhttp_start(settings.inter, settings.port);
    
    if (settings.verbose > 0)
        fprintf(stderr, "server listening on %s:%d\n", settings.inter, settings.port);
                
    evhttp_set_gencb(httpd, request_handler, NULL);
    event_dispatch();
    evhttp_free(httpd);
    memcached_free(memc);
    return 0;
}