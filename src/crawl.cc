#include <stdio.h>
#include <curl/curl.h>
#include <assert.h>
#include <string>
#include <set>
#include <deque>

#ifdef HAS_CPP11
#include <unordered_map>
#include <unordered_set>

namespace local {
    using std::unordered_map;
    using std::unordered_set;
}

#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>

namespace local {
    using std::tr1::unordered_map;
    using std::tr1::unordered_set;
}
#endif

#include <json/json.h>
#include <sqlite3.h>

#include <string.h>
#include <getopt.h>

#include <signal.h>
#include <stdlib.h>

#include "token_bucket.h"

static bool wantsTerminate = false, wantsSave = false;

class Crawler {
private:
    CURL *_curl;
    sqlite3 *_db;

    sqlite3_stmt *_acctReplace;
    sqlite3_stmt *_acctIIgnore;
    sqlite3_stmt *_acctError;
    sqlite3_stmt *_storeEdge;

    TokenBucket _bucket;

    struct UserData {
        unsigned int _id;
        std::string _type;
    };

    class CrawlResult {
    public:
        unsigned int _id;
        std::string _name;

        char _type;
        bool _success;
        std::string _why;

        CrawlResult(std::string name = "<invalid>") : _name(name), _success(false), _type('?') {}

        CrawlResult return_why(std::string v) {
            _why = v;
            return *this;
        }

        CrawlResult return_why(std::string v, json_object *jObj) {
            _why = v;
            json_object_put( jObj );
            return *this;
        }

        ~CrawlResult() {}
    };

    local::unordered_map<std::string,UserData> _newUsers;
    local::unordered_set<std::string> _seen;

    local::unordered_map<std::string, CrawlResult> _fetched;

    local::unordered_set<std::string> _toFetch;
    local::unordered_set<std::string> _pendToFetch;

    char unBuf[30];
    char miscBuf[2048];

    static size_t __curlWrite(char *ptr, size_t size, size_t nmemb, void *userdata);

    void _handleSqlFail(int rc, const char *v = "<unknown>");

    void _load();

    void _finalize(size_t *failed, bool midSave);
public:
    Crawler();

    void addAccount(const char *, json_object *);

    void addPending(std::string name) {
        _toFetch.insert( name );
    }

    int run(int argc, char **argv);

    void runCrawl();

    CrawlResult fetchUser(std::string name);
    void parseAndStore(const char*, int, json_object*);
};

static void _terminate(int v) {
    wantsTerminate = true;
    printf("                                      <terminating>"
        "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b");
    fflush(stdout);

    signal(SIGINT,SIG_DFL);
    signal(SIGTERM,SIG_DFL);
}

static void _usr1(int v) {
    wantsSave = true;                           //<----------->
    printf("                                      <save pend. >"
        "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b");
    fflush(stdout);
}

Crawler::Crawler() : _bucket(3,3) {
    _curl = curl_easy_init();
    assert( _curl );

    curl_easy_setopt(_curl, CURLOPT_USERAGENT, "EdgeFetcher/0.3 ( anall@andreanall.com )");
    curl_easy_setopt(_curl, CURLOPT_NOSIGNAL, 1); 

    int rc = sqlite3_open("data.db", &_db);
    if ( rc ) _handleSqlFail( rc, "open" );
    
    rc = sqlite3_prepare_v2(_db, "REPLACE INTO account (id,username,journaltype,fetched,failed) VALUES (?,?,?,?,?);", -1, &_acctReplace, NULL);
    if ( rc ) _handleSqlFail( rc, "REPL acct" );

    rc = sqlite3_prepare_v2(_db, "INSERT OR IGNORE INTO account (id,username,journaltype,fetched,failed) VALUES (?,?,?,?,?);", -1, &_acctIIgnore, NULL);
    if ( rc ) _handleSqlFail( rc, "INS IGNORE acct" );

    rc = sqlite3_prepare_v2(_db, "REPLACE INTO account_errors (username,error) VALUES (?,?);", -1, &_acctError, NULL);
    if ( rc ) _handleSqlFail( rc, "REPL acct_error" );
    
    rc = sqlite3_prepare_v2(_db, "INSERT OR IGNORE INTO edges (edge,src,dest) VALUES (?,?,?);", -1, &_storeEdge, NULL);
    if ( rc ) _handleSqlFail( rc, "INS IGNORE store_idge" );

    signal(SIGINT,_terminate);
    signal(SIGTERM,_terminate);
    signal(SIGUSR1,_usr1);
    signal(SIGUSR2,_terminate);
}

int Crawler::run(int argc, char **argv) {
    int skip_pending = 0;
    int iters = -1;
    int c;
    int option_index = 0;

    while (1) {
        static struct option long_options[] = {
            {"no-load",    no_argument,       &skip_pending, 1},
            {"iterations", no_argument,       NULL, 'i'},
            {0, 0, 0, 0}
        };

        c = getopt_long (argc, argv, "ni:",long_options, &option_index);

        if ( c == -1 ) break;
        switch (c) {
        case 0: break;
        case 'n': skip_pending = 1; break;
        case 'i': iters = atoi(optarg); break;
        default: return -1;
        }
    }

    printf("Fetching edges! ");
    if ( iters != -1 ) printf(" ( %i iteration limit )",iters);
    if ( skip_pending ) printf(" ( no preloaded pending items )");
    printf("\n");

    _load();
    if ( skip_pending )
        _toFetch.clear();

    while ( optind < argc ) {
        const char *v = argv[optind++];
        printf(" * Seeding %s\n", v);
        _toFetch.insert(v);
    }

    while ( _toFetch.size() ) {
        runCrawl();
    }
    return 0;
}

void Crawler::addAccount(const char *acctId, json_object *cv) {
    json_object *oName = json_object_object_get( cv, "name" );
    const char *name = json_object_get_string( oName );
    if ( ! name ) {
        printf("Fail, somehow!\n");
        abort();
    }
    if ( _seen.count( name ) ) return;

    json_object *oType = json_object_object_get( cv, "type" );
    const char *type = json_object_get_string( oType );

    _seen.insert( name );
    UserData ud;
    ud._id = atoi(acctId);
    ud._type = type;

    _newUsers[name] = ud;
}

void Crawler::_handleSqlFail(int rc, const char *v) {
    fprintf(stderr, "FAIL (%s): %s %i\n", v, sqlite3_errmsg(_db),rc);
    sqlite3_close(_db);

    abort();
}

static char *toSqlText(const std::string &n) {
    size_t l = n.length();
    char *rv = (char *)malloc( l + 2 );
    rv[l] = 0;
    strcpy( rv, n.c_str() );
    return rv;
}

static char *toSqlText(char v) {
    char *rv = (char *)malloc( 2 );
    rv[0] = v;
    rv[1] = 0;
    return rv;
}

static int runStatement(sqlite3_stmt *stmt) {
    int rc = SQLITE_BUSY;
    while ( rc != SQLITE_DONE ) {
        rc = sqlite3_step(stmt);
        if ( rc && rc != SQLITE_DONE ) {
            sqlite3_reset(stmt);
            return rc;
        }
    }
    sqlite3_reset(stmt);
    return SQLITE_OK;
}

void Crawler::_finalize(size_t *fvRet, bool midSave) {
    int rc;
    size_t tv;
    if ( fvRet == 0 ) fvRet = &tv;
    *fvRet = 0;

    rc= sqlite3_exec(_db, "BEGIN", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "BEGIN <existing>");

    {
        local::unordered_map<std::string, CrawlResult>::iterator it;

        for ( it = _fetched.begin(); it != _fetched.end(); it++ ) {
            const std::string &name = (*it).first;
            const CrawlResult &cr = (*it).second;

            sqlite3_bind_int ( _acctReplace, 1, cr._id );
            sqlite3_bind_text( _acctReplace, 2, toSqlText( name ), name.length(), free );
            sqlite3_bind_text( _acctReplace, 3, toSqlText( cr._type ), 1, free );
            sqlite3_bind_int ( _acctReplace, 4, 1 );
            sqlite3_bind_int ( _acctReplace, 5, !cr._success );

            rc = runStatement( _acctReplace );
            if ( rc ) _handleSqlFail(rc,"Adding account");

            if ( ! cr._success ) {
                (*fvRet)++;
                sqlite3_bind_text( _acctError, 1, toSqlText( name ), name.length(), free );
                sqlite3_bind_text( _acctError, 2, toSqlText( cr._why ), cr._why.length(), free );

                rc = runStatement( _acctError );
                if ( rc ) _handleSqlFail(rc,"Adding account failure");
            }
        }

        _fetched.clear();
    }

    rc = sqlite3_exec(_db, "COMMIT", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "COMMIT <existing>");


    rc = sqlite3_exec(_db, "BEGIN", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "BEGIN <new>");

    {
        local::unordered_map<std::string,UserData>::iterator it;

        for ( it = _newUsers.begin(); it != _newUsers.end(); it++ ) {
            const std::string &name = (*it).first;
            const UserData &ud = (*it).second;

            sqlite3_bind_int ( _acctIIgnore, 1, ud._id );
            sqlite3_bind_text( _acctIIgnore, 2, toSqlText( name ), name.length(), free );
            sqlite3_bind_text( _acctIIgnore, 3, toSqlText( ud._type ), 1, free );
            sqlite3_bind_int ( _acctIIgnore, 4, 0 );
            sqlite3_bind_int ( _acctIIgnore, 5, 0 );

            rc = runStatement( _acctIIgnore );
            if ( rc ) _handleSqlFail(rc,"Adding account good");

            _pendToFetch.insert( name );
        }
    }

    rc = sqlite3_exec(_db, "COMMIT", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "COMMIT <new>");

    _newUsers.clear();

    // FIXME: Use JSON generation
    FILE *data = fopen("data.json","w");
    if ( data ) {
        fprintf(data,"{\"seen\":%lu,\"pending_round\":%lu,\"to_fetch\":%lu,\"mid\":%i}",_seen.size(),_pendToFetch.size(),_toFetch.size(),midSave);
        fclose(data);
    }

    if ( midSave ) return;

    _toFetch.insert( _pendToFetch.begin(), _pendToFetch.end() );
    _pendToFetch.clear();

}

void Crawler::_load() {
    size_t ct = 0;
    printf("%8lu LOADING"
        "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b", ct);
    fflush(stdout);

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(_db, "SELECT username, fetched FROM account", -1, &stmt, NULL);
    if ( rc ) _handleSqlFail( rc, "LOAD <stmt>" );

    rc = SQLITE_ROW;
    while ( rc != SQLITE_DONE ) {
        rc = sqlite3_step(stmt);
        if ( rc == SQLITE_ROW ) {
            const char *uname = (const char *)sqlite3_column_text(stmt,0);
            int fetched = sqlite3_column_int(stmt,1);

            if ( fetched )
                _seen.insert( uname );
            else
                _toFetch.insert( uname );

            if ( (++ct % 50) == 0 ) {
                printf("%8lu"
                    "\b\b\b\b\b\b\b\b", ct);
                fflush(stdout);
            }
        } else if ( rc == SQLITE_DONE ) {
            break;
        } else if ( rc ) {
            _handleSqlFail( rc, "LOAD" );
        }
    }    
    printf("-------- LOADED %lu rows\n", ct);
    sqlite3_finalize(stmt);
}

void Crawler::runCrawl() {
    bool reloop = true;
    while ( reloop ) {
        reloop = false;
        printf("%8lu\b\b\b\b\b\b\b\b", _toFetch.size());
        fflush(stdout);
        while ( _toFetch.size() ) {
            size_t tFetch = _toFetch.size();
            std::string what = *( _toFetch.begin() );
            _toFetch.erase( what );

            if ( ( tFetch % 2 ) == 0 ) {
                printf("%8lu %-25s"
                    "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b"
                    "\b\b\b\b\b\b\b\b", tFetch, what.c_str() );
                fflush(stdout);
            }

            CrawlResult res = fetchUser( what );
            _fetched[ res._name ] = res;

            if ( ( tFetch & 255 ) == 0 && tFetch > 0 ) wantsSave = true;
            if ( wantsTerminate || wantsSave )
                break;
        }

        size_t newCt = _newUsers.size();
        printf("%8lu %-25s"
            "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b"
            "\b\b\b\b\b\b\b\b",newCt,"FINALIZE");
        fflush(stdout);

        size_t failed = 0;
        _finalize( &failed, wantsSave );

        if ( wantsSave )
            printf("%8lu %-25s ( %lu failed, %lu pending new, %lu pending ) %lu\n",_toFetch.size(),"MID_SAVE",failed,_pendToFetch.size(),newCt,time(NULL));
        else
            printf("-------- %-25s ( %lu new, %lu failed ) %lu\n","DONE",newCt,failed,time(NULL));

        if ( wantsSave )
            reloop = true;
        wantsSave = false;

        if ( wantsTerminate )
            exit(0);
    }
}

size_t Crawler::__curlWrite(char *ptr, size_t size, size_t nmemb, void *userdata) {
    std::string *d = (std::string *)userdata;

    d->append(ptr, size*nmemb);

    return size*nmemb;
}

Crawler::CrawlResult Crawler::fetchUser(std::string _who) {
    _bucket.consumeAndWait();

    CrawlResult rv(_who);

    strcpy( unBuf, _who.c_str() );

    for ( int i = 0; i < strlen(unBuf); i++ ) {
        if ( unBuf[i] == '_' ) unBuf[i] = '-';
    }
    sprintf(miscBuf,"http://%s.dreamwidth.org/data/edges",unBuf);
    curl_easy_setopt(_curl, CURLOPT_URL, miscBuf);

    std::string dv;

    curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, __curlWrite);
    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, &dv);

    curl_easy_perform(_curl);

    curl_easy_setopt(_curl, CURLOPT_WRITEDATA, 0);

    json_object *jObj = json_tokener_parse( dv.c_str() );
    if ( is_error(jObj) ) return rv.return_why("Failed to parse");

    json_object *error = json_object_object_get( jObj, "error" );
    const char *cError = json_object_get_string( error );
    if ( cError )
        return rv.return_why(cError, jObj);

    json_object *id = json_object_object_get( jObj, "account_id" );
    int acctId = json_object_get_int( id );

    json_object *name = json_object_object_get( jObj, "name" );
    const char *cName  = json_object_get_string( name );
    if ( !cName )
        return rv.return_why("Couldn't get name",jObj);

    json_object *accts = json_object_object_get( jObj, "accounts" );
    if ( is_error(accts) )
        return rv.return_why("Couldn't get accounts",jObj);

    json_object *type = json_object_object_get( jObj, "account_type" );
    const char *cType = json_object_get_string( type );
    if ( !cType )
        return rv.return_why("Couldn't get type",jObj);

    rv._type = cType[0];
    rv._name = cName;
    rv._id = acctId;

    _seen.insert( cName );

    {
        json_object_object_foreach(accts, k, v) {
            addAccount( k, v );
        }
    }

    int rc = sqlite3_exec(_db, "BEGIN", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "BEGIN <new>");
    
    parseAndStore( "trusted",    acctId, jObj );
    parseAndStore( "trusted_by", acctId, jObj );

    parseAndStore( "watched",    acctId, jObj );
    parseAndStore( "watched_by", acctId, jObj );

    parseAndStore( "member",    acctId, jObj );
    parseAndStore( "member_of", acctId, jObj );
    
    parseAndStore( "maintainer", acctId, jObj );
    
    rc = sqlite3_exec(_db, "COMMIT", 0, 0, 0);
    if ( rc ) _handleSqlFail(rc, "COMMIT <existing>");

    json_object_put( jObj );

    rv._success = true;

    return rv;
}
    
void Crawler::parseAndStore(const char *type, int from, json_object *oData) {
    sqlite3_bind_text( _storeEdge, 1, type, strlen( type ), SQLITE_TRANSIENT );
    sqlite3_bind_int ( _storeEdge, 2, from );

    json_object *data = json_object_object_get( oData, type );
    if ( ! data || is_error(data) )
        return;

    int rc;
    uint32_t len =  json_object_array_length( data );
    for ( uint32_t i = 0; i < len; i++ ) {
        json_object *ai = json_object_array_get_idx( data, i );
        if ( !ai ) continue;
        int iId  = json_object_get_int( ai );
        sqlite3_bind_int ( _storeEdge, 3, iId );
    
        rc = runStatement( _storeEdge );
        if ( rc ) _handleSqlFail( rc, "FAIL STORE" );
    }
}

int main(int argc, char **argv) {
    Crawler c;
    c.run(argc, argv);
}
