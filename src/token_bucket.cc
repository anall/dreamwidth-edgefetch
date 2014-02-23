#include "token_bucket.h"
#include <algorithm>
#include <unistd.h>
#include <assert.h>

static const uint32_t USEC_PER_SEC = 1E6;

static uint32_t diff_us(timeval &t1, timeval &t2) {
    return (((t1.tv_sec - t2.tv_sec) * 1000000) + 
            (t1.tv_usec - t2.tv_usec));
}
 

TokenBucket::TokenBucket( uint32_t tps, uint32_t limit ) : tokensPerSecond_(tps), tokenLimit_(limit), tokens_(limit) {
    usecPerToken_ = USEC_PER_SEC / tps;
    gettimeofday( &lastTokenAdd_, NULL );
}

void TokenBucket::consumeAndWait() {
    // Maybe add tokens
    timeval tmp;
    gettimeofday( &tmp, NULL );
    uint32_t elapsedUs = diff_us( tmp, lastTokenAdd_ );
    uint32_t tokensToAdd = elapsedUs / usecPerToken_;
    if ( tokensToAdd > 0 ) {
#ifdef TOKEN_DEBUG
        printf(" * Adding tokens: %i\n",tokensToAdd);
#endif
        lastTokenAdd_ = tmp;
        tokens_ = std::min( tokenLimit_, tokens_ + tokensToAdd );
    }
    if ( tokens_ > 0 ) {
        --tokens_;
    } else {
        assert( usecPerToken_ > elapsedUs );
        uint32_t usecToWait = usecPerToken_ - elapsedUs;
        usleep( usecToWait );
        gettimeofday( &lastTokenAdd_, NULL );
    }
#ifdef TOKEN_DEBUG
    printf(" * Tokens: %i\n",tokens_);
#endif

}
