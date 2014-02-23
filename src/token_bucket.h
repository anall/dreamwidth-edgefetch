#ifndef _TOKEN_BUCKET_H_SNR9NU82_
#define _TOKEN_BUCKET_H_SNR9NU82_
#include <stdint.h>
#include <sys/time.h>

class TokenBucket {
private:
    uint32_t tokensPerSecond_, tokenLimit_, tokens_;
    uint32_t usecPerToken_;
    timeval lastTokenAdd_;
public:
    TokenBucket(uint32_t tokensPerSecond, uint32_t limit);

    void consumeAndWait();
};

#endif /* end of include guard: _TOKEN_BUCKET_H_SNR9NU82_ */
