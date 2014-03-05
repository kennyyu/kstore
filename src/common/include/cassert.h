#ifndef _CASSERT_H_
#define _CASSERT_H_

// Compile time asserts
// technique taken from:
// http://stackoverflow.com/questions/807244/c-compiler-asserts-how-to-implement

#define _impl_PASTE(a,b) a##b
#define _impl_CASSERT_LINE(predicate, line, file) \
    typedef char _impl_PASTE(assertion_failed_##file##_,line)[2*!!(predicate)-1];

#define CASSERT(predicate) _impl_CASSERT_LINE(predicate,__LINE__,__FILE__)

#endif
