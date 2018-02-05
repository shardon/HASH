#ifndef _HASH_H_
#define _HASH_H_

#ifndef __NO_SYSTEM_INCLUDES
#include <stdio.h>
#endif

#if defined(__cplusplus)
extern "C" {
#endif

#define HASH_MAXKEY 64		/* The max length of key */

/* Flags for hash table. */
#define HASH_LOCK 0x01		/* Locking access */
#define HASH_FROZEN 0x02	/* Automatic resizing not allowed */
#define HASH_NCOPY 0x04		/* Store pointer rather than copy data */

typedef void (*HFREE) (void *);
typedef unsigned int (*HFUN)(void *key, size_t length_of_key);
typedef void (*HWALK) (void *key,size_t key_size,void *data,size_t data_size);

typedef struct hashtable *HTAB ;

struct hashtable
{

	void*	entity ;
	int		(*put)(HTAB htab, void *key, size_t ks, void *data, size_t ds,void **rd);
	void*	(*get)(HTAB htab, void *key, size_t ks);
	int		(*del)(HTAB htab, void *key, size_t ks,void **rd);
	int		(*all)(HTAB htab,HWALK walk);
	void	(*prf)(HTAB htab,FILE *out);

};

extern HTAB hash_create(int size, int flags);
extern int hash_close(HTAB htab) ;

#endif
