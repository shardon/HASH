#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "hash.h"

/* Structure for each entry in table. */
typedef struct hash_entry
{
    void *key;
    size_t key_size;
    void *data;
    size_t data_size;
    struct hash_entry *next;

}hash_entry;

typedef struct hash_entity
{
	HFUN hash_func;
	HFREE hash_free;
	size_t size;           /* Number of buckets. */
	size_t entries;        /* Number of entries in table. */
	hash_entry **buckets;
	unsigned int flags;
	pthread_mutex_t lock;
	float high_mark, low_mark;

	hash_entry *recycle; /* recycle bin */
	int rsize ; /*size of recycle bin */

}hash_entity;

static u_int hash_size(u_int s) 
{ 
	u_int i = 1;
	while(i < s) i <<= 1;
	return i;
} 

#if 1
// classic hash function
#define mix(a,b,c)				\
{						\
  a -= b; a -= c; a ^= (c>>13);			\
  b -= c; b -= a; b ^= (a<<8);			\
  c -= a; c -= b; c ^= (b>>13);			\
  a -= b; a -= c; a ^= (c>>12);			\
  b -= c; b -= a; b ^= (a<<16);			\
  c -= a; c -= b; c ^= (b>>5);			\
  a -= b; a -= c; a ^= (c>>3);			\
  b -= c; b -= a; b ^= (a<<10);			\
  c -= a; c -= b; c ^= (b>>15);			\
}
typedef unsigned int ub4;

static unsigned int hash_func(void *key, size_t length)
{
	register unsigned int a,b,c,len;
	char *k = key;

	/* Set up the internal state */
	len = length;
	a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
	c = 0;               /* the previous hash value */

	/*---------------------------------------- handle most of the key */
	while(len >= 12)
	{
		a += (k[0] +((ub4)k[1]<<8) +((ub4)k[2]<<16) +((ub4)k[3]<<24));
		b += (k[4] +((ub4)k[5]<<8) +((ub4)k[6]<<16) +((ub4)k[7]<<24));
		c += (k[8] +((ub4)k[9]<<8) +((ub4)k[10]<<16)+((ub4)k[11]<<24));
		mix(a,b,c);
		k += 12; len -= 12;
	}

	/*------------------------------------- handle the last 11 bytes */
	c += length;
	switch(len)
	{              /* all the case statements fall through */
		case 11: c+=((ub4)k[10]<<24);
		case 10: c+=((ub4)k[9]<<16);
		case 9 : c+=((ub4)k[8]<<8);
		/* the first byte of c is reserved for the length */
		case 8 : b+=((ub4)k[7]<<24);
		case 7 : b+=((ub4)k[6]<<16);
		case 6 : b+=((ub4)k[5]<<8);
		case 5 : b+=k[4];
		case 4 : a+=((ub4)k[3]<<24);
		case 3 : a+=((ub4)k[2]<<16);
		case 2 : a+=((ub4)k[1]<<8);
		case 1 : a+=k[0];
		/* case 0: nothing left to add */
	}
	mix(a,b,c);
	/*-------------------------------------------- report the result */
	return c;
}
#else
// string hash from perl
static unsigned int hash_func_perl(void *key, size_t length)
{
	register size_t i = length;
	register u_int hv = 0; /* could put a seed here instead of zero */
	register const unsigned char *s = (char *)key;
	while (i--)
	{
		hv += *s++;
		hv += (hv << 10);
		hv ^= (hv >> 6);
	}
	hv += (hv << 3);
	hv ^= (hv >> 11);
	hv += (hv << 15);

	return hv;
}
//string hash from glib
static unsigned int hash_func_glib (void *key, size_t length)
{
	const char *p = (char *)key;
	register unsigned int h = *p;
	register int i= 1 ;

	if (h) while( i < length ) h = (h << 5) -h + p[i++] ;

	return h;

}
#endif

static int myhash_rehash(hash_entity *ht);
static int myhash_put(HTAB htab, void *key, size_t ks, void *data, size_t ds,void **rd);
static void *myhash_get(HTAB htab, void *key, size_t ks);
static int myhash_del(HTAB htab, void *key, size_t ks,void **rd);
static void myhash_profile(HTAB htab ,FILE *out);
static int myhash_walk(HTAB htab ,HWALK walk);

/* Function to create a new hash table. */
extern HTAB hash_create(int size, int flags)
{
	HTAB htmp ;
	hash_entity *ht;

	htmp = (HTAB)malloc(sizeof(struct hashtable));

	size = hash_size(size);
	ht = malloc(sizeof(*ht));

	ht->size = size;
	ht->entries = 0;
	ht->flags = flags;
	ht->buckets = calloc(size, sizeof(*ht->buckets));
	pthread_mutex_init(&ht->lock, NULL);
	ht->high_mark = 0.7;
	ht->low_mark = 0.3;
	ht->hash_func = hash_func;
	ht->hash_free = NULL ;

	ht->recycle = NULL;
	ht->rsize = 0;

	htmp->put = myhash_put;
	htmp->get = myhash_get ;
	htmp->del = myhash_del;
	htmp->prf = myhash_profile;
	htmp->all = myhash_walk;

	htmp->entity = ht ;

	return htmp;
}

static int myhash_sethashfunction(HTAB htab, HFUN hf)
{
	hash_entity *ht = (hash_entity *)htab->entity;

	if(ht->entries) return -1;
	ht->hash_func = hf? hf: hash_func;
	return 0;
}

static int myhash_sethashfree(HTAB htab, HFREE hfree)
{
	hash_entity *ht = (hash_entity *)htab->entity;

	if(ht->entries) return -1;
	if(ht->hash_free) ht->hash_free = hfree;
	return 0;
}

static inline void lock_hash(hash_entity *ht)
{
	if(ht->flags & HASH_LOCK ) pthread_mutex_lock(&ht->lock);
}

static inline void unlock_hash(hash_entity *ht)
{
	if(ht->flags & HASH_LOCK ) pthread_mutex_unlock(&ht->lock);
}

static inline int hash_cmp(void *key, size_t ks, hash_entry *he)
{
	if(ks != he->key_size) return 1;
	//if(key == he->key) return 0;
	return memcmp(key, he->key, ks);
}

static inline hash_entry *
hash_addentry(hash_entity *ht, u_int hv, void *key, size_t ks, void *data, size_t ds)
{
	hash_entry *he = NULL ;

	if(ht->recycle)
	{
		he = ht->recycle ;
		ht->recycle = he->next ;
		ht->rsize -- ;
	}else
	{
		he = malloc(sizeof(hash_entry));
	}

	he->key = memcpy(malloc(ks),key,ks);
	he->key_size = ks;

	he->data = data;
	he->data_size = ds;

	he->next = ht->buckets[hv];
	ht->buckets[hv] = he;
	ht->entries++;

	return he;
}

/* Find in table. */
static void *myhash_get(HTAB htab, void *key, size_t ks)
{
    u_int hv;
    hash_entry *hr = NULL;

	hash_entity *ht = (hash_entity *)htab->entity;

    if(ks == (size_t) -1) ks = strlen(key) + 1;

    lock_hash(ht);
    hv = (ht->hash_func(key, ks) & (ht->size-1)) ;

    for(hr = ht->buckets[hv]; hr; hr = hr->next) if(!hash_cmp(key, ks, hr)) break;

    unlock_hash(ht);

	return( hr ? hr->data : NULL);
}

static int myhash_put(HTAB htab, void *key, size_t ks, void *data, size_t ds,void **rd)
{
    u_int hv;
    hash_entry *hr;
    int ret = 0;

	hash_entity *ht = (hash_entity *)htab->entity;

    if(ks == (size_t) -1) ks = strlen(key) + 1;

    lock_hash(ht);
    hv = (ht->hash_func(key, ks) & (ht->size-1)) ;

    for(hr = ht->buckets[hv]; hr; hr = hr->next)
	if(!hash_cmp(key, ks, hr)) break;

	if( !(ht->flags & HASH_NCOPY) && ds > 0)
		data = memcpy(malloc(ds),data,ds) ;

	if(hr)
	{
		if( !(ht->flags & HASH_NCOPY) && hr->data_size > 0) free(hr->data);
		else if(rd) *rd = hr->data;

		hr->data = data;

	}else
	{
		hash_addentry(ht, hv, key, ks, data, ds);
		ret = 1;
	}

	unlock_hash(ht);

	if(ret && !(ht->flags & HASH_FROZEN))
		if((float) ht->entries / ht->size > ht->high_mark)
			myhash_rehash(ht);
	return ret;
}

static int myhash_del(HTAB htab, void *key, size_t ks,void **rd)
{
	int hv;
	hash_entry *he = NULL, *hep = NULL;

	hash_entity *ht = (hash_entity *)htab->entity;

	if(ks == (size_t) -1) ks = strlen(key) + 1;

	lock_hash(ht);
    hv = (ht->hash_func(key, ks) & (ht->size-1)) ;

	for(he = ht->buckets[hv]; he; he = he->next)
	{
		if(!hash_cmp(key, ks, he)) break;
		hep = he;
	}

	if(he)
	{
		if(!(ht->flags & HASH_NCOPY) && he->data_size > 0) free(he->data);
		else if(rd) *rd = he->data;

		if(hep) hep->next = he->next;
		else ht->buckets[hv] = he->next;

		ht->entries--;

		free(he->key);

		if(ht->rsize < 2048)
		{
			he->next = ht->recycle ;
			ht->recycle = he ;
			ht->rsize ++;
		}else free(he);
	}

	unlock_hash(ht);
	if(he && !(ht->flags & HASH_FROZEN))
		if((float) ht->entries / ht->size < ht->low_mark) myhash_rehash(ht);

	return(he?0:1);
}

static int myhash_walk(HTAB htab ,HWALK walk)
{
	int i ;
	hash_entry *he= NULL;
	hash_entity *ht = (hash_entity *)htab->entity;

	if( !walk || !ht->entries ) return (ht->entries);

	for(i= 0; i< ht->size; i++)
    	for(he= ht->buckets[i]; he; he = he->next)
			walk(he->key,he->key_size, he->data, he->data_size);

	return(ht->entries);
}

extern int hash_close(HTAB htab)
{
    size_t i;

	hash_entity *ht = (hash_entity *)htab->entity;

	for(i = 0; i < ht->size; i++)
	{
		if(ht->buckets[i] != NULL)
		{
			hash_entry *he = ht->buckets[i];
			while(he)
			{
				hash_entry *hn = he->next;
				if(!(ht->flags & HASH_NCOPY) && he->data_size > 0) free(he->data);
				else if(ht->hash_free) ht->hash_free(he->data);
				free(he->key); free(he); he = hn;
			}
		}
	}

	free(ht->buckets);
	pthread_mutex_destroy(&ht->lock);

	while(ht->recycle)
	{
		hash_entry *tmp  = ht->recycle->next;
		free(ht->recycle); ht->recycle = tmp;
	}

	free(ht); free(htab);

    return 0;
}

static int myhash_rehash(hash_entity *ht)
{
	hash_entry **nb = NULL;
	size_t ns, i;

	lock_hash(ht);

	ns = hash_size(ht->entries * 2 / (ht->high_mark + ht->low_mark));
	if(ns == ht->size) goto end;

	nb = calloc(ns, sizeof(*nb));
	if(!nb) goto end;

	for(i = 0; i < ht->size; i++)
	{
		hash_entry *he = ht->buckets[i];
		while(he)
		{
			hash_entry *hn = he->next;
    		int hv= (ht->hash_func(he->key, he->key_size) & (ns-1)) ;
			he->next = nb[hv];
			nb[hv] = he;
			he = hn;
		}
	}

	ht->size = ns;
	free(ht->buckets);
	ht->buckets = nb;

	end:
	unlock_hash(ht);
	return 0;
}

static void ** myhash_keys(HTAB htab, int *n, int fast)
{
	void **keys;
	size_t i, j;

	hash_entity *ht = (hash_entity *)htab->entity;

	if(ht->entries == 0)
	{
		*n = 0;
		return NULL;
	}

	lock_hash(ht);

	keys = malloc(ht->entries * sizeof(*keys));

	for(i = 0, j = 0; i < ht->size; i++)
	{
		hash_entry *he;
		for(he = ht->buckets[i]; he; he = he->next)
		keys[j++] = fast? he->key: memcpy(malloc(he->key_size),he->key, he->key_size);
	}

	*n = ht->entries;

	unlock_hash(ht);

	return keys;
}

static int myhash_setthresholds(HTAB htab, float low, float high)
{
	hash_entity *ht = (hash_entity *)htab->entity;

    float h = high < 0? ht->high_mark: high;
    float l = low < 0? ht->low_mark: low;

    if(h < l) return -1;

    ht->high_mark = h;
    ht->low_mark = l;

    return 0;
}

static int myhash_getflags(HTAB htab)
{
	hash_entity *ht = (hash_entity *)htab->entity;
	return ht->flags;
}

static int myhash_setflags(HTAB htab, int flags)
{
	hash_entity *ht = (hash_entity *)htab->entity;

	lock_hash(ht);
	ht->flags = flags;
	unlock_hash(ht);
	return ht->flags;
}

static int myhash_setflag(HTAB htab, int flag)
{
	hash_entity *ht = (hash_entity *)htab->entity;

	lock_hash(ht);
	ht->flags |= flag;
	unlock_hash(ht);
	return ht->flags;
}

static int myhash_clearflag(HTAB htab, int flag)
{
	hash_entity *ht = (hash_entity *)htab->entity;

	lock_hash(ht);
	ht->flags &= ~flag;
	unlock_hash(ht);
	return ht->flags;
}

static void myhash_profile(HTAB htab,FILE *out)
{
	int max,min, total, i, nodes, used;
	double avg;
	hash_entry* curhash;
	hash_entry** bucket ;
	int 	*ss ;

	if(!out) return ;

	hash_entity *ht = (hash_entity *)htab->entity;

	bucket = (hash_entry** )ht->buckets ;

	total = i = nodes = used = 0;
	max = 0;
	min = 0x7fffffff ; 
	avg = 0;

	ss = (int* )malloc(sizeof(int)) ;

	for (i = 0; i < ht->size; i++)
	{
		if (bucket[i] != NULL)
		{
			used++;
			nodes = 0;
			curhash = bucket[i];
			while( curhash )
			{
				nodes++;
				curhash = curhash->next; 
			}
			total += nodes; 
			if (nodes < min ) 
			{
				min = nodes ;
			}
			if (nodes > max)
			{
				ss = (int* )realloc(ss,nodes*sizeof(int)) ;
				//bzero( (void *)(ss+max),(size_t)(nodes-max)*sizeof(int) ) ;
				memset( (void *)(ss+max),0,(size_t)(nodes-max)*sizeof(int) ) ;
				max = nodes;
			}
			ss[nodes-1]++ ;

		}
	}
	avg = (double)(total) / (double)(used);
	fprintf(out, "Total elements : %d\n", total);
	fprintf(out, "Total buckets : %d\n", ht->size);
	fprintf(out, "Used buckets : %d\n", used);
	fprintf(out, "Unused buckets : %d\n",ht->size - used) ;
	fprintf(out, "Max elements of bucket : %d\n", max);
	fprintf(out, "Min elements of bucket : %d\n", min);
	fprintf(out, "Average elements in bucket: %lf\n", avg);
	for(i=0;i<max;i++)
	{
		fprintf(out,"\nAmount of buckets with %2d elements : %d",i+1, ss[i]) ;
	}
	free(ss) ;
}
