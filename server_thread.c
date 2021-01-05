#include "request.h"
#include "server_thread.h"
#include "common.h"
#include <stdbool.h>
#include <malloc.h>

struct block {
    struct file_data data;
    bool pinned;
    bool used;
    pthread_mutex_t lock;
    struct block * next;
};

struct hashTable {
    struct block * myhash;
    int totalSpace_left;
    bool filled;
};
struct hashTable filehash;
int Tsize = 1;

struct server {
    int nr_threads;
    int max_requests;
    int max_cache_size;
    int exiting;


    pthread_mutex_t lock;
    pthread_cond_t empty;
    pthread_cond_t full;

    pthread_t * thread;

    int *buffer;
    int in_rq;
    int out_rq;
    /* add any other parameters you need */
};

/* static functions */
unsigned long hash(char *str);
void recieve(void * mem);
bool cache_evict(int amount_to_evict);
bool cache_insert(struct file_data*);
struct block * cache_lookup(char *); /*return the hash key for the spot*/
void copy_from_db(struct block*, struct request*); /*copy contents from block->file_data to request*/

/* initialize file data */
static struct file_data *
file_data_init(void) {
    struct file_data *data;

    data = Malloc(sizeof (struct file_data));
    data->file_name = NULL;
    data->file_buf = NULL;
    data->file_size = 0;
    return data;
}

/*the stub function that the thread will */
void thread_stub(void (*thread_main)(void *), void *arg) {
    thread_main(arg);
}

/* free all file data */
static void
file_data_free(struct file_data *data) {
    //    free(data->file_name);
    //    free(data->file_buf);
    //    free(data);
}

static void
do_server_request(struct server *sv, int connfd) {
    int ret;
    struct request *rq;
    struct file_data *data;

    data = file_data_init();

    /* fill data->file_name with name of the file being requested */
    rq = request_init(connfd, data);
    if (!rq) {
        file_data_free(data);
        return;
    }

    /*Lab 5: before making a disk read check if the file is already stored in
     the cache
     if is is just load from the cache
     else if the thread can proceed */

    struct block * _ret = cache_lookup(data->file_name);

    if (_ret != NULL) {
        //_ret != NULL
        //        printf("here\n");
        copy_from_db(_ret, rq);
        goto send_now;
    }


    /* read file, 
     * fills data->file_buf with the file contents,
     * data->file_size with file size. */
    ret = request_readfile(rq);
    if (ret == 0) { /* couldn't read file */
        goto out;
    }

    /*Lab 5: Goes here*/
    //after the file has been read 
    //check if i can add it to the cache 
    //if i can add it to the cache

    //fails if the file is to big for the cache or
    // if the cache cannot give up enough memory to make this happen

    cache_insert(data);
send_now:

    /* send file to client */
    request_sendfile(rq);



out:
    request_destroy(rq);
    file_data_free(data);
}

/* entry point functions */

struct server *
server_init(int nr_threads, int max_requests, int max_cache_size) {
    struct server *sv;

    sv = Malloc(sizeof (struct server));
    sv->nr_threads = nr_threads;
    sv->max_requests = max_requests + 1;
    sv->max_cache_size = max_cache_size;
    sv->exiting = 0;
    pthread_mutex_init(&sv->lock, NULL);
    pthread_cond_init(&sv->empty, NULL);
    pthread_cond_init(&sv->full, NULL);

    /* Lab 4: create queue of max_request size when max_requests > 0 */
    if (max_requests > 0) {
        sv->buffer = malloc((max_requests + 1) * sizeof (int));
        sv->in_rq = 0;
        sv->out_rq = 0;
    }

    if (max_cache_size != 0) {
        Tsize = (2*max_cache_size)+1;
        /* Lab 5: init server cache and limit its size to max_cache_size */
        filehash.filled = false;
        filehash.totalSpace_left = max_cache_size;
        filehash.myhash = malloc(sizeof (struct block)*Tsize);
    } else {
        filehash.filled = true;
        filehash.totalSpace_left = 0;
        filehash.myhash = NULL;
    }


    /* Lab 4: create worker threads when nr_threads > 0 */
    if (nr_threads > 0) {
        sv->thread = malloc(nr_threads * sizeof (pthread_t));

        for (int i = 0; i < nr_threads; i++) {
            pthread_create(&sv->thread[i], NULL, (void *(*)(void *))recieve, sv);
        }
    }
    return sv;
}

void
server_request(struct server *sv, int connfd) {
    if (sv->nr_threads == 0) { /* no worker threads */
        do_server_request(sv, connfd);
    } else {
        /*  Save the relevant info in a buffer and have one of the
         *  worker threads do the work. */

        pthread_mutex_lock(&sv->lock);
        //        printf("got lock in send\n");
        while ((sv->in_rq - sv->out_rq + sv->max_requests) % sv->max_requests == sv->max_requests - 1) {
            //            printf("fuck\n");
            pthread_cond_wait(&sv->full, &sv->lock);

            //            if (sv->exiting) {
            //                pthread_mutex_unlock(&sv->lock);
            //                return;
            //            }

        }

        sv->buffer[sv->in_rq] = connfd;

        if (sv->in_rq == sv->out_rq) {
            //            printf("signal send\n");
            pthread_cond_broadcast(&sv->empty);
        }
        sv->in_rq = (sv->in_rq + 1) % sv->max_requests;

        pthread_mutex_unlock(&sv->lock);
    }
}


//write this at the end 

void
server_exit(struct server *sv) {
    /* when using one or more worker threads, use sv->exiting to indicate to
     * these threads that the server is exiting. make sure to call
     * pthread_join in this function so that the main server thread waits
     * for all the worker threads to exit before exiting. */


    pthread_mutex_lock(&sv->lock);
    sv->exiting = 1;
    pthread_cond_broadcast(&sv->empty);
    pthread_cond_broadcast(&sv->full);

    pthread_mutex_unlock(&sv->lock);

    for (int i = 0; i < sv->nr_threads; i++) {
        pthread_join(sv->thread[i], NULL);
    }

    /* make sure to free any allocated resources */
    if (sv->buffer != NULL)
        free(sv->buffer);
    sv->buffer = NULL;
    if (sv->thread != NULL)
        free(sv->thread);
    sv->thread = NULL;

    free(sv);
    sv = NULL;
}

void recieve(void * mem) {

    struct server * sv = (struct server*) mem;

    while (true) {
        //        printf("here\n");
        pthread_mutex_lock(&sv->lock);

        //        printf("currnum:%d\n",sv->current_number_of_requests);

        while (sv->in_rq == sv->out_rq && !sv->exiting) {
            //            printf("fuck\n");
            pthread_cond_wait(&sv->empty, &sv->lock);



        }

        if (sv->exiting) {
            pthread_mutex_unlock(&sv->lock);
            return;
        }

        int connfd = sv->buffer[sv->out_rq];
        //        printf("connfd:%d\n",connfd);


        if ((sv->in_rq - sv->out_rq + sv->max_requests) % sv->max_requests == sv->max_requests - 1)
            pthread_cond_signal(&sv->full);

        sv->out_rq = (sv->out_rq + 1) % sv->max_requests;

        pthread_mutex_unlock(&sv->lock);
        do_server_request(sv, connfd);

    }
}

/*
 * cache_lookup(file)
 * cache_insert(file)
 * cache_insert(amount_to_evict)
 * copy_from_db
 */



void copy_from_db(struct block* from, struct request* to) {
    pthread_mutex_lock(&from->lock);
    from->pinned = true; /*this will be check before modifying any data*/
    request_set_data(to, &from->data);
    from->used = true;

    pthread_mutex_unlock(&from->lock);
}

struct block * cache_lookup(char* name) {
    /*channing*/
    if (filehash.myhash == NULL)
        return (NULL);

    char * temp = malloc(sizeof (name) + 1);
    strcpy(temp, name);
    unsigned long key = hash(temp);
    pthread_mutex_lock(&(filehash.myhash[key]).lock);

    if ((filehash.myhash[key]).next == NULL) {
        char * temp = (filehash.myhash[key]).data.file_name;

        if (temp == NULL) {
            pthread_mutex_unlock(&(filehash.myhash[key]).lock);
            return (NULL);
        }

        if (strcmp(temp, name) == 0) {
            pthread_mutex_unlock(&(filehash.myhash[key]).lock);
            return (&filehash.myhash[key]);
        }

    } else {
        pthread_mutex_lock(&(filehash.myhash[key]).lock);
        struct block * tp = filehash.myhash[key].next;

        while (tp->next != NULL) {

            if (strcmp(tp->data.file_name, name) == 0) {
                pthread_mutex_unlock(&(filehash.myhash[key]).lock);
                return (&filehash.myhash[key]);
            }
            tp = tp->next;
        }

    }
    pthread_mutex_unlock(&(filehash.myhash[key]).lock);
    return (NULL);
}

bool cache_evict(int amount_to_evict) {
    return (false);
}

bool cache_insert(struct file_data* data) {

    if (filehash.myhash == NULL)
        return (NULL);
    if (filehash.filled)
        return (false);
    if (filehash.totalSpace_left < data->file_size)
        return (false);

    char * temp = malloc(sizeof (data->file_name) + 1);
    strcpy(temp, data->file_name);
    unsigned long key = hash(temp);

    if ((filehash.myhash[key]).used) {

        pthread_mutex_lock(&(filehash.myhash[key]).lock);

        if (strcmp((filehash.myhash[key]).data.file_name, data->file_name) == 0) {
            pthread_mutex_unlock(&(filehash.myhash[key]).lock);
            return (false);
        }

        struct block * tp = (filehash.myhash[key]).next;

        while (tp->next != NULL) {

            if (strcmp((tp->data.file_name), data->file_name) == 0) {
                pthread_mutex_unlock(&(filehash.myhash[key]).lock);
                return (false);
            }

            tp = tp->next;
        }

        tp->next = (struct block *) malloc(sizeof (struct block));

        tp = tp->next;

        tp->data.file_name = data->file_name;
        tp->data.file_buf = data->file_buf;
        tp->data.file_size = data->file_size;

        tp->used = true;
        tp->next = NULL;
        tp->pinned = false;

        filehash.totalSpace_left -= data->file_size;

        pthread_mutex_unlock(&(filehash.myhash[key]).lock);

    } else {
        filehash.totalSpace_left -= data->file_size;
        pthread_mutex_lock(&(filehash.myhash[key]).lock);

        (filehash.myhash[key]).used = true;
        (filehash.myhash[key]).next = NULL;

        (filehash.myhash[key]).data.file_name = data->file_name;
        (filehash.myhash[key]).data.file_buf = data->file_buf;
        (filehash.myhash[key]).data.file_size = data->file_size;

        pthread_mutex_unlock(&(filehash.myhash[key]).lock);

    }

    if (filehash.totalSpace_left == 0)
        filehash.filled = true;

    return (true);
}

/*
 * hash functions
 */
unsigned long hash(char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash % Tsize;
}