#include<string.h>
#include<stdio.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <signal.h>
#include <wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <stdint.h>

#define NUM_WORKERS 5000
#define HASH_SLOTS 20000

typedef struct _msg {
	char type;
	int32_t key;
	int32_t valueSize;
	char *value;
} msg;

void print_msg(msg m){
	printf("msg:\ntype:%c\nkey:%d\nvsz:%d\nval:",m.type,ntohl(m.key),ntohl(m.valueSize));
	if(m.value !=NULL){
		printf("%s",m.value);
	}
	else{
		printf("NULL");
	}
	printf("\n");
}
	

typedef struct _kv {
	int key;
	char *value;
	struct _kv * next;
} kv; 

typedef struct _dict {
	kv **slots;
	int n;
	sem_t mutex;
} dict;

dict kv_table;

void init_dict(dict *d,int size){
	d->slots = calloc(size, sizeof(kv *));
	d->n = size;
	sem_init(&d->mutex, 0, 1);
	for(int i=0;i<size;i++){
		d->slots[i] = NULL;
	}
}

kv *dict_get(dict *d, int k){
	int slot = k % d->n;
	kv * pair = d->slots[slot];
	while(pair!=NULL && pair->key!=k) pair = pair->next;
	return pair;
}

void dict_add(dict *d, int k, char *value){
	int slot = k % d->n;
	kv * pair = malloc(sizeof(kv));
	pair->key = k;
	pair->value = value;
	pair->next = d->slots[slot];
	d->slots[slot] = pair;
}

int dict_del(dict *d, int k){
	int ret = 0;
	int slot = k % d->n;
	kv * pair = d->slots[slot];
	kv ** prev = &d->slots[slot];
	while(pair!=NULL && pair->key!=k){ 
		prev = &pair->next;
		pair = pair->next;
	}
	if(pair!=NULL){
		*prev = pair->next;
		ret = 1;
		free(pair);
	}
	return ret;
}

int dict_create(dict *d, int k, char *value){
	int r = 0;
	sem_wait(&d->mutex);
	kv *pair = dict_get(d,k);
	if(pair != NULL){
		r = -1;
	}
	else {
		dict_add(d, k, value);
	}
	sem_post(&d->mutex);
	return r;
}

char * dict_read(dict *d, int k){
	char * ret = NULL;
	sem_wait(&d->mutex);
	kv *pair = dict_get(d,k);
	if(pair != NULL) {
		ret = malloc(sizeof(char) * (strlen(pair->value)+1));
		strcpy(ret, pair->value);
	}
	sem_post(&d->mutex);
	return ret;
}

int dict_update(dict *d, int k, char *value){
	int r = 0;
	sem_wait(&d->mutex);
	kv *pair = dict_get(d,k);
	if(pair != NULL){
		free(pair->value);
		pair->value = value;
	}
	else {
		r = -1;
	}
	sem_post(&d->mutex);
	return r;
}

int dict_delete(dict *d, int k){
	int r = 0;
	sem_wait(&d->mutex);
	if(!dict_del(d,k)) r = -1;
	sem_post(&d->mutex);
	return r;
}
	

ssize_t rio_readn(int fd, void *usrbuf, size_t n) 
{
    size_t nleft = n;
    ssize_t nread;
    char *bufp = usrbuf;

    while (nleft > 0) {
	if ((nread = read(fd, bufp, nleft)) < 0) {
	    if (errno == EINTR) /* Interrupted by sig handler return */
		nread = 0;      /* and call read() again */
	    else
		return -1;      /* errno set by read() */ 
	} 
	else if (nread == 0)
	    break;              /* EOF */
	nleft -= nread;
	bufp += nread;
    }
    return (n - nleft);         /* Return >= 0 */
}
ssize_t rio_writen(int fd, void *usrbuf, size_t n) 
{
    size_t nleft = n;
    ssize_t nwritten;
    char *bufp = usrbuf;

    while (nleft > 0) {
	if ((nwritten = write(fd, bufp, nleft)) <= 0) {
	    if (errno == EINTR)  /* Interrupted by sig handler return */
		nwritten = 0;    /* and call write() again */
	    else
		return -1;       /* errno set by write() */
	}
	nleft -= nwritten;
	bufp += nwritten;
    }
    return n;
}

typedef struct _queue {
	int *buffer;
	int n;
	int front;
	int rear;
	sem_t mutex;
	sem_t slots;
	sem_t items;
} queue;

queue conn_buf;

void init_queue(queue *q, int bufSize){
	q->buffer = malloc(sizeof(int) * bufSize);
	q->n = bufSize;
	q->front = 0;
	q->rear = 0;
	sem_init(&q->mutex, 0, 1);
	sem_init(&q->slots, 0, bufSize);
	sem_init(&q->items, 0, 0);
}
	

void add_to_queue(queue * q,int e){
	sem_wait(&q->slots);
	sem_wait(&q->mutex);
	q->buffer[q->rear] = e;
	q->rear = (q->rear + 1) % q->n;
	sem_post(&q->mutex);
	sem_post(&q->items);
}

int dequeue(queue *q){
	sem_wait(&q->items);
	sem_wait(&q->mutex);
	int e = q->buffer[q->front];
	q->front = (q->front + 1) % q->n;
	sem_post(&q->mutex);
	sem_post(&q->slots);
	return e;
} 
	

void error(char *msg)
{
    perror(msg);
    exit(1);
}

msg execute_msg(msg m){
	//print_msg(m);
	int r = 0;
	kv * pair;
	char *response = malloc(sizeof(char)*100);
	char resType;
	m.key = ntohl(m.key);
	m.valueSize = ntohl(m.valueSize);
	if(m.type == 'c'){
		r = dict_create(&kv_table, m.key, m.value);
		if(r<0){
			resType = 'e';
			strcpy(response,"Key already exists");
		}
		else {
			resType = 'k';
			strcpy(response,"OK");
		}
			
	}
	else if(m.type == 'r'){
		char * value = dict_read(&kv_table,m.key);
		if(value == NULL){
			resType = 'e';
			strcpy(response,"Key does not exist");
		}
		else {
			resType = 'k';
			free(response);
			response = value;
		}
	}
	else if(m.type == 'u'){
		r = dict_update(&kv_table, m.key, m.value);
		if(r < 0){
			resType = 'e';
			strcpy(response,"Key does not exist");
		}
		else {
			resType = 'k';
			strcpy(response,"OK");
		}
	}
	else if(m.type == 'd'){
		r = dict_delete(&kv_table, m.key);
		if(r < 0){
			resType = 'e';
			strcpy(response,"Key does not exist");
		}
		else {
			resType = 'k';
			strcpy(response,"OK");
		}
	}
	else {
		resType = 'e';
		strcpy(response,"Invalid command");
	}
	msg resp;
	resp.type = resType;
	resp.valueSize = htonl(strlen(response));
	resp.value = response;
	return resp;
}


		

void *worker(void *data) {
	int worker_id = (int) data;
	int n,r;
	char buf[256];
	msg m;
	while(1) {
		int sockfd = dequeue(&conn_buf);
		while(1){
			m.value = NULL;
			r = rio_readn(sockfd,&m.type,1);
			if(r != 1) break;
			r = rio_readn(sockfd,&m.key,sizeof(m.key));
			if(r != sizeof(m.key)) break;
			if(m.type == 'c' || m.type == 'u'){
				r = rio_readn(sockfd,&m.valueSize,sizeof(m.valueSize));
				if(r != sizeof(m.valueSize)) break;
				int32_t vSize = ntohl(m.valueSize);
				m.value = malloc(sizeof(char)*(vSize+1));
				r = rio_readn(sockfd,m.value,vSize);
				if(r != vSize) {
					free(m.value);
					break;
				}
			}
			//printf("req:\n");
			msg response = execute_msg(m);
			//printf("response\n");
			//print_msg(response);
			
			r = rio_writen(sockfd, &response.type, 1);
			if(r < 0){
				free(response.value);
				break;
			}
			r = rio_writen(sockfd, &response.valueSize, sizeof(response.valueSize));
			if(r < 0){
				free(response.value);
				break;
			}
			r = rio_writen(sockfd, response.value, ntohl(response.valueSize));
			free(response.value);
			if(r < 0) break;
		}
		close(sockfd);
      	}
}


int main(int argc, char **argv){
	//printf("Main starts\n");
	char *ip = argv[1];
	char *port = argv[2];
	int sockfd, newsockfd, portno, clilen;
     	char buffer[256];
     	struct sockaddr_in serv_addr, cli_addr;
     	int n;
	pthread_t worker_threads[NUM_WORKERS];
	int worker_id[NUM_WORKERS];
	init_queue(&conn_buf,NUM_WORKERS);
	init_dict(&kv_table,1000);
	signal(SIGPIPE,SIG_IGN);
     	if (argc < 3) {
        	fprintf(stderr,"Usage: server.c ip port");
         	exit(1);
     	}
     	sockfd = socket(AF_INET, SOCK_STREAM, 0);
     	if (sockfd < 0) 
        	error("ERROR opening socket");
     	bzero((char *) &serv_addr, sizeof(serv_addr));
     	portno = atoi(argv[2]);
     	serv_addr.sin_family = AF_INET;
     	inet_pton(AF_INET, argv[1], &(serv_addr.sin_addr));
     	serv_addr.sin_port = htons(portno);
     	if (bind(sockfd, (struct sockaddr *) &serv_addr,
              sizeof(serv_addr)) < 0) 
              error("ERROR on binding");
     	listen(sockfd,5);

	for(int i = 0; i < NUM_WORKERS; i++){
      		pthread_create(&worker_threads[i], NULL, worker, (void *) i);
  	}
	//printf("Threads created\n");
	printf("server running\n");
	while(1){
		clilen = sizeof(cli_addr);
		newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
		if (newsockfd < 0) 
          		error("ERROR on accept");
		add_to_queue(&conn_buf,newsockfd);
	}
}
