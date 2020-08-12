#include<string.h>
#include<stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <errno.h>
#include <stdint.h>
#include <ctype.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h> 
#include <sys/time.h>   
#include <sys/resource.h> 

#define MAX_INPUT_SIZE 1024
#define MAX_TOKEN_SIZE 64
#define MAX_NUM_TOKENS 64

int N;
pthread_t *worker_threads;
long *requests_completed;
double *sum_response_time;
char server_ip[100] = "0.0.0.0";
char server_port[100] = "8000";
char test_val[100] = "test";
int test_valSize = 4;

typedef struct _msg {
	char type;
	int32_t key;
	int32_t valueSize;
	char *value;
} msg;

void perr(char *helper){
	printf("%s (%s)\n",helper, strerror(errno));
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

void alarm_handler(int sig){
	int r;
	for(int i=0; i < N; i++){
    		r = pthread_cancel(worker_threads[i]);
		if (r != 0) printf("Error cancelling threads (%s)\n",strerror(errno));
  	}
}

int open_conn(char *ip,char *port){
	int sockfd, portno, n;

    	struct sockaddr_in serv_addr;
	
   	portno = atoi(port);
    	sockfd = socket(AF_INET, SOCK_STREAM, 0);
    	if (sockfd < 0){ 
        	printf("ERROR opening socket (%s)\n",strerror(errno));
		return -1;
	}

    	bzero((char *) &serv_addr, sizeof(serv_addr));
    	serv_addr.sin_family = AF_INET;
    	inet_pton(AF_INET, ip, &(serv_addr.sin_addr));
    	serv_addr.sin_port = htons(portno);
    	if (connect(sockfd,(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0){
        	printf("ERROR connecting (%s)\n",strerror(errno));
		close(sockfd);
		return -1;
	}
	return sockfd;
}

int write_msg(int connfd, msg m){
	//print_msg(m);
	int r;
	r = rio_writen(connfd,&m.type,1);
	char err_help[] = "Error connecting to server\n";
	if( r < 0){
		perr(err_help);
		return r;
	}
	r = rio_writen(connfd,&m.key,sizeof(m.key));
	if( r < 0){
		perr(err_help);
		return r;
	}
	if(m.value != NULL){
		r = rio_writen(connfd,&m.valueSize,sizeof(m.valueSize));
		if(r<0){
			perr(err_help);
			return r;
		}
		r = rio_writen(connfd,m.value,ntohl(m.valueSize));
		if(r<0){
			perr(err_help);
			return r;
		}
	}
	return 1;
}

msg createMsg(char type,int key,int valSize,char *value){
	msg m;
	m.type = type;
	m.key = key;
	m.key = htonl(m.key);
	m.valueSize = valSize;
	m.valueSize = htonl(m.valueSize);
	m.value = value;
	return m;
}

int try_open(){
	int connfd;
	while((connfd = open_conn(server_ip, server_port)) < 0){
		printf("Error opening connection\n");
	}
	return connfd;
}

void *worker(void *data){
	char tmp[100];
	int id = (int) data;
	int r = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	msg m, resp;
	resp.value = tmp;
	if (r!=0){
		printf("Error making thread cancellable\n");
		return -1;
	}
	int connfd = try_open();
	int mtype = 0, key;
	struct timespec start, finish; 
	while(1){
		key = rand() % 20000;
		switch(mtype){
		case 0:
			m = createMsg('c', key, test_valSize, test_val);
			break;
		case 1:
			m = createMsg('r', key, 0, NULL);
			break;
		case 2:
			m = createMsg('u', key, test_valSize, test_val);
			break;
		case 3:
			m = createMsg('d', key, 0, NULL);
			break;
		}
		int r = write_msg(connfd, m);
		clock_gettime(CLOCK_REALTIME, &start); 

		if(r <= 0){
			close(connfd);
			try_open();
		}
		else {
			int ok = 0;
			r = rio_readn(connfd, &resp.type, 1);
			r = rio_readn(connfd, &resp.valueSize, sizeof(resp.valueSize));
			int32_t vSize = ntohl(resp.valueSize);
			if(r==sizeof(resp.valueSize)){
				r = rio_readn(connfd, resp.value, vSize);
				if(r==vSize){
					resp.value[vSize] = '\0';
					if(resp.type == 'e' || resp.type == 'k'){
						ok = 1;
					}
					else {
						printf("Server response not recognized\n");
					}
				}
			}
			if(!ok){
				printf("Error in getting response from server\n");
				close(connfd);
				connfd = try_open();
			}
			else {
				requests_completed[id]++;
				clock_gettime(CLOCK_REALTIME, &finish); 
				long seconds = finish.tv_sec - start.tv_sec; 
    				long ns = finish.tv_nsec - start.tv_nsec; 
    
    				if (start.tv_nsec > finish.tv_nsec) { // clock underflow 
					--seconds; 
					ns += 1000000000; 
    				} 
    		 		sum_response_time[id] += (double)seconds + (double)ns/(double)1000000000; 
			}
				
		}
		
			
		mtype = (mtype + 1) % 4;
	}
}
		

int main(int argc, char **argv){
	int duration;
	N = atoi(argv[1]);
	duration = atoi(argv[2]);
	worker_threads = malloc(sizeof(pthread_t) * N);
	requests_completed = malloc(sizeof(long) * N);
	sum_response_time = malloc(sizeof(long) * N);
	signal(SIGALRM,alarm_handler);

	for(int i = 0; i < N; i++){
      		pthread_create(&worker_threads[i], NULL, worker, (void *) i);
  	}

	alarm(duration);
	
	long total_reqs = 0;
	double total_resp_time = 0;
	for(int i = 0; i < N; i++){
      		pthread_join(worker_threads[i], NULL);
		total_reqs += requests_completed[i];
		total_resp_time += sum_response_time[i];
  	}
	
	printf("throughput = %lf requests/sec\n", ((double)total_reqs)/duration);
	printf("avg response time = %lf secs\n", total_resp_time/total_reqs);
	

}
