//
//  PQueue.h
//  Test
//
//  Created by Huiyong on 15/11/28.
//  Copyright © 2015年 Huiyong. All rights reserved.
//

#ifndef PQueue_h
#define PQueue_h

typedef struct {
    void  **buf;
    int n, alloc;
    int (*cmp)(const void *d1, const void *d2);
} PQueue;

#define priq_purge(q) (q)->n = 1
#define priq_size(q) ((q)->n - 1)

PQueue * priq_new(int size, int (*cmp)(const void *d1, const void *d2));

int priq_push(PQueue * q, void *data);

/* remove top item. returns 0 if empty.*/
void * priq_pop(PQueue * q);


/* get the top element without removing it from queue */
void* priq_top(PQueue * q);

#endif /* PQueue_h */
