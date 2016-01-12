//
//  PQueue.c
//  Test
//
//  Created by Huiyong on 15/11/28.
//  Copyright © 2015年 Huiyong. All rights reserved.
//

#include <stdio.h>
#include "PQueue.h"
/* first element in array not used to simplify indices */
PQueue * priq_new(int size, int (*cmp)(const void *d1, const void *d2))
{
    if (size < 4) size = 4;
    
    PQueue * q = (PQueue *)malloc(sizeof(PQueue));
    q->buf = (void **)malloc(sizeof(*(q->buf)) * size);
    if(NULL == q || NULL == q->buf || NULL == cmp){
        return NULL;
    }
    q->cmp = cmp;
    q->alloc = size;
    q->n = 1;
    
    return q;
}

int priq_push(PQueue * q, void *data)
{
    void **b;
    int n, m;
    if(NULL == q){
        return 1;
    }
    if (q->n >= q->alloc) {
        q->alloc *= 2;
        b = (void **)realloc(q->buf, sizeof(q->buf[0]) * q->alloc);
        if(NULL == b){
            return 1;
        }
        q->buf = b;
    } else
        b = q->buf;
    
    n = q->n++;
    /* append at end, then up heap */
    while ((m = n / 2) && q->cmp(data, b[m]) > 0) {
        b[n] = b[m];
        n = m;
    }
    b[n]= data;
    return 0;
}

/* remove top item. returns 0 if empty.*/
void * priq_pop(PQueue * q)
{
    if(NULL == q){
        return 0;
    }
    
    if (q->n == 1) return 0;
    
    void **b = q->buf;
    
    void *out= q->buf[1];
    
    
    /* pull last item to top, then down heap. */
    --(q->n);
    
    int n = 1, m;
    while ((m = n * 2) < q->n) {
        if (m + 1 < q->n &&   q->cmp(b[m], b[m + 1]) < 0) m++;
        
        if (q->cmp (b[q->n], b[m]) > 0)
            break;
        b[n] = b[m];
        n = m;
    }
    
    b[n] = b[q->n];
    if (q->n < q->alloc / 2 && q->n >= 16)
        q->buf = (void **)realloc(q->buf, (q->alloc /= 2) * sizeof(b[0]));
    
    return out;
}

/* get the top element without removing it from queue */
void* priq_top(PQueue * q)
{
    if (q->n == 1) return 0;
    
    return q->buf[1];
}