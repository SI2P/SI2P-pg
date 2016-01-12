#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "PQueue.h"
#include <regex.h>
#include "catalog/pg_type.h"
#define MISS -2147483647

#ifndef min
#define min(a,b) (((a) < (b)) ? (a) : (b))
#endif

PG_MODULE_MAGIC;

/*
 * pre declaration, descriptions are displayed below
 */


int dominates(const int x, const int y, int type);
int dataset_score_compare(const void *a, const void *b);
int dataset_pq_compare(const void *a, const void *b);

int dataset_dom_compare(const void *a, const void *b);
char * parseCommand(const char * cmd);

Datum native_tkd(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(native_tkd); // version 1 function set to postgresql server

typedef struct DATASET{
	int *missing;	//value 1 represents for missing data, 0 otherwise
	int *value;		//value of data
	int score;
	int index;
}Dataset;

int N,D,K; // number of objects, number of dimentions, top K as in query
int cmpDim; 
int *candidateset; // candidate set

int *incomparable,incomparablenumber; // set of incomparable values, number of incomparable values, respectively
int *dominating_type;
int * cmp_type;
Dataset *dataset; // date set of all objects
Dataset *** arr;
int * arr_index;
Dataset ** accessQ;
size_t * miss_count;
size_t * no_miss_count;
char ** colum_names;
int colum_count;
/*
 *check domination
 *type = 0 for min, otherwise max is preferred
 *

 */
int dominates(const int x, const int y, int type){
	if( x == y){
		return 0;
	}
	else{
		if(0 == type){
			return x<y?1:-1;
		}
		else{
			return x>y?1:-1;
		}
	}
    
}


int dataset_score_compare(const void *a, const void *b) {
	Dataset * d1 = *(Dataset **) a;
   	Dataset * d2 = *(Dataset **) b;
   	int score1 = ((Dataset*)d1)->score;
   	int score2 = ((Dataset*)d2)->score;
    if (score1 == score2){
    	return 0;

    }
    else return score1>score2? -1:1;
}

int dataset_pq_compare(const void *a, const void *b) {
    Dataset * d1 = (Dataset*) a;
   	Dataset * d2 = (Dataset *) b;
   	int score1 = ((Dataset*)d1)->score;
   	int score2 = ((Dataset*)d2)->score;
    if (score1 == score2){
    	return 0;
    }
    else return score1>score2? -1:1;
}

int dataset_dom_compare (const void * a, const void * b)
{
   Dataset * d1 = *(Dataset **) a;
   Dataset * d2 = *(Dataset **) b;
  
    return -dominates(d1->value[cmpDim], d2->value[cmpDim], cmp_type[cmpDim]); 
}



char * parseCommand(const char * cmd){
    int cmd_len = strlen(cmd);
    char * new_cmd = (char *)palloc((cmd_len + 1) * sizeof(char));
    unsigned int new_cmd_index = 0;
    unsigned int current_index = 0;
    char * regexString = "([a-zA-Z_0-9]+)([ \t]+(max|min))[ \t]*";
    size_t maxMatches = 100;
    colum_names = palloc(100*sizeof(char *));
    size_t maxGroups = 5;
    dominating_type = palloc(sizeof(int) * 100);
    
    regex_t regexCompiled;
    regmatch_t groupArray[maxGroups];
    if (regcomp(&regexCompiled, regexString, REG_EXTENDED))
    {
        printf("Could not compile regular expression.\n");
        return NULL;
    };
    
    unsigned int m = 0;
    char * cursor = cmd;
    for (m = 0; m < maxMatches; m ++)
    {
        if (regexec(&regexCompiled, cursor, maxGroups, groupArray, 0))
            break;  // No more matches
        
        //unsigned int offset = groupArray[0].rm_eo;
        unsigned int start = groupArray[2].rm_so;
        unsigned int end = groupArray[2].rm_eo;
        unsigned int colum_len = groupArray[1].rm_eo - groupArray[1].rm_so;
        colum_names[m] = palloc(colum_len + 1);
        memcpy(colum_names[m], &cursor[groupArray[1].rm_so], colum_len);
        colum_names[m][colum_len] = '\0';
        //elog(INFO, "%s\n", colum_names[m]);
        //elog(INFO, "%c\n", cursor[groupArray[3].rm_so+1]);
        dominating_type[m] = tolower(cursor[groupArray[3].rm_so+1])=='i'? 0:1;

        for(int i=0; i < start; i++) {
            new_cmd[new_cmd_index++] = cmd[current_index++];
        }
        current_index += end - start;
        
        cursor += end;
    }
    colum_count = m;
    if(colum_count < 1){
    	elog(ERROR, "Please specify preference over column values(max or min)");
    }
    
    regfree(&regexCompiled);
    while (current_index < cmd_len) {
        new_cmd[new_cmd_index++] = cmd[current_index++];
    }
    new_cmd[new_cmd_index] = '\0';
    elog(INFO, "new command: %s", new_cmd);
    return new_cmd;
}





Datum native_tkd(PG_FUNCTION_ARGS){
	char *command;
	int i, j;
	int ret, curdm; // return value, current dimetion
	int call_cntr;
	int max_calls;
	int *retarr;

	FuncCallContext *funcctx;	// context switch variable
	AttInMetadata *attinmeta;	// not known------------------------
	TupleDesc tupdesc;			// tuple descriptor, for getting data


	/*
	 * this section will be executed only for the first call of function
	 * connect to postgres server and execute the first command and get data
	 */
	if(SRF_IS_FIRSTCALL()){
		MemoryContext oldcontext;
	
		/* 
		 * create a function context for cross-call persistence 
		 */
		funcctx = SRF_FIRSTCALL_INIT();

		/* 
		 * switch to memory context appropriate for multiple function calls 
		 * */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* 
		 * get arguments, convert given text object to a C string that can be used by server 
		 * */
		command = text_to_cstring(PG_GETARG_TEXT_P(0));

		/*
		 * arguments error
		 * */
		if(!PG_ARGISNULL(1))
			K = PG_GETARG_INT32(1);
		else
			K = 1;
		// if(!PG_ARGISNULL(2))
  //           dominating_type = PG_GETARG_INT32(2);
  //       else
  //           dominating_type = 0;
		elog(INFO, "%s", command);
        char * new_cmd = parseCommand(command);
		SPI_connect(); // open internal connection to database

		ret = SPI_exec(new_cmd, 0); // run the SQL command, 0 for no limit of returned row number
		
		N = SPI_processed; // save the number of rows
		
		dataset = (Dataset *)palloc0(sizeof(Dataset)*(N+2));
		accessQ = (Dataset **) palloc(sizeof(Dataset*)  * N);
		candidateset = (int *)palloc0(sizeof(Dataset *)*K);


		if(dataset == NULL){
			exit(1);
		}
		
		PQueue * q = priq_new(K+1, dataset_pq_compare);

		/*
		 * some rows are fetched
		 * */
		if(ret > 0 && SPI_tuptable != NULL){
			TupleDesc tupdesc;
			SPITupleTable *tuptable;
			HeapTuple tuple;
			char *type_name;
			

			/*
			 * get tuple description
			 * */
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			cmp_type = (int *) palloc(sizeof(int) * 100);
			D = 0;
			/* 
			 * for each colum, check type
			 * */
			for(i = 1; i <= tupdesc->natts; ++i){ 
				type_name = SPI_gettype(tupdesc, i);// get type of data
				char *col_name = SPI_fname(tupdesc, i);
				elog(INFO,"%s", col_name);


				if(strcmp(type_name,"int4") == 0 || strcmp(type_name,"int2") ==0){//add float4 or flat8 types if want (2)
					for(int k=0; k<colum_count; k++){
						if(strcmp(col_name, colum_names[k]) == 0){
							elog(INFO, "selected col: %s", col_name);
							cmp_type[D] = dominating_type[k];
							++D;
							break;
						}
					}
					
				}
				pfree(col_name);
			}
			elog(INFO, "D:%d", D);
			miss_count = (size_t *)palloc0(sizeof(size_t) * D );

			no_miss_count = (size_t *)palloc0(sizeof(size_t) * D );

			arr = (Dataset ***) palloc0(sizeof(Dataset**) * D);
			arr_index = (int *) palloc0(sizeof(int) * D);
			if (NULL == miss_count || NULL == arr)
			{
				/* code */
				elog(ERROR, "miss_count or arr assign error");
				exit(1);
			}
			for(int i=0; i<D; i++){
				arr[i] = (Dataset **) palloc(sizeof(Dataset *) * N);
				if (NULL == arr[i])
				{
					/* code */
					elog(ERROR, "arr  %d assign error", i);
					exit(1);
				}
			}
			/* 
			 * for each tuple
			 * and palloc all memory needed
			 * */

			 elog(INFO, "start get dataset");
			for(i = 0; i < N; ++i){
				accessQ[i] = &dataset[i];
				dataset[i].missing = (int *)palloc(sizeof(int)*(D+2));
				dataset[i].value = (int *)palloc(sizeof(int)*(D+2));
				dataset[i].index = i;

				if(dataset[i].missing == NULL || dataset[i].value == NULL){
					exit(1);
				}
				curdm = 0;

				tuple = tuptable->vals[i]; // get the ith tuple

				/* 
				 * for each dimention of a tuple 
				 * */
				for(j = 1; j <= tupdesc->natts; ++j) {
					type_name = SPI_gettype(tupdesc, j);
					char *col_name = SPI_fname(tupdesc, j);

					if(strcmp(type_name,"int4") == 0 || strcmp(type_name,"int2") == 0 ){

						for(int k=0; k<colum_count; k++){
							if(strcmp(col_name, colum_names[k]) == 0){

								if(SPI_getvalue(tuple, tupdesc, j) == NULL) { // value is missing
									dataset[i].missing[curdm] = 1;
									dataset[i].value[curdm] = MISS; 
									miss_count[curdm]++;
								}
								else{ // value is not missing
									dataset[i].missing[curdm] = 0;
									dataset[i].value[curdm] = atof(SPI_getvalue(tuple, tupdesc, j));
									dataset[i].score = 0;
									arr[curdm][arr_index[curdm]++] = &dataset[i];
									no_miss_count[curdm] ++;
								}
									dataset[i].score = N -1;

								++curdm;
								break;
							}
						}
						
					}
		
				}
				
			}
			elog(INFO, "start sort values");
			for(int i=0; i<D; i++){
				cmpDim = i;
				qsort(&arr[i][0], no_miss_count[i], sizeof(Dataset *), dataset_dom_compare);
			}
			elog(INFO, "start cal max score");
			for (int i = 0; i < D; ++i)
			{
				int last_pos = 0;
				int total = N - miss_count[i]-1;
				//arr[i][k]->score= min(miss_count[i], arr[i][k]->score);
				for(int k = 0; k < total; ++k){
					if(arr[i][k+1]->value[i] != arr[i][k]->value[i]){
						last_pos = k+1;
					}
					arr[i][k+1]->score = min(N - last_pos-1, arr[i][k+1]->score);
				}
			}
			elog(INFO, "debug: %d %d" , dataset[536].index, dataset[536].score);
			elog(INFO, "start sort max score");

			qsort(&accessQ[0], N, sizeof(Dataset *), dataset_score_compare);
			elog(INFO, "finish sort max score");
			//elog(INFO, "N: %d", N);

			for(i = 0; i < N; ++i){
				Dataset * d = accessQ[i];

				//elog(INFO, "id: %d, max score:%d\n", d->index, d->score);
				if(priq_size(q) >= K && d->score < ((Dataset *)priq_top(q))->score){
					break;
				}
				d->score = 0;
				for(int j = 0; j< N; ++j){
					int dom_count = 0;
					int dominated_count =0;
					for(int k=0; k<D; ++k){
						if( !d->missing[k] && !dataset[j].missing[k]){
							int result = dominates(d->value[k], dataset[j].value[k], dominating_type[k]);
							if(1 == result){
								dom_count++;
							}
							else if(-1 == result){
								dominated_count++;
							}
						}
					}
					if(dom_count >0 && 0 == dominated_count){
						d->score++;
					}
					
				}
				//elog(INFO, "id: %d, score:%d\n", d->index, d->score);
				if(priq_size(q) < K){
					priq_push(q, d);
					//elog(INFO, "push %d: %d,----top: %d", d->index, d->score, ((Dataset *)priq_top(q))->score);

				}
				else{
					Dataset * top =  (Dataset *)priq_top(q);
					//elog(INFO, "push %d: %d, top %d:%d", d->index, d->score, top->index, top->score);

					if(d->score > top->score){
						//elog(INFO, "push");

						priq_pop(q);
						priq_push(q, d);
					}

				}
			}
				


		}
		pfree(command);
		K = min(K, priq_size(q) );
		funcctx->max_calls = K;
	
		/*
		 * allocate local variable retstruct and store the result tuple init
		 * */
		retarr = (int *)palloc(sizeof(int)*(K+2));
		if(retarr == NULL){
			exit(1);
		}
		while(priq_size(q) > K){
			priq_pop(q);
		}
		for(i = K-1; i >=0 && priq_size(q) >=0; --i ){
			retarr[i] = ((Dataset *) priq_pop(q))->index;
			//elog(INFO, "score: %d, index: %d ", dataset[retarr[i]].score, dataset[retarr[i]].index);
		}
		funcctx->user_fctx = retarr;

		if(get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function returning record called in context that cannot accept type record")));
		
		/* Generate attribute metadata needed later to produce tuples from raw C strings */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        /* MemoryContext switch to old context */
        MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;
	retarr = funcctx->user_fctx;
	
	if(call_cntr < max_calls){
		char **values;
		HeapTuple tuple;
		HeapTuple ret_tuple;
		Datum result;
		TupleDesc tupdesc;
		SPITupleTable *tuptable;

		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;

		TupleDesc  new_desc = CreateTemplateTupleDesc(tupdesc->natts +1, false);
		for(i = 1; i <= tupdesc->natts; ++i ){
			TupleDescCopyEntry(new_desc, i, tupdesc, i);
		}
		TupleDescInitEntry(new_desc,tupdesc->natts +1 , "Score", INT4OID, -1, 0);

		
		/*
         * Prepare a values array for building the returned tuple.
         * This should be an array of C strings which will
         * be processed later by the type input functions.
         */
		values = (char **)palloc((tupdesc->natts+1) * sizeof(char *));
		if(values == NULL){
			exit(1);
		}

		for(i = 0; i < tupdesc->natts; ++i ){
			tuple = tuptable->vals[retarr[call_cntr]];
			values[i] = (SPI_getvalue(tuple, tupdesc, i+1));
		}
		values[tupdesc->natts] = (char *) palloc(10 * sizeof(char));
		snprintf(values[tupdesc->natts] , 10, "%d",  dataset[retarr[call_cntr]].score);

        ret_tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(new_desc), values); // build a return tuple 
		
        result = HeapTupleGetDatum(ret_tuple); // make the tuple into a datum
		
		SRF_RETURN_NEXT(funcctx,result);
	}
	else{
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

