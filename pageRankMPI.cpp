// Code contains redacted code from academic course work
// to be shared for employement review

#include <iostream>
#include <cstdio>
#include <mpi.h>
#include <map>

// Globals
Graph g;
uint g_max_iterations;
int world_size;
int world_rank;

struct vertRange{
    int start = 0;
    int end = 0;
};

void pageRankMPI(){
    uintV n = g.n_;
    uintV m = g.m_;
    double time_taken;
    timer t1;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];

    t1.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Edge Decomposition
    std::map<int, vertRange> proccRanges;
    uintV start_vertex = 0;
    uintV end_vertex = 0;

    uintV temp_start_vertex = 0;
    uintV temp_end_vertex = 0;

    for (uintV i = 0; i < world_size; i++){
        temp_start_vertex = temp_end_vertex;
        long count = 0;
        while (temp_end_vertex < n){
            count += g.vertices_[temp_end_vertex].getOutDegree();
            temp_end_vertex += 1;
            if (count >= m/world_size) break;
        }

        vertRange temp;
        temp.start = temp_start_vertex;
        temp.end = temp_end_vertex;
        proccRanges[i] = temp;
        if (i == world_rank) {
            
            start_vertex = temp_start_vertex;
            end_vertex = temp_end_vertex;
        }; 
    }

    double tCommTime = 0;
    long edges_processed = 0;
    timer tComm;
    PageRankType *pr_temp_n = new PageRankType[n];
    long temp_count_self = proccRanges[world_rank].end - proccRanges[world_rank].start;
    PageRankType *pr_temp_self = new PageRankType[temp_count_self];
    long startingRangeSelf = proccRanges[world_rank].start;

    for (int iter = 0; iter < g_max_iterations; iter++){
        for (uintV u = start_vertex; u < end_vertex; u++){
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++){
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        // sync starts
        
        tComm.start();

        // ALL PROCC SEND PR_NEXT TO ROOT 
        if (world_rank != 0){

            MPI_Send(
            /* data         = */ pr_next,
            /* count        = */ n, 
            /* datatype     = */ PAGERANK_MPI_TYPE, 
            /* destination  = */ 0, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD
            );
            
        }
        else { // receive
            for (int i = 0; i < world_size; i++){ // receive from i
                if (i == world_rank) continue; // dont receive from self
                MPI_Recv(
                /* data         = */ pr_temp_n, // bug: rip had this as pr_next
                /* count        = */ n, 
                /* datatype     = */ PAGERANK_MPI_TYPE, 
                /* source       = */ i, 
                /* tag          = */ 0, 
                /* communicator = */ MPI_COMM_WORLD, 
                /* status       = */ MPI_STATUS_IGNORE);
                for (int y = 0; y < n; y++){
                    pr_next[y] += pr_temp_n[y];
                }
            }
        }

        // ALL PROCC RECEIVE UPDATED PR_NEXT verts FROM ROOT

        if (world_rank != 0){ 
            MPI_Recv(
            /* data         = */ pr_temp_self, 
            /* count        = */ temp_count_self, 
            /* datatype     = */ PAGERANK_MPI_TYPE, 
            /* source       = */ 0, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD, 
            /* status       = */ MPI_STATUS_IGNORE);
            for (int y = 0; y < temp_count_self; y++){
                pr_next[startingRangeSelf + y] = pr_temp_self[y];
            }
        }
        else { 
            for (int i = 1; i < world_size; i++){ 
                PageRankType *pr_temp = new PageRankType[proccRanges[i].end - proccRanges[i].start];
                long startingRange = proccRanges[i].start;
                long temp_count = proccRanges[i].end - proccRanges[i].start;
                for (int y = 0; y < temp_count; y++){
                    pr_temp[y] = pr_next[startingRange + y];
                }
                MPI_Send(
                /* data         = */ pr_temp, 
                /* count        = */ temp_count, 
                /* datatype     = */ PAGERANK_MPI_TYPE, 
                /* destination  = */ i, 
                /* tag          = */ 0, 
                /* communicator = */ MPI_COMM_WORLD
                );
                delete[] pr_temp;
            }
        }

        tCommTime += tComm.stop();
        // sync ends

        for (uintV v = start_vertex; v < end_vertex; v++){
            pr_next[v] = PAGE_RANK(pr_next[v]);
            pr_curr[v] = pr_next[v];
        }
        // Reset n for all vertices
        for (uintV v= 0; v < n; v++){
            pr_next[v] = 0.0;
        }
    }

    long local_sum = 0;
    for (int i = start_vertex; i < end_vertex; i++){
        local_sum += pr_curr[i];
    }

    // sync 2
    PageRankType global_sum = 0;
    if (world_rank == 0){
        global_sum += local_sum;
        printf("rank, number of edges, communication time\n");
        for (int rank = 1; rank < world_size; rank++){
            long procc_local_sum = 0;
            MPI_Recv(
            /* data         = */ &procc_local_sum, 
            /* count        = */ 1, 
            /* datatype     = */ PAGERANK_MPI_TYPE, 
            /* source       = */ rank, 
            /* tag          = */ 0, 
            /* communicator = */ MPI_COMM_WORLD, 
            /* status       = */ MPI_STATUS_IGNORE);
            global_sum += procc_local_sum;
        }

    }
    else {
        MPI_Send(
        /* data         = */ &local_sum, 
        /* count        = */ 1, 
        /* datatype     = */ PAGERANK_MPI_TYPE, 
        /* destination  = */ 0, 
        /* tag          = */ 0, 
        /* communicator = */ MPI_COMM_WORLD
        );
    }

    delete[] pr_temp_n;
    delete[] pr_temp_self;
    delete[] pr_curr;
    delete[] pr_next;
}

int main(int argc, char *argv[])
{
    g_max_iterations = _options["Iterations"].as<uint>();
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    g.readGraph(input_file);

    pageRankMPI();
    return 0;
}
