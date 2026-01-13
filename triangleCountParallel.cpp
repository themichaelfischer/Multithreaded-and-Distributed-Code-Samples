// Code contains redacted code from academic course work
// to be shared for employement review

#include <future>
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>
#include <mutex>
#include <vector>
#include <condition_variable>

struct Data{
  uint thread_id;
  uint triangle_count;
  double time_taken;
};

// GLOBALS
Graph g;
std::vector<Data> g_dataArr;
uint g_num_workers = 1;
uint g_total_triangles = 0;
int g_active_threads = 0;
std::mutex m_active_thread;
std::mutex m_data_thread;
std::mutex m_total_triangles;
std::condition_variable c_no_more_active_threads;

long countTriangles(uintV *array1, uintE len1, uintV *array2, uintE len2, uintV u, uintV v) {
  return count; // redacted: returns triangle count for that vertex
}

void triangleCountThread(uintV u, uintV n, int t_id){
  long triangle_count = 0;
  timer t;
  t.start();
  for (u; u < n; u++) {
    uintE out_degree = g.vertices_[u].getOutDegree();
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                       g.vertices_[u].getInDegree(),
                                       g.vertices_[v].getOutNeighbors(),
                                       g.vertices_[v].getOutDegree(), u, v);
    }
  }

  Data data;
  data.thread_id = t_id;
  data.triangle_count = triangle_count;
  data.time_taken = t.stop();

  m_data_thread.lock();
  g_dataArr.push_back(data);
  m_data_thread.unlock();

  m_total_triangles.lock();
  g_total_triangles += triangle_count;
  m_total_triangles.unlock();

  m_active_thread.lock();
  g_active_threads--;
  if (g_active_threads == 0){
    c_no_more_active_threads.notify_all();
  }
  m_active_thread.unlock();
}

void triangleCount() {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;

  std::vector<std::thread> threadsArr;
  int remainder = 0;

  if (n % g_num_workers != 0){ remainder = n % g_num_workers; }
  uintV sub_n = n / g_num_workers; 

  t1.start();
  for (int i = 0; i < g_num_workers; i++){
    m_active_thread.lock();
    g_active_threads++;
    m_active_thread.unlock();
    if ((i + 1) == g_num_workers){
      threadsArr.emplace_back(triangleCountThread, sub_n * i, (sub_n * (i+1)) + remainder, i);
      continue;
    }
    threadsArr.emplace_back(triangleCountThread, sub_n * i, (sub_n * (i+1)), i);
  }

  std::unique_lock<std::mutex> lk(m_active_thread);
  c_no_more_active_threads.wait(lk, [] {return g_active_threads == 0;});
  m_active_thread.unlock(); 

  m_active_thread.lock();
  if (g_active_threads == 0) {
      for (std::thread &th : threadsArr){
          if (th.joinable()){
              th.join();
          }
      }
  }
  m_active_thread.unlock();
  time_taken = t1.stop();
}

int main(int argc, char *argv[]) {
  auto _options = options.parse(argc, argv);
  g_num_workers = _options["workers"].as<uint>();
  g.readGraph(input_file);
  triangleCount();
  return 0;
}
