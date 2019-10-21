#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include "tbb/tbb.h"

using namespace std;

#include "Tree.h"

void loadKey(TID tid, Key &key) { return; }

void run(char **argv) {
    std::cout << "Simple Example of P-ART" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    std::vector<Key *> Keys;

    Keys.reserve(n);
    ART_ROWEX::Tree tree(loadKey);
    auto t = tree.getThreadInfo();
    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = 2 * i;
        Keys[i] = Keys[i]->make_leaf(keys[i] + 1, sizeof(uint64_t), keys[i]);
        tree.insert(Keys[i], t);
        Keys[i] = Keys[i]->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
    }

    const int num_thread = atoi(argv[2]);
    // tbb::task_scheduler_init init(num_thread);
    std::thread *tid[num_thread];
    int every_thread_num = n / num_thread + 1;
    printf("operation,n,ops/s\n");
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        for (int i = 0; i < num_thread; i++) {
            tid[i] = new std::thread(
                [&](int id) {
                    auto tt = tree.getThreadInfo();
                    for (int j = 0; j < every_thread_num; j++) {
                        int k = j * num_thread + id;
                        if (k < n) {
                            tree.insert(Keys[k], tt);
                        }
                    }
                },
                i);
        }

        for (int i = 0; i < num_thread; i++) {
            tid[i]->join();
        }
        // tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
        //                   [&](const tbb::blocked_range<uint64_t> &range) {
        //                       auto t = tree.getThreadInfo();
        //                       for (uint64_t i = range.begin(); i !=
        //                       range.end();
        //                            i++) {
        //                           tree.insert(Keys[i], t);
        //                       }
        //                   });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n,
               (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n,
               duration.count() / 1000000.0);
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();

        for (int i = 0; i < num_thread; i++) {
            tid[i] = new std::thread(
                [&](int id) {
                    auto tt = tree.getThreadInfo();
                    for (int j = 0; j < every_thread_num; j++) {
                        int k = j * num_thread + id;
                        if (k < n) {
                            tree.lookup(Keys[k], tt);
                        }
                    }
                },
                i);
        }

        for (int i = 0; i < num_thread; i++) {
            tid[i]->join();
        }
        // tbb::parallel_for(
        //     tbb::blocked_range<uint64_t>(0, n),
        //     [&](const tbb::blocked_range<uint64_t> &range) {
        //         auto t = tree.getThreadInfo();
        //         for (uint64_t i = range.begin(); i != range.end(); i++) {
        //             uint64_t *val =
        //                 reinterpret_cast<uint64_t *>(tree.lookup(Keys[i],
        //                 t));
        //             if (*val != keys[i]) {
        //                 std::cout << "wrong value read: " << *val
        //                           << " expected:" << keys[i] << std::endl;
        //                 throw;
        //             }
        //         }
        //     });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n,
               (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n,
               duration.count() / 1000000.0);
    }

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf(
            "usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: "
            "number of threads (integer)\n",
            argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}