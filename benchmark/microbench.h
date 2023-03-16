#ifndef _MICRO_BEHCN_H
#define _MICRO_BEHCN_H

#include "config.h"
#include "generator.h"
#include "util.h"
#include <assert.h>
#include <utility>

enum OperationType {
    INSERT,
    REMOVE,
    UPDATE,
    GET,
    SCAN,
    MIXED,
    READMODIFYWRITE,
    _OpreationTypeNumber
};

template <typename T> inline void swap(T &a, T &b) {
    T tmp = a;
    a = b;
    b = tmp;
}

long long *random_shuffle(int s) {
    long long *x = new long long[s];
    for (int i = 0; i < s; i++) {
        x[i] = i;
    }
    for (int i = 0; i < s; i++) {
        swap(x[i], x[random() % s]);
    }
    return x;
}

class Benchmark {
  public:
    WorkloadGenerator *workload;
    long long key_range;
    long long init_key;
    long long *x;
    Config _conf;
    DataSet *dataset;
    RandomGenerator rdm;

    Benchmark(Config &conf) : init_key(0), _conf(conf) {
        dataset = new DataSet(conf.init_keys, conf.val_length, conf.email);
        if (conf.workload == RANDOM) {
            workload = new RandomGenerator();
        } else if (conf.workload == ZIPFIAN) {
            workload = new ZipfWrapper(conf.skewness, conf.init_keys);
        }

        x = NULL;
    }

    virtual ~Benchmark() {
        if (x != NULL)
            delete[] x;
    }

    virtual std::pair<OperationType, long long> nextIntOperation() {
        return std::make_pair(INSERT, workload->Next());
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next();
        return std::make_pair(INSERT, dataset->wl_str[next % _conf.init_keys]);
    }

    virtual long long nextInitIntKey() {
        if (x == NULL)
            x = random_shuffle(_conf.init_keys);
        return x[init_key++ % _conf.init_keys];
        // return init_key++ % _conf.init_keys;
    }

    virtual std::string nextInitStrKey() {
        return dataset->wl_str[(init_key++) % _conf.init_keys];
    }
} __attribute__((aligned(64)));

class ReadOnlyBench : public Benchmark {
  public:
    ReadOnlyBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation() {
        long long d = workload->Next() % _conf.init_keys;
        return std::make_pair(GET, d);
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        return std::make_pair(GET, dataset->wl_str[next]);
    }
} __attribute__((aligned(64)));

class InsertOnlyBench : public Benchmark {
    RandomGenerator rdm;

  public:
    InsertOnlyBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation() {
        long long d = workload->Next() % _conf.init_keys;
#ifdef INSERT_DUP
        long long x = 1;
#else
        long long x = rdm.randomInt() % 128;
#endif

        return std::make_pair(INSERT, d * x);
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        char c = rdm.randomInt() % 94 + 33;
        char d = rdm.randomInt() % 94 + 33;
        s = d + s;
        s = c + s;
        return std::make_pair(INSERT, s);
    }

#ifndef INSERT_DUP

    long long nextInitIntKey() { return 128 * Benchmark::nextInitIntKey(); }

#endif
} __attribute__((aligned(64)));

class UpdateOnlyBench : public Benchmark {
  public:
    UpdateOnlyBench(Config &conf) : Benchmark(conf) {}

    OperationType nextOp() { return UPDATE; }

    std::pair<OperationType, long long> nextIntOperation() {
        long long d = workload->Next() % _conf.init_keys;
        return std::make_pair(UPDATE, d);
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        return std::make_pair(UPDATE, dataset->wl_str[next]);
    }
} __attribute__((aligned(64)));

class DeleteOnlyBench : public Benchmark {
  public:
    DeleteOnlyBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation() {
        long long d = workload->Next() % _conf.init_keys;
        return std::make_pair(REMOVE, d);
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        return std::make_pair(REMOVE, dataset->wl_str[next]);
    }
} __attribute__((aligned(64)));

class MixedBench : public Benchmark {
    int round;
    long long key;
    std::string skey;

  public:
    MixedBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation() {
        std::pair<OperationType, long long> result;
        long long _key = workload->Next() % _conf.init_keys;
        switch (round) {
        case 0:
            key = workload->Next() % _conf.init_keys;
            result = std::make_pair(REMOVE, key);
            break;
        case 1:
            result = std::make_pair(INSERT, key);
            break;
        case 2:
            result = std::make_pair(UPDATE, _key);
            break;
        case 3:
            result = std::make_pair(GET, _key);
            break;
        default:
            assert(0);
        }
        round++;
        round %= 4;
        return result;
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        std::pair<OperationType, std::string> result;
        long long next = workload->Next() % _conf.init_keys;
        std::string _key = dataset->wl_str[next];
        switch (round) {
        case 0:
            next = workload->Next() % _conf.init_keys;
            skey = dataset->wl_str[next];
            result = std::make_pair(REMOVE, skey);
            break;
        case 1:
            result = std::make_pair(INSERT, skey);
            break;
        case 2:
            result = std::make_pair(UPDATE, _key);
            break;
        case 3:
            result = std::make_pair(GET, _key);
            break;
        default:
            assert(0);
        }
        round++;
        round %= 4;
        return result;
    }
} __attribute__((aligned(64)));

class ScanBench : public Benchmark {
  public:
    ScanBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation() {
        long long d = workload->Next() % _conf.init_keys;
        return std::make_pair(SCAN, d);
    }

    std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        return std::make_pair(SCAN, s);
    }
} __attribute__((aligned(64)));

class YSCBA : public Benchmark {
  public:
    //	readRate = 0.5;
    //	writeRate = 0.5;
    int read_ratio = 50;

    RandomGenerator rdm;

    YSCBA(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k > read_ratio) {
            long long x = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  x * (workload->Next() % _conf.init_keys));
        } else {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            char c = rdm.randomInt() % 94 + 33;
            char d = rdm.randomInt() % 94 + 33;
            s = d + s;
            s = c + s;
            return std::make_pair(INSERT, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

class YSCBB : public Benchmark {
  public:
    //	readRate = 0.95;
    //	writeRate = 0.05;
    int read_ratio = 95;
    RandomGenerator rdm;

    YSCBB(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k < read_ratio) {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        } else {
            long long x = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  x * (workload->Next() % _conf.init_keys));
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            char c = rdm.randomInt() % 94 + 33;
            char d = rdm.randomInt() % 94 + 33;
            s = d + s;
            s = c + s;
            return std::make_pair(INSERT, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// 50% read, 50% update
class YSCBC : public Benchmark {
  public:
    //	readRate = 0.5;
    //	writeRate = 0.5;
    int read_ratio = 50;

    RandomGenerator rdm;

    YSCBC(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k > read_ratio) {
            return std::make_pair(UPDATE, (workload->Next() % _conf.init_keys));
        } else {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            return std::make_pair(UPDATE, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// workload D, 95 read 5 insert
class YSCBD : public Benchmark {
  public:
    int read_ratio = 95;

    RandomGenerator rdm;

    YSCBD(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k < read_ratio) {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        } else {
            int d = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  (workload->Next() % _conf.init_keys) * d);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            // char c = rdm.randomInt() % 94 + 33;
            // char d = rdm.randomInt() % 94 + 33;
            // s = d + s;
            // s = c + s;
            return std::make_pair(INSERT, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// scan/insert
class YSCBE : public Benchmark {
  public:
    int scan_ratio = 95;
    RandomGenerator rdm;

    YSCBE(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k < scan_ratio) {
            return std::make_pair(SCAN, workload->Next() % _conf.init_keys);
        } else {
            int d = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  (workload->Next() % _conf.init_keys) * d);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k < scan_ratio) {
            return std::make_pair(SCAN, s);
        } else {
            char c = rdm.randomInt() % 94 + 33;
            char d = rdm.randomInt() % 94 + 33;
            s = d + s;
            s = c + s;
            return std::make_pair(INSERT, s);
        }
    }
} __attribute__((aligned(64)));


// 读50% 更新50%
class YCSBA : public Benchmark {
  public:
    //	readRate = 0.5;
    //	updateRate = 0.5;
    int read_ratio = 50;

    RandomGenerator rdm;

    YCSBA(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k > read_ratio) {
            long long x = rdm.randomInt() % 128;
            return std::make_pair(UPDATE,
                                  x * (workload->Next() % _conf.init_keys));
        } else {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            return std::make_pair(UPDATE, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// 读95% 更新5%
class YCSBB : public Benchmark {
  public:
    //	readRate = 0.95;
    //	updateRate = 0.05;
    int read_ratio = 95;

    RandomGenerator rdm;

    YCSBB(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k > read_ratio) {
            long long x = rdm.randomInt() % 128;
            return std::make_pair(UPDATE,
                                  x * (workload->Next() % _conf.init_keys));
        } else {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            return std::make_pair(UPDATE, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// 读100%
class YCSBC : public Benchmark {
  public:
    //	readRate = 1.00;
    int read_ratio = 100;

    RandomGenerator rdm;

    YCSBC(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        return std::make_pair(GET, workload->Next() % _conf.init_keys);
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        return std::make_pair(GET, s);
    }
} __attribute__((aligned(64)));

// 读95% 插入5%
class YCSBD : public Benchmark {
  public:
    //	readRate = 0.95;
    //	insertRate = 0.05;
    int read_ratio = 95;

    RandomGenerator rdm;

    YCSBD(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k > read_ratio) {
            long long x = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  x * (workload->Next() % _conf.init_keys));
        } else {
            return std::make_pair(GET, workload->Next() % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            return std::make_pair(INSERT, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));

// 范围查询 95% 插入5%
class YCSBE : public Benchmark {
  public:
    int scan_ratio = 95;
    RandomGenerator rdm;

    YCSBE(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k < scan_ratio) {
            return std::make_pair(SCAN, workload->Next() % _conf.init_keys);
        } else {
            int d = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  (workload->Next() % _conf.init_keys) * d);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k < scan_ratio) {
            return std::make_pair(SCAN, s);
        } else {
            // char c = rdm.randomInt() % 94 + 33;
            // char d = rdm.randomInt() % 94 + 33;
            // s = d + s;
            // s = c + s;
            return std::make_pair(INSERT, s);
        }
    }
} __attribute__((aligned(64)));

// 读 50% ，读-改-写50%
class YCSBF : public Benchmark {
  public:
    int read_ratio = 50;
    RandomGenerator rdm;

    YCSBF(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation() {
        int k = rdm.randomInt() % 100;
        if (k < read_ratio) {
            return std::make_pair(READMODIFYWRITE, workload->Next() % _conf.init_keys);
        } else {
            int d = rdm.randomInt() % 128;
            return std::make_pair(INSERT,
                                  (workload->Next() % _conf.init_keys) * d);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation() {
        int k = rdm.randomInt() % 100;
        long long next = workload->Next() % _conf.init_keys;
        std::string s = dataset->wl_str[next];
        if (k > read_ratio) {
            return std::make_pair(READMODIFYWRITE, s);
        } else {
            return std::make_pair(GET, s);
        }
    }
} __attribute__((aligned(64)));


#endif
