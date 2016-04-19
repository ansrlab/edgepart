#pragma once

#include <vector>
#include <boost/unordered_map.hpp>

#include "util.hpp"

template<typename ValueType, typename KeyType, typename IdxType = vid_t>
class MinHeap {
private:
    IdxType n;
    std::vector<std::pair<ValueType, KeyType>> heap;
    boost::unordered_map<KeyType, IdxType> key2idx;

public:
    MinHeap() : n(0), heap(), key2idx() { }

    IdxType shift_up(IdxType cur) {
        if (cur == 0) return 0;
        IdxType p = (cur-1) / 2;

        if (heap[cur].first < heap[p].first) {
            std::swap(heap[cur], heap[p]);
            std::swap(key2idx[heap[cur].second], key2idx[heap[p].second]);
            return shift_up(p);
        }
        return cur;
    }

    void shift_down(IdxType cur) {
        IdxType l = cur*2 + 1;
        IdxType r = cur*2 + 2;

        if (l >= n)
            return;

        IdxType m = cur;
        if (heap[l].first < heap[cur].first)
            m = l;
        if (r < n && heap[r].first < heap[m].first)
            m = r;

        if (m != cur) {
            std::swap(heap[cur], heap[m]);
            std::swap(key2idx[heap[cur].second], key2idx[heap[m].second]);
            shift_down(m);
        }
    }

    void insert(ValueType value, KeyType key) {
        heap[n] = std::make_pair(value, key);
        key2idx[key] = n++;
        IdxType cur = shift_up(n-1);
        shift_down(cur);
    }

    bool contains(KeyType key) {
        return key2idx.count(key);
    }

    void decrease_key(KeyType key, ValueType d = 1) {
        if (d == 0) return;
        auto it = key2idx.find(key);
        CHECK(it != key2idx.end()) << "key not found";
        IdxType cur = it->second;

        CHECK_GE(heap[cur].first, d) << "value cannot be negative";
        heap[cur].first -= d;
        shift_up(cur);
    }

    bool remove(KeyType key) {
        auto it = key2idx.find(key);
        if (it == key2idx.end())
            return false;
        IdxType cur = it->second;

        n--;
        if (n > 0) {
            heap[cur] = heap[n];
            key2idx[heap[cur].second] = cur;
            cur = shift_up(cur);
            shift_down(cur);
        }
        key2idx.erase(it);
        return true;
    }

    bool get_min(ValueType& value, KeyType& key) {
        if (n > 0) {
            value = heap[0].first;
            key = heap[0].second;
            return true;
        } else
            return false;
    }

    void reset(IdxType nelements) {
        n = 0;
        heap.resize(nelements);
        key2idx.clear();
    }

    void clear() {
        n = 0;
        heap.clear();
        key2idx.clear();
    }
};
