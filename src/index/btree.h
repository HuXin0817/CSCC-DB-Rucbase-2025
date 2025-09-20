#pragma once

#include <algorithm>
#include <queue>

namespace btree
{
    static constexpr size_t node_limit = 256;
    static constexpr size_t split_prev_node_size = node_limit >> 1;
    static constexpr size_t split_next_node_size = node_limit - split_prev_node_size;

    template <typename key_t, typename compare>
    class btree_iterator;

    template <typename key_t, typename compare>
    class btree_mid_node;

    template <typename key_t, typename compare>
    class btree_leaf_node
    {
        friend class btree_iterator<key_t, compare>;
        friend class btree_mid_node<key_t, compare>;

    public:
        explicit btree_leaf_node(compare *c) : cmp(c) {}

        btree_leaf_node(compare *c, const key_t &first_value) : size(1), cmp(c)
        {
            keys[0] = first_value;
        }

        void insert(const key_t &key)
        {
            size_t pos = upper_bound_idx(key);
            std::copy_backward(keys + pos, keys + size, keys + size + 1);
            keys[pos] = key;
            ++size;
        }

        void erase(const key_t &key)
        {
            size_t pos = lower_bound_idx(key);
            std::copy(keys + pos + 1, keys + size, keys + pos);
            --size;
        }

        void split_to_new_node(btree_leaf_node *new_node)
        {
            std::copy(keys + split_prev_node_size, keys + size, new_node->keys);
            new_node->size = split_next_node_size;
            size = split_prev_node_size;
        }

        size_t lower_bound_idx(const key_t &key) const
        {
            auto it = std::lower_bound(keys, keys + size, key, [this](const key_t &a, const key_t &b)
                                       { return (*cmp)(a, b); });
            return it - keys;
        }

        size_t upper_bound_idx(const key_t &key) const
        {
            auto it = std::upper_bound(keys, keys + size, key, [this](const key_t &a, const key_t &b)
                                       { return (*cmp)(a, b); });
            return it - keys;
        }

        bool contains(const key_t &key) const
        {
            return std::binary_search(keys, keys + size, key, [this](const key_t &a, const key_t &b)
                                      { return (*cmp)(a, b); });
        }

        const key_t &front() const { return keys[0]; }

        bool empty() const { return size == 0; }

        bool is_full() const { return size == node_limit; }

        void reset()
        {
            size = 0;
            next = nullptr;
        }

    private:
        size_t size = 0;
        key_t keys[node_limit];
        btree_leaf_node *next = nullptr;
        compare *cmp;
    };

    template <typename key_t, typename compare>
    class btree_iterator
    {
    public:
        using btree_leaf_node_t = btree_leaf_node<key_t, compare>;

        btree_iterator() = default;

        btree_iterator(btree_leaf_node_t *node, size_t idx) : node(node), idx(idx) {}

        btree_iterator operator++()
        {
            idx++;
            if (idx == node->size)
            {
                node = node->next;
                idx = 0;
            }
            return *this;
        }

        bool operator==(const btree_iterator &other) const { return node == other.node && idx == other.idx; }

        key_t operator*() const { return node->keys[idx]; }

    private:
        btree_leaf_node_t *node = nullptr;
        size_t idx = 0;
    };

    template <typename key_t, typename compare>
    class btree_mid_node
    {
    public:
        using btree_iterator_t = btree_iterator<key_t, compare>;
        using btree_leaf_node_t = btree_leaf_node<key_t, compare>;

        explicit btree_mid_node(compare *c) : cmp(c) {}

        btree_mid_node(compare *c, const key_t &first_value) : size(1), cmp(c)
        {
            sons[0] = new btree_leaf_node_t(cmp, first_value);
        }

        void insert(const key_t &key)
        {
            size_t idx = find_son_idx(key);
            auto son_node = sons[idx];
            son_node->insert(key);
            if (son_node->is_full())
            {
                auto split_node = new btree_leaf_node_t(cmp);
                son_node->split_to_new_node(split_node);
                std::copy_backward(sons + idx + 1, sons + size, sons + size + 1);
                ++size;
                sons[idx + 1] = split_node;
                sons[idx]->next = sons[idx + 1];
                if (idx + 2 < size)
                {
                    sons[idx + 1]->next = sons[idx + 2];
                }
            }
        }

        void erase(const key_t &key)
        {
            size_t idx = find_son_idx(key);
            auto son_node = sons[idx];
            if (son_node->size == 1)
            {
                std::copy(sons + idx + 1, sons + size, sons + idx);
                --size;
                if (idx > 0)
                {
                    sons[idx - 1]->next = sons[idx];
                }
                delete son_node;
            }
            else
            {
                son_node->erase(key);
            }
        }

        void split_to_new_node(btree_mid_node *new_node)
        {
            std::copy(sons + split_prev_node_size, sons + size, new_node->sons);
            new_node->size = split_next_node_size;
            size = split_prev_node_size;
        }

        bool is_full() const { return size == node_limit; }

        btree_leaf_node_t *front_leaf() const { return sons[0]; }

        void set_next(btree_leaf_node_t *next_node) { sons[size - 1]->next = next_node; }

        btree_iterator_t begin() const { return btree_iterator_t(sons[0], 0); }

        btree_iterator_t end() const { return btree_iterator_t(sons[size - 1]->next, 0); }

        bool empty() const { return size == 0; }

        btree_iterator_t lower_bound(const key_t &key) const
        {
            auto son = sons[find_son_idx(key)];
            auto son_idx = son->lower_bound_idx(key);
            if (son_idx == son->size)
            {
                return btree_iterator_t(son->next, 0);
            }
            else
            {
                return btree_iterator_t(son, son_idx);
            }
        };

        btree_iterator_t upper_bound(const key_t &key) const
        {
            auto son = sons[find_son_idx(key)];
            auto son_idx = son->upper_bound_idx(key);
            if (son_idx == son->size)
            {
                return btree_iterator_t(son->next, 0);
            }
            else
            {
                return btree_iterator_t(son, son_idx);
            }
        }

        bool contains(const key_t &key) const
        {
            if (size == 0)
            {
                return false;
            }
            return sons[find_son_idx(key)]->contains(key);
        }

        const key_t &front() const { return sons[0]->front(); }

    private:
        size_t find_son_idx(const key_t &key) const
        {
            auto it = std::upper_bound(sons, sons + size, key, [this](const key_t &a, const btree_leaf_node_t *b)
                                       { return (*cmp)(a, b->front()); });
            size_t idx = it - sons;
            return idx == 0 ? 0 : idx - 1;
        }

        size_t size = 0;
        btree_leaf_node_t *sons[node_limit];
        compare *cmp;
    };

    static constexpr size_t root_node_size = 0x10000;

    template <typename key_t, typename compare = std::less<key_t>>
    class btree_set
    {
        using btree_mid_node_t = btree_mid_node<key_t, compare>;

    public:
        using iterator = btree_iterator<key_t, compare>;

        explicit btree_set(const compare &c = compare()) : size(0), cmp(c) {}

        void insert(const key_t &key)
        {
            if (size == 0)
            {
                sons[0] = new btree_mid_node_t(&cmp, key);
                size = 1;
                return;
            }
            size_t idx = find_son_idx(key);
            auto son_node = sons[idx];
            son_node->insert(key);
            if (son_node->is_full())
            {
                auto split_node = new btree_mid_node_t(&cmp);
                son_node->split_to_new_node(split_node);
                std::copy_backward(sons + idx + 1, sons + size, sons + size + 1);
                sons[idx + 1] = split_node;
                ++size;
                sons[idx]->set_next(sons[idx + 1]->front_leaf());
                if (idx + 2 < size)
                {
                    sons[idx + 1]->set_next(sons[idx + 2]->front_leaf());
                }
            }
            else if (idx + 1 < size)
            {
                sons[idx]->set_next(sons[idx + 1]->front_leaf());
            }
        }

        void erase(const key_t &key)
        {
            auto idx = find_son_idx(key);
            auto son_node = sons[idx];
            son_node->erase(key);
            if (son_node->empty())
            {
                std::copy(sons + idx + 1, sons + size, sons + idx);
                --size;
                if (idx > 0)
                {
                    if (idx < size)
                    {
                        sons[idx - 1]->set_next(sons[idx]->front_leaf());
                    }
                    else
                    {
                        sons[idx - 1]->set_next(nullptr);
                    }
                }
                delete son_node;
            }
            else if (idx > 0)
            {
                sons[idx - 1]->set_next(sons[idx]->front_leaf());
            }
        }

        iterator begin() const
        {
            if (size == 0)
            {
                return end();
            }
            return sons[0]->begin();
        }

        iterator end() const { return iterator(nullptr, 0); }

        iterator lower_bound(const key_t &key) const
        {
            if (size == 0)
            {
                return end();
            }
            return sons[find_son_idx(key)]->lower_bound(key);
        };

        iterator upper_bound(const key_t &key) const
        {
            if (size == 0)
            {
                return end();
            }
            return sons[find_son_idx(key)]->upper_bound(key);
        }

        bool contains(const key_t &key) const
        {
            if (size == 0)
            {
                return false;
            }
            return sons[find_son_idx(key)]->contains(key);
        }

    private:
        size_t find_son_idx(const key_t &key) const
        {
            auto it = std::upper_bound(sons, sons + size, key, [this](const key_t &a, const btree_mid_node_t *b)
                                       { return cmp(a, b->front()); });
            size_t idx = it - sons;
            return idx == 0 ? 0 : idx - 1;
        }

        size_t size = 0;
        btree_mid_node_t *sons[root_node_size];
        compare cmp;
    };

} // namespace btree
