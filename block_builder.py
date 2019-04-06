#!/usr/bin/python3

BLOCK_MAX_WEIGHT = 4000000


class MempoolTransaction():
    def __init__(self, txid, fee, weight, parents):
        self.txid = txid
        self.fee = int(fee)
        self.weight = int(weight)
        self.parents = list(filter(None, parents.split(';')))

    def __repr__(self):
        parents_str = ''
        for p in self.parents:
            parents_str += p + ';'
        parents_str = parents_str[:-1]
        return '{},{},{},{}'.format(self.txid, self.fee, self.weight, parents_str)


# parse mempool as dictionary, for quicker access to transactions
def parse_mempool_csv():
    """Parse the CSV file and return a list of MempoolTransactions."""
    mempool = {}
    with open('mempool.csv') as f:
        for line in f:
            txid, fee, weight, parents = line.strip().split(',')
            mempool[txid] = MempoolTransaction(txid, fee, weight, parents)
    return mempool


# keep only tx with parents in the mempool, removes from mempool inplace
def reduce_to_mempool_parents(mempool_dict):
    tx_to_remove = []
    for txid, tx_data in mempool.items():
        for parent in tx_data.parents:
            if parent not in mempool.keys():
                tx_to_remove.append(txid)
                continue
    for txid in tx_to_remove:
        del mempool[txid]


# build ancestor (great-parents) set for transaction
# avoid forbidden set of tx (which are already used before)
def build_ancestors_set(mempool_dict, txid, excluded_tx=None):
    if excluded_tx is None:
        excluded_tx = {}

    ancestors = set()
    if txid in excluded_tx:
        return ancestors

    unvisited_parents = set(mempool_dict[txid].parents)
    while unvisited_parents:
        parent = unvisited_parents.pop()
        if parent in excluded_tx:
            continue
        ancestors.add(parent)
        unvisited_parents = unvisited_parents.union(set(mempool_dict[parent].parents))
    return ancestors


# calculate required accumulated weight and fees for tx (by itself and ancestors)
def accumulated_fee_and_weight_for_txs(mempool, txs):
    accum_fee = 0
    accum_weight = 0
    for txid in txs:
        accum_fee += mempool[txid].fee
        accum_weight += mempool[txid].weight
    return accum_fee, accum_weight


# get currently maximal fee tx (excluding some tx as itself and ancestors), but with accumulated weight limit
# if all remaining tx weights are below limit, returns ''
def get_max_fee_tx(mempool, weight_limit, excluded_tx=None):
    if excluded_tx is None:
        excluded_tx = {}

    max_fee_tx = ''
    max_fee = -1
    max_tx_weight = -1
    max_tx_ancestors = set()

    for txid in mempool.keys():
        if txid in excluded_tx:
            continue
        ancestors = build_ancestors_set(mempool, txid, excluded_tx)
        ancestors.add(txid)
        fee, weight = accumulated_fee_and_weight_for_txs(mempool, ancestors)

        if weight <= weight_limit:
            if fee > max_fee:
                max_fee = fee
                max_fee_tx = txid
                max_tx_weight = weight
                max_tx_ancestors = ancestors

    return max_fee_tx, max_fee, max_tx_weight, max_tx_ancestors


# the greedy solution of taking the maximal accumulated fee tx (constrained under current weight limit)
def greedy_block_from_mempool(mempool):

    tx_in_block = set()
    remaining_weight = BLOCK_MAX_WEIGHT

    if not mempool:
        return tx_in_block

    while True:
        txid, tx_fee, tx_weight, ancestors = get_max_fee_tx(mempool, remaining_weight, tx_in_block)

        # all remaining tx are above remaining weight, so finish
        if txid == '':
            break

        remaining_weight -= tx_weight
        tx_in_block = tx_in_block.union(ancestors)

    return tx_in_block


# build recursively an ordered list of ancestors, parents appear before children
def recursive_order_parents_first(mempool, txid, ordered_ancestors):
    if txid in ordered_ancestors:
        return

    # add all parents first
    for parent in mempool[txid].parents:
        if parent not in ordered_ancestors:
            recursive_order_parents_first(mempool, parent, ordered_ancestors)

    ordered_ancestors.append(txid)


def order_parents_first(mempool, block_set):
    ordered_block_tx = []
    for txid in block_set:
        recursive_order_parents_first(mempool, txid, ordered_block_tx)
    return ordered_block_tx


def write_block_file(ordered_block):
    with open('block.txt', 'w') as f:
        for txid in ordered_block:
            f.write('{}\n'.format(txid))


mempool = parse_mempool_csv()

reduce_to_mempool_parents(mempool)

block_set = greedy_block_from_mempool(mempool)

ordered_block_list = order_parents_first(mempool, block_set)

write_block_file(ordered_block_list)
