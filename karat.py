
'''

Suppose we have some input data describing a graph of relationships between parents and children over multiple generations. The data is formatted as a list of (parent, child) pairs, where each individual is assigned a unique integer identifier.

For example, in this diagram, 6 and 8 have a common ancestor of 4.

         14  13
         |   |
1   2    4   12
 \ /   / | \ /
  3   5  8  9
   \ / \     \
    6   7     11

parentChildPairs1 = [
    (1, 3), (2, 3), (3, 6), (5, 6), (5, 7), (4, 5),
    (4, 8), (4, 9), (9, 11), (14, 4), (13, 12), (12, 9)
]

Write a function that takes the graph, as well as two of the individuals in our dataset, as its inputs and returns true if and only if they share at least one ancestor.

Sample input and output:

hasCommonAncestor(parentChildPairs1, 3, 8) => false
hasCommonAncestor(parentChildPairs1, 5, 8) => true
hasCommonAncestor(parentChildPairs1, 6, 8) => true
hasCommonAncestor(parentChildPairs1, 6, 9) => true
hasCommonAncestor(parentChildPairs1, 1, 3) => false
hasCommonAncestor(parentChildPairs1, 7, 11) => true
hasCommonAncestor(parentChildPairs1, 6, 5) => true
hasCommonAncestor(parentChildPairs1, 5, 6) => true

Additional example: In this diagram, 4 and 12 have a common ancestor of 11.

        11
       /  \
      10   12
     /  \
1   2    5
 \ /    / \
  3    6   7
   \        \
    4        8

parentChildPairs2 = [
    (11, 10), (11, 12), (10, 2), (10, 5), (1, 3),
    (2, 3), (3, 4), (5, 6), (5, 7), (7, 8),
]

hasCommonAncestor(parentChildPairs2, 4, 12) => true
hasCommonAncestor(parentChildPairs2, 1, 6) => false
hasCommonAncestor(parentChildPairs2, 1, 12) => false

n: number of pairs in the input

'''

parent_child_pairs_1 = [
    (1, 3), (2, 3), (3, 6), (5, 6), (5, 7), (4, 5),
    (4, 8), (4, 9), (9, 11), (14, 4), (13, 12), (12, 9)
]

parent_child_pairs_2 = [
    (11, 10), (11, 12), (10, 2), (10, 5), (1, 3),
    (2, 3), (3, 4), (5, 6), (5, 7), (7, 8)
]



def display(input_func):
    print ('%s ==> %s' % (input_func, str(eval(input_func))))


def findNodesWithZeroAndOneParents(parent_child_pairs):
    pc_dict = dict()
    parents, one_parent = [], []   
    for p,c in parent_child_pairs:
        if str(p) not in pc_dict: pc_dict[str(p)] = [1,0]
        else: pc_dict[str(p)][0] += 1
            
        if str(c) not in pc_dict: pc_dict[str(c)] = [0,1]
        else: pc_dict[str(c)][1] += 1
           
    for k,v in pc_dict.items():
        if v[1] == 0:
            parents.append(int(k))
        if v[1] == 1:
            one_parent.append(int(k))
    return list([parents,one_parent])


def hasCommonAncestor(parent_child_pairs_1, n1, n2):
    def find_root_parents(parent_child_pairs_1, c, ps, o):
        for pp,cc in parent_child_pairs_1:
            if c == cc:
                if pp in ps: 
                    o.append(pp)
                else:
                    find_root_parents(parent_child_pairs_1, pp, ps, o)

    pc_dict_all = dict()
    
    # find root parents (ps) and non-root parent or children nodes (cs)
    ps = findNodesWithZeroAndOneParents(parent_child_pairs_1)[0]
    ca = set(a for a,b in parent_child_pairs_1)
    cb = set(b for a,b in parent_child_pairs_1)
    cs = [cc for cc in ca.union(cb) if cc not in ps]

    for p in ps: pc_dict_all[str(p)] = set()
    
    # base case 1
    if n1!=n2 and (n1 in ps and n2 in ps): return False
        
    for c in cs:
        o = []
        find_root_parents(parent_child_pairs_1, c, ps, o)
        if o == []: 
            print('***ERROR*** with finding root parents of child = %s' % (c))
        for root_p in o:
            pc_dict_all[str(root_p)].add(c)
    
    for k,v in pc_dict_all.items():
        if n1 in v and n2 in v:
            return True
    return False



to_test = [[3,8],[5,8],[6,8],[6,9],[1,3],[7,11],[6,5],[5,6]]

for n1,n2 in to_test:
    display('hasCommonAncestor(parent_child_pairs_1, %s, %s)' % (n1,n2))
    
to_test = [[4,12],[1,6],[1,12]]

for n1,n2 in to_test:
    display('hasCommonAncestor(parent_child_pairs_2, %s, %s)' % (n1,n2))
