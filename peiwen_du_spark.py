import sys,time
from pyspark import SparkContext
from itertools import combinations
from decimal import *

fiter_number = int(sys.argv[1])
inputfile = sys.argv[2]
betweenness_file =sys.argv[3]
community_file = sys.argv[4]
#
# fiter_number = 7
# inputfile = "/Users/peiwendu/Downloads/public_data/sample_data.csv"
# betweenness_file ="betweenness.txt"
# community_file = "community1.txt"

sc = SparkContext(appName="inf553_summer_hw4", master="local[*]")
start = time.time()

def findBetweeness(root,p_c,n):
    res = []
    if root in p_c:
        occurence_nodes = []
        father_of_node = dict()
        node_value = dict()
        path_num = dict()
        layers = []
        edge_betweenness_in_each_graph = dict()
        last_layer_nodes = [root]
        path_num[root] = 1
        occurence_nodes.append(root)
        layers.append(last_layer_nodes)
        while len(occurence_nodes) <= n:
            now_layer_nodes = []
            for node in last_layer_nodes:
                child_nodes = p_c[node]
                for cn in child_nodes:
                    if cn in now_layer_nodes:
                        father_of_node[cn].append(node)
                        path_num[cn] += path_num[node]
                    else:
                        if cn not in occurence_nodes:
                            now_layer_nodes.append(cn)
                            father_of_node[cn] = [node]
                            path_num[cn] = path_num[node]
                            occurence_nodes.append(cn)
            if len(now_layer_nodes) == 0:
                break
            last_layer_nodes = now_layer_nodes
            layers.insert(0,last_layer_nodes)
        # print(layers)
        # print("BFS")
        # now_layer_nodes = []
        child_of_node = dict()

        for layer in layers:
            # print(layer)
            last_layer_nodes = []
            for node in layer:
                if node not in node_value:
                    node_value[node] = Decimal(1)
                # p=0
                # for fn in father_of_node[node]:
                #     p += len(father_of_node.get(fn,[root]))
                if node not in father_of_node:
                    break
                for fn in father_of_node[node]:
                    edge_betweenness_in_each_graph[(fn, node)] = Decimal(node_value[node] / path_num[node] * path_num[fn])
                    if fn in last_layer_nodes:
                        child_of_node[fn].append(node)
                    else:
                        last_layer_nodes.append(fn)
                        child_of_node[fn] = [node]
            for fn in last_layer_nodes:
                node_value[fn] = Decimal(1)
                for cn in child_of_node[fn]:
                    node_value[fn] += edge_betweenness_in_each_graph[(fn, cn)]
                # print(fn)
                # print(node_value[fn])


        # print(len(edge_betweenness_in_each_graph))
        for key in edge_betweenness_in_each_graph:
            res.append((tuple(sorted(key)),float(edge_betweenness_in_each_graph[key])))
    return res
#     for key in edge_betweenness_in_each_graph:
#         if tuple(sorted(key)) in edge_betweenness:
#             edge_betweenness[tuple(sorted(key))] += edge_betweenness_in_each_graph[key]
#         else:
#             edge_betweenness[tuple(sorted(key))] = edge_betweenness_in_each_graph[key]
#
# for key in edge_betweenness:
#     edge_betweenness[key] = edge_betweenness[key]/2

sample = sc.textFile(inputfile)
head = sample.first()
user_buiness_in_sample = sample.filter(lambda x:x!=head).map(lambda x:x.split(","))
users = user_buiness_in_sample.map(lambda x:x[0]).distinct().collect()
businesses = user_buiness_in_sample.map(lambda x:x[1]).distinct().collect()
user_index = dict()
business_index = dict()
for i,u in enumerate(users):
    user_index[u] = i
for i,b in enumerate(businesses):
    business_index[b] = i

# construct graph
graph = user_buiness_in_sample.map(lambda x:(business_index[x[1]],[user_index[x[0]]])).reduceByKey(lambda x,y:x+y).flatMap(lambda x:[(tuple(sorted(pair)),1) for pair in combinations(x[1],2)]).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=fiter_number).map(lambda x:x[0])
nodes = graph.flatMap(lambda x:[x[0],x[1]]).distinct()

# calculate betweenness
childnodes = graph.flatMap(lambda x:[(x[0],[x[1]]),(x[1],[x[0]])]).reduceByKey(lambda x,y:x+y).collect()
parent_child = dict()
for node in childnodes:
    parent_child[node[0]] = node[1]

n = len(childnodes)

betweenness = nodes.flatMap(lambda x:findBetweeness(x,parent_child,n)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0],x[1]/2))
# print(betweenness)

original_betweeness = betweenness.map(lambda x:(sorted([users[x[0][0]],users[x[0][1]]]),x[1])).sortBy(lambda x:x[0][0]).sortBy(lambda x:x[1],False)

with open(betweenness_file,"w") as f:
    for pair in original_betweeness.collect():
        f.write("(")
        f.write(pair[0][0])
        f.write(", ")
        f.write(pair[0][1])
        f.write(")")
        f.write(", ")
        f.write(str(pair[1]))
        f.write("\n")


def findSet(root,new_p_c):
    one_set = [root]
    last_layer_nodes = [root]
    while len(one_set) <= n:
        if root not in new_p_c:
            break
        now_layer_nodes = []
        for node in last_layer_nodes:
            child_nodes = new_p_c[node]
            for cn in child_nodes:
                if cn not in now_layer_nodes:
                    if cn not in one_set:
                        now_layer_nodes.append(cn)
        if len(now_layer_nodes) == 0:
            break
        last_layer_nodes = now_layer_nodes
        one_set.extend(last_layer_nodes)
    return tuple(sorted(one_set))


m = graph.count()
edges = graph.collect()

def modularity(map):
    res = []
    for i in combinations(map,2):
        if tuple(sorted(i)) in edges:
            res.append(float(Decimal(1)-Decimal((len(parent_child[i[0]])*len(parent_child[i[1]])/Decimal(2*m)))))
        else:
            res.append(float(Decimal(0) - Decimal((len(parent_child[i[0]]) * len(parent_child[i[1]])/ Decimal(2 * m)))))
    # print("sum:")
    # print(sum(res))
    return sum(res)

# delete_edge = betweenness.sortBy(lambda x:x[1],False).map(lambda x:(x[0][0],x[0][1])).first()
# print(delete_edge)
# sets = graph.filter(lambda x: x == delete_edge).flatMap(lambda x: [x[0], x[1]]).map(lambda x:findSet(x, parent_child))
# Q = sets.map(modularity).sum() / (2 * m)
# print(len(sets.take(1)[0]))
# print(Q)

increase = Decimal(1)
delete_edge = betweenness.sortBy(lambda x: x[1], False).map(lambda x: (x[0][0], x[0][1])).first()
delete_edges = [delete_edge]
default_sets = sc.emptyRDD()
Q_max = Decimal(0)
while increase>=0.0:
    new_graph = graph.filter(lambda x: x not in delete_edges)
    # print(new_graph.count())
    children = new_graph.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collect()
    new_parent_child = dict()
    for node in children:
        new_parent_child[node[0]] = node[1]

    sets = graph.filter(lambda x: x in delete_edges).flatMap(lambda x: [x[0], x[1]]).distinct().map(lambda x: findSet(x, new_parent_child)).distinct()
    # print(sets.filter(lambda x:len(x)==1).collect())
    # print(nodes.subtract(sets.flatMap(lambda x:[x])))
    left_sets = nodes.subtract(sets.flatMap(lambda x:list(x))).map(lambda x: findSet(x, new_parent_child)).distinct()
    # print(nodes.count())
    # print(sets.flatMap(lambda x:list(x)).count())
    # print(nodes.subtract(sets.flatMap(lambda x:list(x))).count())
    # print(sets.collect())
    all_sets = sets.union(left_sets)
    Q = float(Decimal(all_sets.map(modularity).sum()) / (2 * m))
    # print("Q:")
    # print(Q)

    if Q_max <= Q:
        default_sets = all_sets
        Q_max = Q
    increase = Q - Q_max
    # print(increase)
    # print(delete_edges)
    delete_edges.append(nodes.flatMap(lambda x:findBetweeness(x,new_parent_child,n)).reduceByKey(lambda x, y: x + y).sortBy(lambda x:x[1], False).map(lambda x:(x[0][0],x[0][1])).first())


communities = default_sets.map(lambda x:sorted([users[i] for i in x])).sortBy(lambda x:x[0]).sortBy(lambda x:len(x)).collect()
# print(default_sets.flatMap(lambda x:list(x)).count())

with open(community_file,"w") as f:
    for c in communities:
        for u in c:
            if list(c).index(u) != len(c)-1:
                f.write(u+", ")
            else:
                f.write(u+"\n")

end = time.time()
print(end-start)