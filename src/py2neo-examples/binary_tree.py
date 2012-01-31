class py2neo_test(object):
    import sys
    sys.path.insert(0, "/home/ayuskauskas/py2neo-me/src")
    from py2neo import neo4j
    db = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
    size = 1000
    def send_in_stages(self,func,objects):

        return_values = []
        for i in xrange(0,len(objects),self.size):
            end_i = i + self.size
            if end_i >= len(objects):
                end_i = len(objects) - 1
            return_values.extend(func(*objects[i:end_i]))

        return return_values

    def create_many(self,num):
        ref = self.db.get_reference_node()
        ref_nodes = [{'name':'seg_nodes'},
                 {'name':'op_nodes'}]

        seg_index = self.db.get_node_index('seg')
        op_index = self.db.get_node_index('op')

        seg_index.start_batch()
        seg_nodes = []
        for i in xrange(num):
            seg_nodes.append({'pk':i,'name':"seg_%i" % i,'node_type':'s'})

        op_nodes = []
        for i in xrange(int(num/2)):
            op_nodes.append({'pk':i, 'node_type':'o'})

        nodes = []
        nodes.extend(ref_nodes)
        nodes.extend(seg_nodes)
        nodes.extend(op_nodes)

        nodes = self.send_in_stages(self.db.create_nodes, nodes)
        
        seg_index.start_batch()
        for i, node in enumerate(nodes[2:]):
            if i < len(seg_nodes):
                seg_index.add(node, 'pk', seg_nodes[i]['pk'])
            if i == len(seg_nodes):
                seg_index.submit_batch()
                op_index.start_batch()
            if i >= len(seg_nodes):
                op_index.add(node, 'pk', op_nodes[i - len(seg_nodes)]['pk'])

        op_index.submit_batch()

        rels = [{'start_node': ref,
                 'end_node': nodes[0],
                 'type': 'seg_nodes'},
                {'start_node': ref,
                 'end_node': nodes[1],
                 'type': 'op_nodes'}]

        #sub ref seg nodes
        for node in nodes[2:(len(seg_nodes) + 2)]:
            rels.append({'start_node': node,
                         'end_node': nodes[0],
                         'type': 'instance_of'})
        #sub ref op nodes
        for node in nodes[len(seg_nodes) + 2:]:
            rels.append({'start_node': node,
                         'end_node': nodes[1],
                         'type': 'instance_of'})

        #create operation relations
        for i,node in enumerate(nodes[len(seg_nodes) + 2:]):
            create_node = nodes[i+2]
            i1 = nodes[(i*2) + 3]
            i2 = nodes[(i*2) + 4]
            rels.append({'start_node': node,
                         'end_node': create_node,
                         'type': 'create'})
            rels.append({'start_node': i1,
                         'end_node': node,
                         'type': 'input'})
            rels.append({'start_node': i2,
                         'end_node': node,
                         'type': 'input'})

        rels = self.send_in_stages(self.db.create_relationships,rels)

    def calc_subtree(self, pk):
        """Return a map of node uris to properties and a map of relationships (using uris)
        for the tree below and including this node."""
        index = self.db.get_node_index('seg')
        node = index.search('pk', [pk])[0][0]
        
        import time
        import collections
        
        t = time.time()
        traverser = node.traverse(order = "depth_first", relationships = [('create', 'in'),
                                                                          ('input', 'in')],
                                       prune = ('javascript',"1==0;"))

        print "traverser %0.2f" % (time.time() - t)

        t = time.time()
        #Get all the properties in one go
        nodes = traverser.nodes_by_uri
        print "traverser get nodes %0.2f" % (time.time() - t)

        t = time.time()
        node_props = self.db.get_properties_by_uri(*nodes)
#        node_props = self.send_in_stages(self.db.get_properties_by_uri, nodes)

        property_map = {}
        for i, node_uri in enumerate(nodes):
            try:
                property_map[node_uri] = node_props[i]
            except IndexError:
                print i
                raise

        property_map[node._uri] = node.get_properties()

        print "property_map %0.2f" % (time.time() - t)

        t = time.time()
        #Map out the relationships
        relationships = collections.defaultdict(list)
        for start, end in traverser.relationship_map:
            relationships[start].append(end)
 
        print "relationship mapping %0.2f" % (time.time() - t)
        return property_map, relationships
