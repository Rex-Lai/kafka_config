import os

# global variable
broker_tag = 'KK'
zk_tag = 'server.'
newline_tag = '\n'
comment_tag = '#'
template = './config'
output = './output'
bk_port = '9092'
zk_port = '2181'
rep_cnt = 20

def check_directory(output_directory):
    if os.path.exists(output_directory) == False:
        os.makedirs(output_directory)

def add_hosts_port(hosts, port):
    result = []
    for host in hosts:
        result.append(host + ':' + port)
    return result

def get_hosts(hosts):
    hosts_port = add_hosts_port(hosts, bk_port)
    return ','.join(hosts_port).replace(newline_tag,'')

def get_hosts_zk(hosts):
    hosts_port = add_hosts_port(hosts, zk_port)
    return ','.join(hosts_port).replace(newline_tag,'')

def get_hosts_with_tag(tag1, tag2, hosts, newline):
    result = []
    cnt = 1
    for host in hosts:
        result.append(tag1 + str(cnt) + tag2 + host.replace(newline,'', 1))
        cnt += 1
    if tag1 == zk_tag:
        return ''.join(result)
    else:
        return ','.join(result)

def get_hosts_with_tag_bk(hosts):
    hosts_port = add_hosts_port(hosts, bk_port)
    return get_hosts_with_tag(broker_tag,'://', hosts_port, newline_tag)

def get_hosts_with_tag_zk(hosts):
    hosts_port = add_hosts_port(hosts, '2888:3888\n')
    return get_hosts_with_tag(zk_tag,'=', hosts_port, newline_tag)

def get_replication_factor(hosts):
    factor = 3 if len(hosts) >= 3 else len(hosts)
    return str(factor)

def get_id(id):
    return id

def uncomment(line):
    return line[1:].lstrip()

# define update functions
update_switcher = {
    # connect-distributed.properties
    'bootstrap.servers=localhost:9092\n': get_hosts,
    'offset.storage.replication.factor=1\n': get_replication_factor,
    'config.storage.replication.factor=1\n': get_replication_factor,
    'status.storage.replication.factor=1\n': get_replication_factor,
    '#rest.port=8083\n': uncomment,
    # server.properties
    'broker.id=0\n': get_id,
    '#listeners=PLAINTEXT://:9092\n': get_hosts_with_tag_bk,
    'offsets.topic.replication.factor=1\n': get_replication_factor,
    'transaction.state.log.replication.factor=1\n': get_replication_factor,
    'zookeeper.connect=localhost:2181\n': get_hosts_zk,
    # kafka-rest.properties
    '#schema.registry.url=http://localhost:8081\n': uncomment,
    'bootstrap.servers=PLAINTEXT://localhost:9092\n': get_hosts_with_tag_bk,
    '# ksql.schema.registry.url=http://localhost:8081\n': uncomment,
}

def create_new_config_file(source, dist, hosts, id):
    dist_file = open(dist, 'w')
    with open(source, 'r') as cfg:
        line = cfg.readline()
        while line:
            update_func = update_switcher.get(line, None)
            if update_func != None:
                strs = line.split('=')
                if update_func.__name__ == 'uncomment':
                    dist_file.write(uncomment(line))
                elif update_func.__name__ == 'get_id':
                    dist_file.write(strs[0] + '=' + update_func(id) + newline_tag)
                else:
                    # check is commented or not
                    if strs[0][:1] == comment_tag:
                        strs[0] = strs[0][1:]
                    dist_file.write(strs[0] + '=' + update_func(hosts) + newline_tag)
            else:
                dist_file.write(line)
            line = cfg.readline()
    dist_file.close()

# /etc/kafka/connect-distributed.properties
# same config file for all nodes
def create_connect(source, dist, hosts):
    create_new_config_file(source, dist, hosts, '')

# /etc/kafka/zookeeper.properties
# same config file for all nodes
def create_zookeeper(source, dist, hosts):
    create_new_config_file(source, dist, hosts, '1')
    with open(dist, 'a') as output:
        # append the following:
        output.write('# multi-node\ntickTime=2000\ninitLimit=5\nsyncLimit=2\n')
        output.write(get_hosts_with_tag_zk(hosts))
        output.write('autopurge.snapRetainCount=3\nautopurge.purgeInterval=24\n')

# /etc/kafka-rest/kafka-rest.properties
# same config for all nodes
def create_kafka_rest(source, dist, hosts):
    create_new_config_file(source, dist, hosts, '')

# /etc/ksqldb/ksql-server.properties
# same config for all nodes
def create_ksqldb(source, dist, hosts):
    create_new_config_file(source, dist, hosts, '')

#================================================================

# /etc/kafka/server.properties
# different config file for each node
def create_kafka(source, dist, hosts):
    for id in range(1, len(hosts)+1):
        create_new_config_file(source, dist + '.' + str(id), hosts, str(id))
        # append default replication factor
        with open(dist + '.' + str(id), 'a') as output:
            output.write('default.replication.factor=1\n')

# /var/lib/zookeeper/myid
# different myid for each node
def create_myid(dist, hosts):
    for id in range(1, len(hosts)+1):
        with open(dist + '.' + str(id), 'w') as output:
            output.write(str(id))

#=============================================================
configs = {
    'connect'   : '/kafka/connect-distributed.properties',
    'kafka'     : '/kafka/server.properties',
    'zookeeper' : '/kafka/zookeeper.properties',
    'kafka_rest': '/kafka-rest/kafka-rest.properties',
    'ksqldb'    : '/ksqldb/ksql-server.properties',
}

create_funcs = {
    'connect'   : create_connect,
    'kafka'     : create_kafka,
    'zookeeper' : create_zookeeper,
    'kafka_rest': create_kafka_rest,
    'ksqldb'    : create_ksqldb,
}

def deploy_configs(hosts):
    # remove host newline
    id = 1
    for host in hosts:
        print('='*rep_cnt + ' start to deploy config files to ' + host)
        for key in configs.keys():
            if key == 'kafka':
                os.system('scp ' + output + configs.get(key) + '.' + str(id) + ' ' + 
                    host.strip() + ':/etc' + configs.get(key))
            else:
                os.system('scp ' + output + configs.get(key) + ' ' + host.strip() + ':/etc' + configs.get(key))
        # copy myid
        os.system('scp ' + output +  '/myid.' + str(id) + ' ' + host.strip() + ':/var/lib/zookeeper/myid')
        id += 1

if __name__ == "__main__":
    # check output
    check_directory(output)
    check_directory(output + '/kafka')
    check_directory(output + '/kafka-rest')
    check_directory(output + '/ksqldb')

    kafka_file = open('./kafka_hosts','r')
    kafka_hosts = kafka_file.readlines()
    kafka_file.close()

    zk_file = open('./zookeeper_hosts', 'r')
    zk_hosts = zk_file.readlines()
    zk_file.close()

    for key in configs.keys():
        func = create_funcs.get(key)
        if key == 'zookeeper':
            current_hosts = zk_hosts
        else:
            current_hosts = kafka_hosts
        print('='*rep_cnt + ' generate: ' + output + configs.get(key))
        func(template + configs.get(key), output + configs.get(key), current_hosts)

    create_myid('./output/myid', kafka_hosts)

    deploy_configs(kafka_hosts)
