package ddlog;

import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import org.junit.Assert;
import org.junit.Test;

public class WeaveTest {
    @Test
    public void testWeave() {
        Translator t = new Translator(null);
        String node_info = "create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null /*primary key*/,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")";
        DDlogIRNode create = t.translateSqlStatement(node_info);
        Assert.assertNotNull(create);

        String pod_info =
                "create table pod_info\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null /*primary key*/,\n" +
                        "  status varchar(36) not null,\n" +
                        "  node_name varchar(36) /*null*/,\n" +
                        "  namespace varchar(100) not null,\n" +
                        "  cpu_request bigint not null,\n" +
                        "  memory_request bigint not null,\n" +
                        "  ephemeral_storage_request bigint not null,\n" +
                        "  pods_request bigint not null,\n" +
                        "  owner_name varchar(100) not null,\n" +
                        "  creation_timestamp varchar(100) not null,\n" +
                        "  priority integer not null,\n" +
                        "  schedulerName varchar(50),\n" +
                        "  has_node_selector_labels boolean not null,\n" +
                        "  has_pod_affinity_requirements boolean not null\n" +
                        ")";
        create = t.translateSqlStatement(pod_info);
        Assert.assertNotNull(create);

        String  pod_ports_request =
                "-- This table tracks the \"ContainerPorts\" fields of each pod.\n" +
                        "-- It is used to enforce the PodFitsHostPorts constraint.\n" +
                        "create table pod_ports_request\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  host_ip varchar(100) not null,\n" +
                        "  host_port integer not null,\n" +
                        "  host_protocol varchar(10) not null\n" +
                        "  /*,foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_ports_request);
        Assert.assertNotNull(create);

        String container_host_ports =
                "-- This table tracks the set of hostports in use at each node.\n" +
                        "-- Also used to enforce the PodFitsHostPorts constraint.\n" +
                        "create table container_host_ports\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  host_ip varchar(100) not null,\n" +
                        "  host_port integer not null,\n" +
                        "  host_protocol varchar(10) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(container_host_ports);
        Assert.assertNotNull(create);

        String pod_node_selector_labels =
                "-- Tracks the set of node selector labels per pod.\n" +
                        "create table pod_node_selector_labels\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  term integer not null,\n" +
                        "  match_expression integer not null,\n" +
                        "  num_match_expressions integer not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(12) not null,\n" +
                        "  label_value varchar(36) /*null*/\n" +
                        "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                        ")";
        create = t.translateSqlStatement(pod_node_selector_labels);
        Assert.assertNotNull(create);

        String pod_affinity_match_expressions =
                "-- Tracks the set of pod affinity match expressions.\n" +
                        "create table pod_affinity_match_expressions\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_selector integer not null,\n" +
                        "  match_expression integer not null,\n" +
                        "  num_match_expressions integer not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(30) not null,\n" +
                        "  label_value varchar(36) not null,\n" +
                        "  topology_key varchar(100) not null\n" +
                        "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                        ")";
        create = t.translateSqlStatement(pod_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_anti_affinity_match_expressions =
                "-- Tracks the set of pod anti-affinity match expressions.\n" +
                        "create table pod_anti_affinity_match_expressions\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(12) not null,\n" +
                        "  label_value varchar(36) not null,\n" +
                        "  topology_key varchar(100) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_anti_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_labels =
                "-- Tracks the set of labels per pod, and indicates if\n" +
                        "-- any of them are also node selector labels\n" +
                        "create table pod_labels\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_labels);
        Assert.assertNotNull(create);

        String node_labels =
                "-- Tracks the set of labels per node\n" +
                        "create table node_labels\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(node_labels);
        Assert.assertNotNull(create);

        String volume_labels =
                "-- Volume labels\n" +
                        "create table volume_labels\n" +
                        "(\n" +
                        "  volume_name varchar(36) not null,\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(volume_labels);
        Assert.assertNotNull(create);

        String pod_by_service =
                "-- For pods that have ports exposed\n" +
                        "create table pod_by_service\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  service_name varchar(100) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_by_service);
        Assert.assertNotNull(create);

        String service_affinity_labels =
                "-- Service affinity labels\n" +
                        "create table service_affinity_labels\n" +
                        "(\n" +
                        "  label_key varchar(100) not null\n" +
                        ")";
        create = t.translateSqlStatement(service_affinity_labels);
        Assert.assertNotNull(create);

        String labels_to_check_for_presence =
                "-- Labels present on node\n" +
                        "create table labels_to_check_for_presence\n" +
                        "(\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  present boolean not null\n" +
                        ")";
        create = t.translateSqlStatement(labels_to_check_for_presence);
        Assert.assertNotNull(create);

        String node_taints =
                "-- Node taints\n" +
                        "create table node_taints\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  taint_key varchar(100) not null,\n" +
                        "  taint_value varchar(100),\n" +
                        "  taint_effect varchar(100) not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(node_taints);
        Assert.assertNotNull(create);

        String pod_taints =
                "-- Pod taints.\n" +
                        "create table pod_tolerations\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  tolerations_key varchar(100),\n" +
                        "  tolerations_value varchar(100),\n" +
                        "  tolerations_effect varchar(100),\n" +
                        "  tolerations_operator varchar(100)/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_taints);
        Assert.assertNotNull(create);

        String node_images =
                "-- Tracks the set of node images that are already\n" +
                        "-- available at a node\n" +
                        "create table node_images\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  image_name varchar(200) not null,\n" +
                        "  image_size bigint not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(node_images);
        Assert.assertNotNull(create);

        String pod_images =
                "-- Tracks the container images required by each pod\n" +
                        "create table pod_images\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  image_name varchar(200) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_images);
        Assert.assertNotNull(create);

        String pods_to_assign_no_limit =
                "-- Select all pods that need to be scheduled.\n" +
                        "-- We also indicate boolean values to check whether\n" +
                        "-- a pod has node selector or pod affinity labels,\n" +
                        "-- and whether pod affinity rules already yields some subset of\n" +
                        "-- nodes that we can assign pods to.\n" +
                        "create view pods_to_assign_no_limit as\n" +
                        "select DISTINCT\n" +
                        "  pod_name,\n" +
                        "  status,\n" +
                        "  node_name as controllable__node_name,\n" +
                        "  namespace,\n" +
                        "  cpu_request,\n" +
                        "  memory_request,\n" +
                        "  ephemeral_storage_request,\n" +
                        "  pods_request,\n" +
                        "  owner_name,\n" +
                        "  creation_timestamp,\n" +
                        "  has_node_selector_labels,\n" +
                        "  has_pod_affinity_requirements\n" +
                        "from pod_info\n" +
                        "where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler'\n" +
                        "-- order by creation_timestamp\n";
        create = t.translateSqlStatement(pods_to_assign_no_limit);
        Assert.assertNotNull(create);

        String batch_size =
                "\n" +
                        "-- This view is updated dynamically to change the limit. This\n" +
                        "-- pattern is required because there is no clean way to enforce\n" +
                        "-- a dynamic \"LIMIT\" clause.\n" +
                        "create table batch_size\n" +
                        "(\n" +
                        "  pendingPodsLimit integer not null --primary key\n" +
                        ")";
        create = t.translateSqlStatement(batch_size);
        Assert.assertNotNull(create);

        String pods_to_assign =
                "create view pods_to_assign as\n" +
                        "select DISTINCT * from pods_to_assign_no_limit -- limit 100\n";
        create = t.translateSqlStatement(pods_to_assign);
        Assert.assertNotNull(create);

        String pods_with_port_request =
                "-- Pods with port requests\n" +
                        "create view pods_with_port_requests as\n" +
                        "select DISTINCT pods_to_assign.controllable__node_name as controllable__node_name,\n" +
                        "       pod_ports_request.host_port as host_port,\n" +
                        "       pod_ports_request.host_ip as host_ip,\n" +
                        "       pod_ports_request.host_protocol as host_protocol\n" +
                        "from pods_to_assign\n" +
                        "join pod_ports_request\n" +
                        "     on pod_ports_request.pod_name = pods_to_assign.pod_name";
        create = t.translateSqlStatement(pods_with_port_request);
        Assert.assertNotNull(create);

        String pod_node_selectors =
                "-- Pod node selectors\n" +
                "create view pod_node_selector_matches as\n" +
                "select DISTINCT pods_to_assign.pod_name as pod_name,\n" +
                "       node_labels.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_node_selector_labels\n" +
                "     on pods_to_assign.pod_name = pod_node_selector_labels.pod_name\n" +
                "join node_labels\n" +
                "        on\n" +
                "           (pod_node_selector_labels.label_operator = 'In'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "            and pod_node_selector_labels.label_value = node_labels.label_value)\n" +
                "        or (pod_node_selector_labels.label_operator = 'Exists'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key)\n" +
                "        or (pod_node_selector_labels.label_operator = 'NotIn')\n" +
                "        or (pod_node_selector_labels.label_operator = 'DoesNotExist')\n" +
                "group by pods_to_assign.pod_name,  node_labels.node_name, pod_node_selector_labels.term,\n" +
                "         pod_node_selector_labels.label_operator, pod_node_selector_labels.num_match_expressions\n" +
                "having case pod_node_selector_labels.label_operator\n" +
                "            when 'NotIn'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "                              and pod_node_selector_labels.label_value = node_labels.label_value))\n" +
                "            when 'DoesNotExist'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key))\n" +
                "            else count(distinct match_expression) = pod_node_selector_labels.num_match_expressions\n" +
                "       end";
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

        /*
        // indexes are ignored
        String indexes =
                "create index pod_info_idx on pod_info (status, node_name);\n" +
                "create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_name);\n" +
                "create index node_labels_idx on node_labels (label_key, label_value);\n" +
                "create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);\n" +
                "create index pod_labels_idx on pod_labels (label_key, label_value);\n";
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);
        */

        String inter_pod_affinity =
                "-- Inter pod affinity\n" +
                "create view inter_pod_affinity_matches_inner as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       pod_labels.pod_name as matches,\n" +
                "       pod_info.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_affinity_match_expressions\n" +
                "     on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name\n" +
                "join pod_labels\n" +
                "        on (pod_affinity_match_expressions.label_operator = 'In'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "            and pod_affinity_match_expressions.label_value = pod_labels.label_value)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'Exists'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'NotIn')\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'DoesNotExist')\n" +
                "join pod_info\n" +
                "        on pod_labels.pod_name = pod_info.pod_name\n" +
                "group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_affinity_match_expressions.label_selector,\n" +
                "         pod_affinity_match_expressions.topology_key, pod_affinity_match_expressions.label_operator,\n" +
                "         pod_affinity_match_expressions.num_match_expressions, pod_info.node_name\n" +
                "having case pod_affinity_match_expressions.label_operator\n" +
                "             when 'NotIn'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "                               and pod_affinity_match_expressions.label_value = pod_labels.label_value))\n" +
                "             when 'DoesNotExist'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key))\n" +
                "             else count(distinct match_expression) = pod_affinity_match_expressions.num_match_expressions\n" +
                "       end";
        create = t.translateSqlStatement(inter_pod_affinity);
        Assert.assertNotNull(create);

        /*
        String inter_pod_affinity_matches =
                "create view inter_pod_affinity_matches as\n" +
                "select *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner";
        create = t.translateSqlStatement(inter_pod_affinity_matches);
        Assert.assertNotNull(create);
        */
        String spare_capacity =
                "-- Spare capacity\n" +
                "create view spare_capacity_per_node as\n" +
                "select node_info.name as name,\n" +
                "       cast(node_info.cpu_allocatable - sum(pod_info.cpu_request) as integer) as cpu_remaining,\n" +
                "       cast(node_info.memory_allocatable - sum(pod_info.memory_request) as integer) as memory_remaining,\n" +
                "       cast(node_info.pods_allocatable - sum(pod_info.pods_request) as integer) as pods_remaining\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'\n" +
                "group by node_info.name, node_info.cpu_allocatable,\n" +
                "         node_info.memory_allocatable, node_info.pods_allocatable";
        create = t.translateSqlStatement(spare_capacity);
        Assert.assertNotNull(create);

        /*
        String view_pods_that_tolerate_node_taints =
                "-- Taints and tolerations\n" +
                "create view pods_that_tolerate_node_taints as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       A.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_tolerations\n" +
                "     on pods_to_assign.pod_name = pod_tolerations.pod_name\n" +
                "join (select *, count(*) over (partition by node_name) as num_taints from node_taints) as A\n" +
                "     on pod_tolerations.tolerations_key = A.taint_key\n" +
                "     and (pod_tolerations.tolerations_effect = null\n" +
                "          or pod_tolerations.tolerations_effect = A.taint_effect)\n" +
                "     and (pod_tolerations.tolerations_operator = 'Exists'\n" +
                "          or pod_tolerations.tolerations_value = A.taint_value)\n" +
                "group by pod_tolerations.pod_name, A.node_name, A.num_taints\n" +
                "having count(*) = A.num_taints";
        create = t.translateSqlStatement(view_pods_that_tolerate_node_taints);
        Assert.assertNotNull(create);
        */

        String nodes_that_have_tollerations =
                "create view nodes_that_have_tolerations as\n" +
                        "select distinct node_name from node_taints";
        create = t.translateSqlStatement(nodes_that_have_tollerations);
        Assert.assertNotNull(create);

        DDlogProgram program = t.getDDlogProgram();
        String p = program.toString();
        Assert.assertNotNull(p);
        String expected = "import sql\nimport sqlop\n" + "\n" +
                "typedef Tnode_info = Tnode_info{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, " +
                "memory_capacity:bigint, ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint}\n" +

                "typedef Tpod_info = Tpod_info{pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, " +
                "owner_name:string, creation_timestamp:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +

                "typedef Tpod_ports_request = Tpod_ports_request{pod_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +

                "typedef Tcontainer_host_ports = Tcontainer_host_ports{pod_name:string, node_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +

                "typedef Tpod_node_selector_labels = Tpod_node_selector_labels{pod_name:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, " +
                "label_value:Option<string>}\n" +

                "typedef Tpod_affinity_match_expressions = Tpod_affinity_match_expressions{pod_name:string, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, " +
                "label_operator:string, label_value:string, topology_key:string}\n" +

                "typedef Tpod_anti_affinity_match_expressions = Tpod_anti_affinity_match_expressions{pod_name:string, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +

                "typedef Tpod_labels = Tpod_labels{pod_name:string, label_key:string, label_value:string}\n" +

                "typedef Tnode_labels = Tnode_labels{node_name:string, label_key:string, label_value:string}\n" +

                "typedef Tvolume_labels = Tvolume_labels{volume_name:string, pod_name:string, label_key:string, label_value:string}\n" +

                "typedef Tpod_by_service = Tpod_by_service{pod_name:string, service_name:string}\n" +

                "typedef Tservice_affinity_labels = Tservice_affinity_labels{label_key:string}\n" +

                "typedef Tlabels_to_check_for_presence = Tlabels_to_check_for_presence{label_key:string, present:bool}\n" +

                "typedef Tnode_taints = Tnode_taints{node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string}\n" +

                "typedef Tpod_tolerations = Tpod_tolerations{pod_name:string, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>}\n" +

                "typedef Tnode_images = Tnode_images{node_name:string, image_name:string, image_size:bigint}\n" +

                "typedef Tpod_images = Tpod_images{pod_name:string, image_name:string}\n" +

                "typedef TRtmp = TRtmp{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, " +
                "owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +

                "typedef Tbatch_size = Tbatch_size{pendingPodsLimit:signed<64>}\n" +

                "typedef Ttmp = Ttmp{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, " +
                "owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +

                "typedef TRtmp1 = TRtmp1{controllable__node_name:Option<string>, host_port:signed<64>, host_ip:string, host_protocol:string}\n" +

                "typedef Ttmp2 = Ttmp2{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, " +
                "owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, " +
                "label_key:string, label_operator:string, label_value:Option<string>}\n" +

                "typedef Ttmp3 = Ttmp3{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, " +
                "owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, " +
                "label_key:string, label_operator:string, label_value:Option<string>, node_name:string, label_key0:string, label_value0:string}\n" +

                "typedef TRtmp4 = TRtmp4{pod_name:string, node_name:string}\n" +

                "typedef Tagg = Tagg{col:bool}\n" +

                "typedef Ttmp5 = Ttmp5{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, " +
                "pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, label_selector:signed<64>, " +
                "match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +

                "typedef Ttmp6 = Ttmp6{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, " +
                "pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, label_selector:signed<64>, " +
                "match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string, pod_name1:string, label_key0:string, label_value0:string}\n" +

                "typedef Ttmp7 = Ttmp7{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, " +
                "pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, label_selector:signed<64>, match_expression:signed<64>, " +
                "num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string, pod_name1:string, label_key0:string, label_value0:string, pod_name2:string, status0:string, " +
                "node_name:Option<string>, namespace0:string, cpu_request0:bigint, memory_request0:bigint, ephemeral_storage_request0:bigint, pods_request0:bigint, owner_name0:string, creation_timestamp0:string, priority:signed<64>, " +
                "schedulerName:Option<string>, has_node_selector_labels0:bool, has_pod_affinity_requirements0:bool}\n" +

                "typedef TRtmp8 = TRtmp8{pod_name:string, matches:string, node_name:Option<string>}\n" +

                "typedef Tagg9 = Tagg9{col:bool}\n" +

                "typedef Ttmp10 = Ttmp10{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, memory_capacity:bigint, " +
                "ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint, pod_name:string, status:string, " +
                "node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, priority:signed<64>, " +
                "schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +

                "typedef TRtmp11 = TRtmp11{name:string, cpu_remaining:signed<64>, memory_remaining:signed<64>, pods_remaining:signed<64>}\n" +

                "typedef Tagg12 = Tagg12{cpu_remaining:signed<64>, memory_remaining:signed<64>, pods_remaining:signed<64>}\n" +

                "typedef TRtmp13 = TRtmp13{node_name:string}\n" +

                "function agg(g: Group<(string, string, signed<64>, string, signed<64>), (TRtmp, Tpod_node_selector_labels, Tnode_labels)>):Tagg =\n" +
                "(var gb, var gb4, var gb5, var gb6, var gb7) = group_key(g);\n" +
                "(var any = false: bool);\n" +
                "(var any9 = false: bool);\n" +
                "(var count_distinct = set_empty(): Set<signed<64>>);\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var v2 = i.2);\n" +
                "(var incr = b_and_RN((v0.label_key == v2.label_key), s_eq_NR(v0.label_value, v2.label_value)));\n" +
                "(any = agg_any_N(any, incr));\n" +
                "(var incr8 = (v0.label_key == v2.label_key));\n" +
                "(any9 = agg_any_R(any9, incr8));\n" +
                "(var incr10 = v0.match_expression);\n" +
                "(set_insert(count_distinct, incr10))}\n" +
                ");\n" +
                "(Tagg{.col = if (gb6 == \"NotIn\") {\n" +
                "(not any)} else {\n" +
                "if (gb6 == \"DoesNotExist\") {\n" +
                "(not any9)} else {\n" +
                "(set_size(count_distinct) as signed<64> == gb7)}}})\n" +

                "function agg9(g: Group<(string, string, signed<64>, string, string, signed<64>, Option<string>), (TRtmp, Tpod_affinity_match_expressions, Tpod_labels, Tpod_info)>):Tagg9 =\n" +
                "(var gb, var gb6, var gb7, var gb8, var gb9, var gb10, var gb11) = group_key(g);\n" +
                "(var any = false: bool);\n" +
                "(var any13 = false: bool);\n" +
                "(var count_distinct = set_empty(): Set<signed<64>>);\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var v2 = i.2);\n" +
                "(var v4 = i.3);\n" +
                "(var incr = ((v0.label_key == v2.label_key) and (v0.label_value == v2.label_value)));\n" +
                "(any = agg_any_R(any, incr));\n" +
                "(var incr12 = (v0.label_key == v2.label_key));\n" +
                "(any13 = agg_any_R(any13, incr12));\n" +
                "(var incr14 = v0.match_expression);\n" +
                "(set_insert(count_distinct, incr14))}\n" +
                ");\n" +
                "(Tagg9{.col = if (gb9 == \"NotIn\") {\n" +
                "(not any)} else {\n" +
                "if (gb9 == \"DoesNotExist\") {\n" +
                "(not any13)} else {\n" +
                "(set_size(count_distinct) as signed<64> == gb10)}}})\n" +

                "function agg12(g: Group<(string, bigint, bigint, bigint), (Tnode_info, Tpod_info)>):Tagg12 =\n" +
                "(var gb, var gb2, var gb3, var gb4) = group_key(g);\n" +
                "(var sum = 0: bigint);\n" +
                "(var sum6 = 0: bigint);\n" +
                "(var sum8 = 0: bigint);\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var incr = v0.cpu_request);\n" +
                "(sum = agg_sum_int_R(sum, incr));\n" +
                "(var incr5 = v0.memory_request);\n" +
                "(sum6 = agg_sum_int_R(sum6, incr5));\n" +
                "(var incr7 = v0.pods_request);\n" +
                "(sum8 = agg_sum_int_R(sum8, incr7))}\n" +
                ");\n" +
                "(Tagg12{.cpu_remaining = (gb2 - sum) as signed<64>,.memory_remaining = (gb3 - sum6) as signed<64>,.pods_remaining = (gb4 - sum8) as signed<64>})" +
                "\n" +

                "input relation Rnode_info[Tnode_info]\n" +
                "input relation Rpod_info[Tpod_info]\n" +
                "input relation Rpod_ports_request[Tpod_ports_request]\n" +
                "input relation Rcontainer_host_ports[Tcontainer_host_ports]\n" +
                "input relation Rpod_node_selector_labels[Tpod_node_selector_labels]\n" +
                "input relation Rpod_affinity_match_expressions[Tpod_affinity_match_expressions]\n" +
                "input relation Rpod_anti_affinity_match_expressions[Tpod_anti_affinity_match_expressions]\n" +
                "input relation Rpod_labels[Tpod_labels]\n" +
                "input relation Rnode_labels[Tnode_labels]\n" +
                "input relation Rvolume_labels[Tvolume_labels]\n" +
                "input relation Rpod_by_service[Tpod_by_service]\n" +
                "input relation Rservice_affinity_labels[Tservice_affinity_labels]\n" +
                "input relation Rlabels_to_check_for_presence[Tlabels_to_check_for_presence]\n" +
                "input relation Rnode_taints[Tnode_taints]\n" +
                "input relation Rpod_tolerations[Tpod_tolerations]\n" +
                "input relation Rnode_images[Tnode_images]\n" +
                "input relation Rpod_images[Tpod_images]\n" +

                "relation Rtmp[TRtmp]\n" +
                "output relation Rpods_to_assign_no_limit[TRtmp]\n" +
                "input relation Rbatch_size[Tbatch_size]\n" +
                "output relation Rpods_to_assign[TRtmp]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "output relation Rpods_with_port_requests[TRtmp1]\n" +
                "relation Rtmp4[TRtmp4]\n" +
                "output relation Rpod_node_selector_matches[TRtmp4]\n" +
                "relation Rtmp8[TRtmp8]\n" +
                "output relation Rinter_pod_affinity_matches_inner[TRtmp8]\n" +
                "relation Rtmp11[TRtmp11]\n" +
                "output relation Rspare_capacity_per_node[TRtmp11]\n" +
                "relation Rtmp13[TRtmp13]\n" +
                "output relation Rnodes_that_have_tolerations[TRtmp13]\n" +

                "Rpods_to_assign_no_limit[v1] :- Rpod_info[v],unwrapBool(b_and_RN(((v.status == \"Pending\") and is_null(v.node_name))," +
                " s_eq_NR(v.schedulerName, \"dcm-scheduler\")))," +
                "var v0 = TRtmp{.pod_name = v.pod_name,.status = v.status,.controllable__node_name = v.node_name,.namespace = v.namespace,.cpu_request = v.cpu_request,.memory_request = v.memory_request," +
                ".ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp,.has_node_selector_labels = v.has_node_selector_labels," +
                ".has_pod_affinity_requirements = v.has_pod_affinity_requirements}," +
                "var v1 = v0.\n" +

                "Rpods_to_assign[v0] :- Rpods_to_assign_no_limit[v],var v0 = v.\n" +

                "Rpods_with_port_requests[v3] :- Rpods_to_assign[v],Rpod_ports_request[v0]," +
                "(v0.pod_name == v.pod_name),true,var v1 = Ttmp{.pod_name = v.pod_name,.status = v.status,.controllable__node_name = v.controllable__node_name,.namespace = v.namespace,.cpu_request = v.cpu_request," +
                ".memory_request = v.memory_request,.ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp," +
                ".has_node_selector_labels = v.has_node_selector_labels,.has_pod_affinity_requirements = v.has_pod_affinity_requirements,.pod_name0 = v0.pod_name,.host_ip = v0.host_ip,.host_port = v0.host_port,.host_protocol = v0.host_protocol}," +
                "var v2 = TRtmp1{.controllable__node_name = v.controllable__node_name,.host_port = v0.host_port,.host_ip = v0.host_ip,.host_protocol = v0.host_protocol},var v3 = v2.\n" +

                "Rpod_node_selector_matches[v12] :- Rpods_to_assign[v],Rpod_node_selector_labels[v0],(v.pod_name == v0.pod_name),true," +
                "var v1 = Ttmp2{.pod_name = v.pod_name,.status = v.status,.controllable__node_name = v.controllable__node_name,.namespace = v.namespace,.cpu_request = v.cpu_request,.memory_request = v.memory_request," +
                ".ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp,.has_node_selector_labels = v.has_node_selector_labels," +
                ".has_pod_affinity_requirements = v.has_pod_affinity_requirements,.pod_name0 = v0.pod_name,.term = v0.term,.match_expression = v0.match_expression,.num_match_expressions = v0.num_match_expressions," +
                ".label_key = v0.label_key,.label_operator = v0.label_operator,.label_value = v0.label_value},Rnode_labels[v2]," +
                "unwrapBool(b_or_NR(b_or_NR(b_or_NR(b_and_RN(((v0.label_operator == \"In\") and (v0.label_key == v2.label_key)), s_eq_NR(v0.label_value, v2.label_value)), ((v0.label_operator == \"Exists\") and (v0.label_key == v2.label_key)))," +
                " (v0.label_operator == \"NotIn\"))," +
                " (v0.label_operator == \"DoesNotExist\"))),true,var v3 = Ttmp3{.pod_name = v1.pod_name,.status = v1.status,.controllable__node_name = v1.controllable__node_name,.namespace = v1.namespace,.cpu_request = v1.cpu_request," +
                ".memory_request = v1.memory_request,.ephemeral_storage_request = v1.ephemeral_storage_request,.pods_request = v1.pods_request,.owner_name = v1.owner_name,.creation_timestamp = v1.creation_timestamp," +
                ".has_node_selector_labels = v1.has_node_selector_labels,.has_pod_affinity_requirements = v1.has_pod_affinity_requirements,.pod_name0 = v1.pod_name0,.term = v1.term,.match_expression = v1.match_expression," +
                ".num_match_expressions = v1.num_match_expressions,.label_key = v1.label_key,.label_operator = v1.label_operator,.label_value = v1.label_value,.node_name = v2.node_name,.label_key0 = v2.label_key," +
                ".label_value0 = v2.label_value},var gb = v.pod_name,var gb4 = v2.node_name,var gb5 = v0.term,var gb6 = v0.label_operator,var gb7 = v0.num_match_expressions," +
                "var aggResult = Aggregate((gb, gb4, gb5, gb6, gb7), agg((v, v0, v2))),var v11 = TRtmp4{.pod_name = gb,.node_name = gb4},aggResult.col,var v12 = v11.\n" +

                "Rinter_pod_affinity_matches_inner[v16] :- Rpods_to_assign[v],Rpod_affinity_match_expressions[v0],(v.pod_name == v0.pod_name),true," +
                "var v1 = Ttmp5{.pod_name = v.pod_name,.status = v.status,.controllable__node_name = v.controllable__node_name,.namespace = v.namespace,.cpu_request = v.cpu_request,.memory_request = v.memory_request," +
                ".ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp,.has_node_selector_labels = v.has_node_selector_labels," +
                ".has_pod_affinity_requirements = v.has_pod_affinity_requirements,.pod_name0 = v0.pod_name,.label_selector = v0.label_selector,.match_expression = v0.match_expression,.num_match_expressions = v0.num_match_expressions," +
                ".label_key = v0.label_key,.label_operator = v0.label_operator,.label_value = v0.label_value,.topology_key = v0.topology_key},Rpod_labels[v2]," +
                "((((((v0.label_operator == \"In\") and (v0.label_key == v2.label_key)) and (v0.label_value == v2.label_value)) or ((v0.label_operator == \"Exists\") and (v0.label_key == v2.label_key))) or (v0.label_operator == \"NotIn\")) " +
                "or (v0.label_operator == \"DoesNotExist\")),true," +
                "var v3 = Ttmp6{.pod_name = v1.pod_name,.status = v1.status,.controllable__node_name = v1.controllable__node_name,.namespace = v1.namespace,.cpu_request = v1.cpu_request,.memory_request = v1.memory_request," +
                ".ephemeral_storage_request = v1.ephemeral_storage_request,.pods_request = v1.pods_request,.owner_name = v1.owner_name,.creation_timestamp = v1.creation_timestamp,.has_node_selector_labels = v1.has_node_selector_labels," +
                ".has_pod_affinity_requirements = v1.has_pod_affinity_requirements,.pod_name0 = v1.pod_name0,.label_selector = v1.label_selector,.match_expression = v1.match_expression,.num_match_expressions = v1.num_match_expressions," +
                ".label_key = v1.label_key,.label_operator = v1.label_operator,.label_value = v1.label_value,.topology_key = v1.topology_key,.pod_name1 = v2.pod_name,.label_key0 = v2.label_key,.label_value0 = v2.label_value}," +

                "Rpod_info[v4],(v2.pod_name == v4.pod_name),true,var v5 = Ttmp7{.pod_name = v3.pod_name,.status = v3.status,.controllable__node_name = v3.controllable__node_name,.namespace = v3.namespace,.cpu_request = v3.cpu_request," +
                ".memory_request = v3.memory_request,.ephemeral_storage_request = v3.ephemeral_storage_request,.pods_request = v3.pods_request,.owner_name = v3.owner_name,.creation_timestamp = v3.creation_timestamp," +
                ".has_node_selector_labels = v3.has_node_selector_labels,.has_pod_affinity_requirements = v3.has_pod_affinity_requirements,.pod_name0 = v3.pod_name0,.label_selector = v3.label_selector," +
                ".match_expression = v3.match_expression,.num_match_expressions = v3.num_match_expressions,.label_key = v3.label_key,.label_operator = v3.label_operator,.label_value = v3.label_value,.topology_key = v3.topology_key," +
                ".pod_name1 = v3.pod_name1,.label_key0 = v3.label_key0,.label_value0 = v3.label_value0,.pod_name2 = v4.pod_name,.status0 = v4.status,.node_name = v4.node_name,.namespace0 = v4.namespace,.cpu_request0 = v4.cpu_request," +
                ".memory_request0 = v4.memory_request,.ephemeral_storage_request0 = v4.ephemeral_storage_request,.pods_request0 = v4.pods_request,.owner_name0 = v4.owner_name,.creation_timestamp0 = v4.creation_timestamp," +
                ".priority = v4.priority,.schedulerName = v4.schedulerName,.has_node_selector_labels0 = v4.has_node_selector_labels,.has_pod_affinity_requirements0 = v4.has_pod_affinity_requirements}," +
                "var gb = v.pod_name,var gb6 = v2.pod_name,var gb7 = v0.label_selector,var gb8 = v0.topology_key,var gb9 = v0.label_operator,var gb10 = v0.num_match_expressions,var gb11 = v4.node_name," +
                "var aggResult = Aggregate((gb, gb6, gb7, gb8, gb9, gb10, gb11), agg9((v, v0, v2, v4))),var v15 = TRtmp8{.pod_name = gb,.matches = gb6,.node_name = gb11},aggResult.col,var v16 = v15.\n" +

                "Rspare_capacity_per_node[v10] :- Rnode_info[v],Rpod_info[v0],unwrapBool(b_and_NN(s_eq_NR(v0.node_name, v.name), " +
                "s_neq_NR(v0.node_name, \"null\"))),true," +
                "var v1 = Ttmp10{.name = v.name,.unschedulable = v.unschedulable,.out_of_disk = v.out_of_disk," +
                ".memory_pressure = v.memory_pressure,.disk_pressure = v.disk_pressure,.pid_pressure = v.pid_pressure," +
                ".ready = v.ready,.network_unavailable = v.network_unavailable,.cpu_capacity = v.cpu_capacity," +
                ".memory_capacity = v.memory_capacity,.ephemeral_storage_capacity = v.ephemeral_storage_capacity," +
                ".pods_capacity = v.pods_capacity,.cpu_allocatable = v.cpu_allocatable,.memory_allocatable = v.memory_allocatable," +
                ".ephemeral_storage_allocatable = v.ephemeral_storage_allocatable,.pods_allocatable = v.pods_allocatable," +
                ".pod_name = v0.pod_name,.status = v0.status,.node_name = v0.node_name,.namespace = v0.namespace," +
                ".cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request," +
                ".pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp," +
                ".priority = v0.priority,.schedulerName = v0.schedulerName,.has_node_selector_labels = v0.has_node_selector_labels," +
                ".has_pod_affinity_requirements = v0.has_pod_affinity_requirements}," +
                "var gb = v.name,var gb2 = v.cpu_allocatable,var gb3 = v.memory_allocatable,var gb4 = v.pods_allocatable," +
                "var aggResult = Aggregate((gb, gb2, gb3, gb4), agg12((v, v0)))," +
                "var v9 = TRtmp11{.name = gb,.cpu_remaining = aggResult.cpu_remaining,.memory_remaining = aggResult.memory_remaining," +
                ".pods_remaining = aggResult.pods_remaining},var v10 = v9.\n" +
                "Rnodes_that_have_tolerations[v1] :- Rnode_taints[v],var v0 = TRtmp13{.node_name = v.node_name},var v1 = v0.";
        Assert.assertEquals(expected, p);
    }
}