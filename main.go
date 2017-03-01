package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// ClusterNodeAdvert is stored by self_advertise_node.sh on container
type ClusterNodeAdvert struct {
	CellGUID string `json:"cell_guid"`
	NodeID   string `json:"node_id"`
	State    string `json:"state"`
	Role     string `json:"role"`
}

// ClusterNodeStatus is the change notification data sent to Hub API
type ClusterNodeStatus struct {
	Cluster string `json:"cluster"`
	Node    string `json:"node"`
	State   string `json:"state"`
	Role    string `json:"role"`
}

// TODO: on start, do bulk update of status for all clusters; then start polling
func main() {
	errors := false
	etcdURL, err := url.Parse(os.Getenv("ETCD_URI"))
	if err != nil {
		log.Fatal(err)
	}
	if etcdURL.Host == "" {
		fmt.Fprintf(os.Stderr, "Missing or malformed $ETCD_URI: '%s'\n", os.Getenv("ETCD_URI"))
		errors = true
	} else {
		fmt.Printf("etcd: %s://%s%s\n", etcdURL.Scheme, etcdURL.Host, etcdURL.Path)
	}

	hubURL, err := url.Parse(os.Getenv("HUB_URI"))
	if err != nil {
		log.Fatal(err)
	}
	if hubURL.Host == "" {
		fmt.Fprintf(os.Stderr, "Missing or malformed $HUB_URI: '%s'\n", os.Getenv("HUB_URI"))
		errors = true
	} else {
		fmt.Printf("hub: %s://%s%s\n", hubURL.Scheme, hubURL.Host, hubURL.Path)
	}

	watcherRootPath := os.Getenv("WATCH_ROOT_PATH")
	if watcherRootPath == "" {
		watcherRootPath = "/service/"
	}
	fmt.Println("watch:", watcherRootPath)

	if errors {
		os.Exit(1)
	}

	host := fmt.Sprintf("%s://%s", etcdURL.Scheme, etcdURL.Host)
	peers := []string{host}

	// caCertPool := x509.NewCertPool()
	//
	// tlsConfig := &tls.Config{
	// 	RootCAs: caCertPool,
	// }
	//
	// transport := &http.Transport{TLSClientConfig: tlsConfig}
	password, _ := etcdURL.User.Password()
	cfg := client.Config{
		Endpoints: peers,
		// Transport:               transport,
		HeaderTimeoutPerRequest: time.Minute,
		Username:                etcdURL.User.Username(),
		Password:                password,
	}

	// And create your client as normal.
	etcdclient, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	kapi := client.NewKeysAPI(etcdclient)

	watcher := kapi.Watcher(watcherRootPath, &client.WatcherOptions{Recursive: true})

	node := regexp.MustCompile("/service/(.*)/nodes/(.*)$")
	clusterNodes := map[string]ClusterNodeStatus{}

	fmt.Printf("Watching '%s' for '%v'...\n", watcherRootPath, node)
	for true {
		resp, err := watcher.Next(context.TODO())

		if err != nil {
			if _, ok := err.(*client.ClusterError); ok {
				fmt.Fprintf(os.Stderr, err.Error())
				continue
			}
			log.Fatal(err)
		}

		// fmt.Println(resp.Action + ": " + resp.Node.Key + " " + resp.Node.Value)

		matches := node.FindStringSubmatch(resp.Node.Key)
		if len(matches) == 3 {
			if resp.Action == "expire" {
				// Node has stopped advertising itself
				go func() {
					status := ClusterNodeStatus{matches[1], matches[2], "", ""}
					publishMissing(hubURL, status)
				}()
				delete(clusterNodes, resp.Node.Key)
			} else {
				// Node is advertising itself
				advert := &ClusterNodeAdvert{}
				if err := json.NewDecoder(strings.NewReader(resp.Node.Value)).Decode(advert); err != nil {
					fmt.Fprintf(os.Stderr, err.Error())
				}

				status := ClusterNodeStatus{
					matches[1],
					matches[2],
					advert.State,
					advert.Role,
				}
				changed := true
				if old, ok := clusterNodes[resp.Node.Key]; ok {
					changed = old != status
				}
				clusterNodes[resp.Node.Key] = status
				if changed {
					go func() {
						if err := publishChange(hubURL, status); err != nil {
							fmt.Fprintf(os.Stderr, err.Error())
						}
					}()
				}
			}
		}

	}

}

func publishMissing(hubURL *url.URL, status ClusterNodeStatus) error {
	status.State = "missing"
	return publishChange(hubURL, status)
}

func publishChange(hubURL *url.URL, status ClusterNodeStatus) error {
	fmt.Println("changed:", status)
	// How to POST PUT?
	hubNotificationURL := fmt.Sprintf("%s/watcher/clusters/%s/nodes/%s", hubURL, status.Cluster, status.Node)
	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(status)
	resp, err := netClient.Post(hubNotificationURL, "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "Failed updating %s, got status code %d\n", hubNotificationURL, resp.StatusCode)
	}

	return nil
}

// _, server := path.Split(resp.Node.Key)
// switch resp.Action {
// case "create":
// 	fmt.Println(server + " has started heart beat")
// case "compareAndSwap":
// 	fmt.Println(server + " heart beat")
// case "compareAndDelete":
// 	fmt.Println(server + " has shut down correctly")
// case "expire":
// 	fmt.Println("*** " + server + " has missed heartbeat")
// case "set":
// 	fmt.Println(server + " set")
// default:
// 	fmt.Println("Didn't handle " + resp.Action)
// }

// 1-cluster 1-node
// /service/dingo-test-5/nodes/54-159-121-202-5000 {"cell_guid":"54.159.121.202","node_id":"54-159-121-202-5000","state":"running","role":"master"}
// 54-159-121-202-5000 set
// /service/dingo-test-5/leader 54-159-121-202-5000
// leader heart beat
// /service/dingo-test-5/nodes/54-159-121-202-5000 {"cell_guid":"54.159.121.202","node_id":"54-159-121-202-5000","state":"stopping","role":"master"}
// 54-159-121-202-5000 set
// /service/dingo-test-5/members/54-159-121-202-5000 {"role":"replica","api_url":"http://54.159.121.202:8008/patroni","conn_url":"postgres://54.159.121.202:5000/postgres","xlog_location":385876120,"state":"running"}
// 54-159-121-202-5000 set

// No clusters
// /service/dingo-test-5/nodes/54-159-121-202-5000
// *** 54-159-121-202-5000 has missed heartbeat
// /service/dingo-test-5/leader
// *** leader has missed heartbeat

// Node has stopped running
// /service/dingo-test-5/nodes/54-159-121-202-5000
// *** 54-159-121-202-5000 has missed heartbeat
// /service/dingo-test-5/members/54-159-121-202-5000
// *** 54-159-121-202-5000 has missed heartbeat

// First cluster starts running
// set: /service/dingo-test-7/nodes/54-159-121-202-5000 {"cell_guid":"54.159.121.202","node_id":"54-159-121-202-5000","state":"api-not-available"}
// set: /service/dingo-test-7/members/54-159-121-202-5000 {"api_url":"http://54.159.121.202:8008/patroni","conn_url":"postgres://54.159.121.202:5000/postgres","role":"uninitialized","state":"stopped"}
// create: /service/dingo-test-7/initialize
// update: /service/dingo-test-7/initialize 6388777380876334280
// set: /service/dingo-test-7/config {"retry_timeout":10,"ttl":30,"postgresql":{"parameters":{"archive_timeout":"10min","hot_standby":"on","wal_keep_segments":8,"archive_command":"/scripts/archive_command.sh \"%p\" \"%f\"","wal_log_hints":"on","max_wal_senders":5,"max_replication_slots":5,"wal_level":"hot_standby","archive_mode":"on"},"use_slots":true,"use_pg_rewind":false,"recovery_conf":{"restore_command":"/scripts/restore_command.sh \"%p\" \"%f\""}},"maximum_lag_on_failover":1048576,"loop_wait":10}
// set: /service/dingo-test-7/leader 54-159-121-202-5000
// set: /service/dingo-test-7/members/54-159-121-202-5000 {"api_url":"http://54.159.121.202:8008/patroni","conn_url":"postgres://54.159.121.202:5000/postgres","xlog_location":26931280,"role":"master","state":"running"}
// set: /service/dingo-test-7/nodes/54-159-121-202-5000 {"cell_guid":"54.159.121.202","node_id":"54-159-121-202-5000","state":"running","role":"master"}
// compareAndSwap: /service/dingo-test-7/leader 54-159-121-202-5000
// set: /service/dingo-test-7/nodes/54-159-121-202-5000 {"cell_guid":"54.159.121.202","node_id":"54-159-121-202-5000","state":"running","role":"master"}
// set: /service/dingo-test-7/optime/leader 33554432

// Delete a node and recreate it as leader
// /service/dingo-test-7/nodes/54-159-121-202-5000 disappeared
// {dingo-test-7 54-159-121-202-5000 api-not-available } changed: false
// {dingo-test-7 54-159-121-202-5000 creating replica uninitialized} changed: true
// {dingo-test-7 54-159-121-202-5000 starting replica} changed: true
// {dingo-test-7 54-159-121-202-5000 starting replica} changed: false
// {dingo-test-7 54-159-121-202-5000 starting replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: true
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running replica} changed: false
// {dingo-test-7 54-159-121-202-5000 running master} changed: true
// {dingo-test-7 54-159-121-202-5000 running master} changed: false
