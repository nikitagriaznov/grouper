package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// To avoid multiple requests to database local cache will be used
// It's consist of list of transactions and list of clusters
type db_cache struct {
	transaction list_of_transactions
	cluster     list_of_clusters
}

// Object is a uniq id of uint32 type. id=0 is invalid
// List of objects is consist of a curtain number of objects
// List of objects is a map where index is an id of object and value is a quantity of objects of selected id in selected list
type list_of_objects map[uint32]uint32

// Transaction is a group of objects with uniq id. id=0 is invalid
// Cluster is a group of transactions with uniq id. A transaction can be a part of only one cluster
// List of transactions consist list of objects that is a part of a transaction, id of cluster to which it belongs to
// and total number of all objects that transaction consist
type single_transaction struct {
	cluster uint32 // cluster id
	object  list_of_objects
}

// Index if the map is an id of transaction
type list_of_transactions map[uint32]single_transaction

// To speedup our calculations we will save total number of uniq objects & number of transactions in cluster.
type single_cluster struct {
	uniq_objects  float32 // number of uniq objects in cluster
	total_objects float32 //total number of objects in cluster
	transactions  float32 // number of transactions in cluster
}

// Index if the map is an id of cluster
type list_of_clusters map[uint32]single_cluster

// Database consists of some information about db and methods to get or put some data
type database struct {
	server string
	driver string
}

// The db that we will use
var SQLite database

func main() {
	SQLite.driver = "sqlite3"
	SQLite.server = "./database.db"
	var (
		temp_db                     db_cache
		repulsion                   float32
		error_in_previous_iteration bool
		round                       uint32
	)
	repulsion = 2
	log.Println("Import started")
	err := temp_db.pull()
	data_has_been_changed := true
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("import success")
	for data_has_been_changed {
		round++
		log.Printf("Round %v start", round)
		data_has_been_changed, err = temp_db.compute(float64(repulsion))
		if err != nil {
			log.Print(err)
			if error_in_previous_iteration {
				break
			} else {
				error_in_previous_iteration = true
				continue
			}
		}
		err = temp_db.push()
		if err != nil {
			log.Print(err)
			if error_in_previous_iteration {
				break
			} else {
				error_in_previous_iteration = true
				continue
			}
		}
		error_in_previous_iteration = false
		log.Printf("Round %v finished", round)
	}
}

// Get data from server, put it to the local cache, make initial calculations
func (db *db_cache) pull() error {
	const query = "SELECT objects_transactions.object_id, objects_transactions.transaction_id, objects_transactions.qty, transactions.cluster_id FROM objects_transactions, transactions"

	(*db).cluster = make(list_of_clusters)
	(*db).transaction = make(list_of_transactions)

	rows, err := SQLite.requestRows(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		temp := new(struct {
			object_id      uint32
			transaction_id uint32
			cluster_id     uint32
			qty            uint32
		})
		rows.Scan(&temp.object_id, &temp.transaction_id, &temp.qty, &temp.cluster_id)
		// "object_id","transaction_id" & "qty" from table "objects_transactions" have "NOT NULL" property
		// Thereby only "cluster_id" = NULL can be reason of en "converting NULL to uint64 is unsupported" error
		if strings.HasSuffix(fmt.Sprint(err), "converting NULL to uint64 is unsupported") {
			temp.cluster_id = 0
			err = nil
		}

		// If cluster vas not exist -> make new one
		_, already_exist := db.cluster[temp.cluster_id]
		if !already_exist {
			var temp2 single_cluster
			temp2.transactions = 1
			(*db).cluster[temp.cluster_id] = temp2
		}

		// Fill transaction
		_, already_exist = db.transaction[temp.transaction_id]
		if !already_exist {
			temp2 := new(single_transaction)
			temp2.cluster = temp.cluster_id
			temp2.object = make(list_of_objects)
			db.transaction[temp.transaction_id] = *temp2
		}
		db.transaction[temp.transaction_id].object[temp.object_id] = temp.qty
	}

	// count additional data for clusters
	db.updateClusters()

	// Check if cluster is empty
	// If cluster is empty -> delete cluster
	for index, data := range db.cluster {
		if data.transactions == 0 {
			delete(db.cluster, index)
		}
	}

	return err
}

// Find the best profit
func (db *db_cache) compute(repulsion float64) (bool, error) {
	if repulsion <= 1 {
		return false, errors.New("repulsion must be bigger than 1")
	}
	var was_changed bool

	// S_i is a total qty of objects in cluster "i"
	// W_i is a number of uniq objects in cluster "i"
	// C_i is a number of transactions in cluster "i"
	// r is user defined number r > 1
	// Profit is the ratio of sum S_i * C_i / (W_i ^ r) for all transactions to total number of transactions
	// upper_part is the sum S_i * C_i / (W_i ^ r) for all transactions
	// lower part is the total number of transactions
	// In this case profit is ratio of upper part to lower part
	ProfitCounter := func() float32 {
		var (
			upper_part float32 // Sum of all objects in transaction
			lower_part float32 // Number of uniq objects in transaction
		)
		lower_part = float32(len(db.transaction))
		for _, cluster_data := range db.cluster {
			upper_part += cluster_data.total_objects * cluster_data.transactions / float32(math.Pow(float64(cluster_data.uniq_objects), repulsion))
		}
		return upper_part / lower_part
	}

	// Searching for the best profit
	for transaction_id, transaction_data := range db.transaction {
		var (
			max_profit_cluster_id uint32
			max_profit            float32
		)
		original_cluster_id := transaction_data.cluster

		// There is a possibility that we need to define a new cluster to maximize profit
		// To check this case define a new cluster for the transaction
		// Searching for a free id...
		for i := 1; i < len(db.cluster)+1; i++ {
			_, is_exist := db.cluster[uint32(i)]
			if !is_exist {
				db.setClusterId(uint32(i), transaction_id)
				max_profit = ProfitCounter()
				max_profit_cluster_id = uint32(i)
				break
			}
		}

		// In case there is no clusters make the first one
		if len(db.cluster) == 0 {
			db.setClusterId(1, transaction_id)
			max_profit = ProfitCounter()
			max_profit_cluster_id = 1
		}

		if max_profit == float32(math.NaN()) {
			return true, errors.New("profit counter error. divide by zero")
		}
		// Iterating over clusters searching the best profit
		for cluster_id := range db.cluster {
			db.setClusterId(cluster_id, transaction_id)
			profit := ProfitCounter()
			if profit > max_profit {
				max_profit = profit
				max_profit_cluster_id = cluster_id
			}
		}
		if max_profit_cluster_id != original_cluster_id {
			was_changed = true
			db.setClusterId(max_profit_cluster_id, transaction_id)
		}
		fmt.Println(max_profit)
	}
	return was_changed, nil
}

//Save data to database
func (db *db_cache) push() error {
	var (
		clusters_to_be_deleted     string
		clusters_to_be_created     string
		transactions_to_be_updated string
		changes_qty                uint32
	)

	// Getting the list of transactions that should be updated
	// Preparing SQL query for updating transaction id
	rows, err := SQLite.requestRows("SELECT id, cluster_id FROM transactions")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			transactions_id uint32
			cluster_id      uint32
		)
		rows.Scan(&transactions_id, &cluster_id)
		data, is_exist := db.transaction[transactions_id]
		if !is_exist {
			continue
		}
		if data.cluster != cluster_id {
			transactions_to_be_updated += fmt.Sprintf("UPDATE transactions SET cluster_id=%v WHERE id=%v;\n", cluster_id, transactions_id)
			changes_qty++
		}
	}

	// Getting the list of clusters that should be deleted
	// Preparing SQL query for removing unnecessary clusters
	rows, err = SQLite.requestRows("SELECT id FROM clusters")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cluster_id uint32
		rows.Scan(&cluster_id)
		_, is_exist := db.cluster[cluster_id]
		if !is_exist {
			clusters_to_be_deleted += fmt.Sprintf("DELETE FROM clusters WHERE id=%v;\n", cluster_id)
			changes_qty++
		} else {
			// Forming list of clusters that is not exist in the database
			delete(db.cluster, cluster_id)
		}
	}

	// Preparing SQL query for creation of new clusters
	for index := range db.cluster {
		clusters_to_be_created += fmt.Sprintf("INSERT INTO clusters (name, id) VALUES (\"C%v\", %v);\n", index, index)
		changes_qty++
	}
	fmt.Print(clusters_to_be_deleted)
	fmt.Print(clusters_to_be_created)
	fmt.Print(transactions_to_be_updated)
	affected_rows_qty, err := SQLite.executeQuery(clusters_to_be_deleted, clusters_to_be_created, transactions_to_be_updated)
	log.Printf("%v rows of %v have been changed", affected_rows_qty, changes_qty)
	if err != nil {
		return err
	} else if affected_rows_qty != changes_qty {
		return errors.New("not all data was written to database")
	}

	return err
}

// Count properties of clusters
func (db *db_cache) updateClusters() {
	for cluster_id, cluster_data := range db.cluster {
		if cluster_id == 0 {
			delete(db.cluster, cluster_id)
		} else {
			var array_of_object_lists []list_of_objects
			var transaction_counter float32
			for _, transaction_data := range db.transaction {
				if transaction_data.cluster == cluster_id {
					transaction_counter++
					array_of_object_lists = append(array_of_object_lists, transaction_data.object)
				}
			}
			cluster_data.transactions = transaction_counter
			cluster_data.total_objects, cluster_data.uniq_objects = objectsSummAndQty(array_of_object_lists...)
			db.cluster[cluster_id] = cluster_data
		}
	}
}

// Count properties of selected cluster
func (db *db_cache) updateCluster(id uint32) {
	temp := db.cluster[id]
	var array_of_object_lists []list_of_objects
	var transaction_counter float32
	for _, temp2 := range db.transaction {
		if temp2.cluster == id {
			transaction_counter++
			array_of_object_lists = append(array_of_object_lists, temp2.object)
		}
	}
	temp.transactions = transaction_counter
	temp.total_objects, temp.uniq_objects = objectsSummAndQty(array_of_object_lists...)
	db.cluster[id] = temp
}

// It also updates information in current & prev cluster
func (db *db_cache) setClusterId(cluster_id uint32, transaction_id uint32) error {
	prev_cluster_id := (*db).transaction[transaction_id].cluster

	// Create new cluster (if not exist)
	_, already_exist := db.cluster[cluster_id]
	if !already_exist {
		temp := new(single_cluster)
		db.cluster[cluster_id] = *temp
	}

	// Write cluster id into transaction
	{
		temp := db.transaction[transaction_id]
		temp.cluster = cluster_id
		db.transaction[transaction_id] = temp
	}

	// Update additional information about clusters
	{
		temp := (*db).cluster[prev_cluster_id]
		temp.transactions--
		if temp.transactions == 0 {
			delete((*db).cluster, prev_cluster_id)
		} else {
			db.updateCluster(prev_cluster_id)
		}
		db.updateCluster(cluster_id)
	}
	return nil
}

func (d *database) requestRows(query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	db, err := sql.Open(d.driver, d.server)
	if err != nil {
		return rows, err
	}
	defer db.Close()
	rows, err = db.Query(query, args...)
	return rows, err
}

func (d *database) executeQuery(query ...string) (uint32, error) {
	var grand_query string
	db, err := sql.Open(d.driver, d.server)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	for _, temp := range query {
		grand_query += temp
	}
	result, err := db.Exec(grand_query)
	if err != nil {
		return 0, err
	}
	affected_rows_qty, err := result.RowsAffected()
	return uint32(affected_rows_qty), err
}

// First value is a total qty (sum) of all of objects in input lists
// Second value is a total number of uniq objects
func objectsSummAndQty(array ...list_of_objects) (float32, float32) {
	var sum uint32
	GrandArray := make(list_of_objects)

	for _, subarray := range array {
		for index, qty := range subarray {
			sum += qty
			GrandArray[index] = 1
		}
	}
	return float32(sum), float32(len(GrandArray))
}
