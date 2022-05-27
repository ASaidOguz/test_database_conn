package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {

	// connect to a database
	conn, err := sql.Open("pgx", "host=localhost port=5432 dbname=test_DB user=postgres password=2061040215")
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to connect %v\n", err))
	}
	defer conn.Close()
	log.Println("Connected to DataBase !")
	// test my connection
	err = conn.Ping()
	if err != nil {
		log.Fatal("Cannot Ping DataBase")
	}
	log.Println("Pinged DataBase !")
	//get rows from table
	err = GetAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}
	//insert a row
	query := ` insert into users (first_name,last_name) values ($1,$2) `
	_, err = conn.Exec(query, "jack", "Brown")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Insertion of one Row completed ")

	//get rows from table again
	err = GetAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}
	//update a row
	sttmnt := `update users set first_name= $1 where id= $2`
	_, err = conn.Exec(sttmnt, "Jackie", 2)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Update of one or more rows is complete !")
	//get rows from table again
	err = GetAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}
	//get one row by id

	var firstName, lastName string
	var id int
	query = `select id ,first_name,last_name from users where id=$1`

	row := conn.QueryRow(query, 1)
	err = row.Scan(&id, &firstName, &lastName)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Query row returns :", id, firstName, lastName)
	//delete a row
	query = `delete from users where id=$1 `
	_, err = conn.Exec(query, 3)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Deleted a row !")
	//get rows
	err = GetAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

}

func GetAllRows(conn *sql.DB) error {

	rows, err := conn.Query("select id,first_name,last_name from users")
	if err != nil {
		log.Println(err)
		return err
	}
	defer rows.Close()
	var firstName, lastName string
	var id int

	for rows.Next() {
		err := rows.Scan(&id, &firstName, &lastName)
		if err != nil {
			log.Println(err)
			return err
		}
		fmt.Println("Record is", id, firstName, lastName)
	}
	if err = rows.Err(); err != nil {
		log.Fatal("Error scanning rows", err)
	}
	fmt.Println("--------------------------------------")
	return nil
}
