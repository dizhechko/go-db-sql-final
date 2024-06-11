package main

import (
	"database/sql"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	return int(id), nil

}

// реализуйте чтение строки по заданному number
// здесь из таблицы должна вернуться только одна строка
// заполните объект Parcel данными из таблицы
func (s ParcelStore) Get(number int) (Parcel, error) {
	p := Parcel{}

	row := s.db.QueryRow("SELECT  number, client, status, address, created_at FROM parcel WHERE number = :number", sql.Named("number", number))
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}
	return p, nil
}

// реализуйте чтение строк из таблицы parcel по заданному client
// здесь из таблицы может вернуться несколько строк
// заполните срез Parcel данными из таблицы
func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	var res []Parcel

	rows, err := s.db.Query("SELECT number, client, status, address, created_at FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		parcel := Parcel{}

		err := rows.Scan(&parcel.Number, &parcel.Client, &parcel.Status, &parcel.Address, &parcel.CreatedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, parcel)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

// реализуйте обновление статуса в таблице parcel
func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number))
	if err != nil {
		return err
	}
	return nil

}

// реализуйте обновление адреса в таблице parcel
// менять адрес можно только если значение статуса registered
func (s ParcelStore) SetAddress(number int, address string) error {
	_, err := s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number and status = :status",
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		return err
	}
	return nil
}

// реализуйте удаление строки из таблицы parcel
// удалять строку можно только если значение статуса registered
func (s ParcelStore) Delete(number int) error {
	_, err := s.db.Exec("DELETE FROM parcel WHERE number = :number and status=:status",
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		return err
	}
	return nil
}
