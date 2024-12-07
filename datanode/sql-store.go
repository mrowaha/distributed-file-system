package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/mrowaha/hdfs-api/api"
	hdfsCommon "github.com/mrowaha/hdfs-api/common"
	hdfsDataNode "github.com/mrowaha/hdfs-api/node"
)

type DataNodeSqlStore struct {
	db *sql.DB
}

func NewDataNodeSqlStore() *DataNodeSqlStore {
	db, err := sql.Open("sqlite3", os.Getenv("DBFile"))
	if err != nil {
		log.Fatalf("data node could not establish db connection: %v", err)
	}
	return &DataNodeSqlStore{
		db: db,
	}
}

func (s *DataNodeSqlStore) BootStrap() {
	query := `
	CREATE TABLE IF NOT EXISTS datanode (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_name TEXT NOT NULL,
		chunk_number INTEGER NOT NULL,
		total_chunks INTEGER NOT NULL,
		chunk_data BLOB NOT NULL		
	);	
	CREATE TABLE IF NOT EXISTS datanode_meta (
		id TEXT PRIMARY KEY
	);
	`

	if _, err := s.db.Exec(query); err != nil {
		log.Fatalf("failed to bootstrap sqlite store: %v", err)
	}
}

func (s *DataNodeSqlStore) Has(fileName string) (chunkNumbers []int64, err error) {
	return make([]int64, 0), nil
}

func (s *DataNodeSqlStore) Write(fileName string, chunkNumber int64, totalChunks int64, chunk []byte) error {
	query := `
		INSERT INTO datanode (file_name, chunk_number, total_chunks, chunk_data)
		VALUES (?, ?, ?, ?);
	`

	_, err := s.db.Exec(query, fileName, chunkNumber, totalChunks, chunk)
	if err != nil {
		return fmt.Errorf("failed to write data to datanode table: %w", err)
	}

	return nil
}

func (s *DataNodeSqlStore) Read(fileName string) ([]*hdfsCommon.Chunk, error) {
	query := `
		SELECT chunk_number, total_chunks, chunk_data
		FROM datanode
		WHERE file_name = ?
		ORDER BY chunk_number ASC;
	`

	rows, err := s.db.Query(query, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to query datanode table: %w", err)
	}
	defer rows.Close()

	var chunks []*hdfsCommon.Chunk

	for rows.Next() {
		var chunkNumber int64
		var totalChunks int64
		var chunkData []byte

		if err := rows.Scan(&chunkNumber, &totalChunks, &chunkData); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		chunks = append(chunks, &hdfsCommon.Chunk{
			FileName:    fileName,
			Data:        chunkData,
			ChunkNumber: chunkNumber,
			TotalChunks: totalChunks,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	return chunks, nil
}

func (s *DataNodeSqlStore) ReadChunks(fileName string, chunks []int64) ([]*api.SendChunkRespose_ChunkData, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks specified")
	}

	// Create placeholders for the IN clause
	placeholders := make([]string, len(chunks))
	args := make([]interface{}, len(chunks)+1) // +1 for fileName
	args[0] = fileName

	for i, chunk := range chunks {
		placeholders[i] = "?"
		args[i+1] = chunk
	}

	query := fmt.Sprintf(`
        SELECT chunk_number, chunk_data
        FROM datanode
        WHERE file_name = ? AND chunk_number IN (%s);
    `, strings.Join(placeholders, ","))

	// Execute the query
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query datanode table for chunk: %w", err)
	}
	defer rows.Close()

	var result []*api.SendChunkRespose_ChunkData
	for rows.Next() {
		var chunkData []byte
		var chunkNumber int64
		if err := rows.Scan(&chunkNumber, &chunkData); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, &api.SendChunkRespose_ChunkData{
			Number: chunkNumber,
			Data:   chunkData,
		})
	}
	return result, nil
}

func (s *DataNodeSqlStore) Me() (uuid.UUID, error) {
	query := `
		SELECT id
		FROM datanode_meta
		LIMIT 1;
	`

	var id string
	err := s.db.QueryRow(query).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return uuid.Nil, fmt.Errorf("no UUID found in the table")
		}
		return uuid.Nil, err
	}

	uuidValue, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid uuid format: %v", err)
	}

	return uuidValue, nil
}

func (s *DataNodeSqlStore) UpdateMe(newUuid uuid.UUID) error {
	query := `
		INSERT INTO datanode_meta (id)
		VALUES (?);
	`

	result, err := s.db.Exec(query, newUuid.String())
	if err != nil {
		return fmt.Errorf("failed to update UUID: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows were updated")
	}

	return nil
}

func (s *DataNodeSqlStore) Meta() (meta []*hdfsDataNode.DataNodeFileMeta, err error) {
	fileNamesQuery := `
		SELECT DISTINCT file_name
		FROM datanode;
	`

	rows, err := s.db.Query(fileNamesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query filenames from datanode table: %w", err)
	}
	defer rows.Close()
	var currFileName string
	for rows.Next() {
		if err := rows.Scan(&currFileName); err != nil {
			return nil, fmt.Errorf("failed to scan row for filename: %w", err)
		}
		query := `
			SELECT chunk_number, total_chunks
			FROM datanode
			WHERE file_name = ?
			ORDER BY chunk_number ASC;
		`

		chunkrows, err := s.db.Query(query, currFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to query datanode table for chunk numbers: %w", err)
		}
		defer chunkrows.Close()

		var chunks []int64
		var totalChunks int64
		for chunkrows.Next() {
			var chunkNumber int64
			if err := chunkrows.Scan(&chunkNumber, &totalChunks); err != nil {
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}
			chunks = append(chunks, chunkNumber)
		}

		if err := chunkrows.Err(); err != nil {
			return nil, fmt.Errorf("error during rows iteration: %w", err)
		}

		fmt.Println("total ", totalChunks)

		fileMeta := &hdfsDataNode.DataNodeFileMeta{
			FileName:    currFileName,
			Chunks:      chunks,
			TotalChunks: totalChunks,
		}

		meta = append(meta, fileMeta)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	return meta, nil
}

func (s *DataNodeSqlStore) Size() float64 {
	fileInfo, err := os.Stat(os.Getenv("DBFile"))
	if err != nil {
		log.Fatalf("Error getting file info: %v\n", err)
	}
	return float64(fileInfo.Size())
}

func (s *DataNodeSqlStore) ListFiles() ([]string, error) {
	fileNamesQuery := `
		SELECT DISTINCT file_name
		FROM datanode;
	`

	rows, err := s.db.Query(fileNamesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query filenames from datanode table: %w", err)
	}
	defer rows.Close()

	fsState := make([]string, 0)
	var currFileName string
	for rows.Next() {
		if err := rows.Scan(&currFileName); err != nil {
			return nil, fmt.Errorf("failed to scan row for filename: %w", err)
		}
		fsState = append(fsState, currFileName)
	}

	return fsState, nil
}

func (s *DataNodeSqlStore) DeleteChunks(fileName string) error {
	query := `
		DELETE FROM datanode
		WHERE file_name = ?;
	`

	_, err := s.db.Exec(query, fileName)
	if err != nil {
		return fmt.Errorf("failed to delete chunks for file %s: %w", fileName, err)
	}

	return nil
}
