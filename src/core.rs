extern crate protobuf;

use lazy_static::lazy_static; // 1.4.0
use std::sync::Mutex;
use protobuf::*;
use crate::proto;
use rusqlite::{params, Connection, NO_PARAMS};
use serde_derive::{Serialize, Deserialize};  
use serde_json;
use log::{info, trace, warn};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono;
use std::collections::HashMap;
use std::borrow::Borrow;
use chrono::NaiveDateTime;
use std::thread;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use tantivy::collector::TopDocs;
use tantivy::query::{QueryParser,Occur, Query, TermQuery, BooleanQuery};
use tantivy::schema::*;
use tantivy::ReloadPolicy;
use tantivy::directory::MmapDirectory;
use std::path::Path;
use std::sync::RwLock;
use std::fs;
use html2text::from_read;
use self::protobuf::rust::quote_escape_bytes;
use http::StatusCode;

struct WorkerData {
    active: bool,
    email: String,
    token: String,
    userId: u32,
    apiPath: String,
}

struct Worker {
    data: Mutex<WorkerData>,
    connection: Mutex<Option<Connection>>,
    index: Mutex<Option<tantivy::Index>>,
    index_writer: Mutex<Option<tantivy::IndexWriter>>,
    index_reader: Mutex<Option<tantivy::IndexReader>>,
    fields: RwLock<HashMap<String, tantivy::schema::Field>>,
}

pub struct AsyncWorker {
    sender: Arc<Mutex<Sender<Vec<u8>>>>,
    pub receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    inner: Arc<Worker>,
}

#[derive(Debug)]
#[derive(Deserialize)]
struct Folder {
    id: String,
    title: String,    
    parentId: Option<String>,
    level: i32,
    userId: i32,
    createdAt: i64,
    updatedAt: i64,
    deletedAt: Option<i64>,
}

#[derive(Debug)]
#[derive(Deserialize)]
struct Note {
    id: String,
    title: String,
    folderId: String,
    userId: i32,
    level:i32,
    text: String,
    createdAt: i64,
    updatedAt: i64,
    deletedAt: Option<i64>,
}

#[derive(Debug)]
#[derive(Deserialize)]
struct SyncNoteShortInfo {
    id: String,
    folderId: String,
    createdAt: chrono::DateTime<chrono::Utc>,
    updatedAt: chrono::DateTime<chrono::Utc>,
    deletedAt: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
#[derive(Deserialize)]
struct SyncFolderInfo {
    id: String,
    title: String,
    parentId: Option<String>,
    level: i32,
    userId: i32,
    createdAt: chrono::DateTime<chrono::Utc>,
    updatedAt: chrono::DateTime<chrono::Utc>,
    deletedAt: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize)]
struct SyncDataHolderResponse {
    notes: Vec<SyncNoteShortInfo>,
    folders: Vec<SyncFolderInfo>,
}

#[derive(Debug)]
#[derive(Serialize)]
struct FolderRemoteUpload<'a> {
    id: &'a str,
    title: &'a str,
    parentId: Option<&'a str>,
    level: i32,
    userId: i32,
    createdAt: chrono::DateTime<chrono::Utc>,
    updatedAt: chrono::DateTime<chrono::Utc>,
    deletedAt: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
#[derive(Serialize)]
struct NoteRemoteUpload<'a> {
    id: &'a str,
    title: &'a str,
    text: &'a str,
    folderId: &'a str,
    userId: i32,
    level:i32,
    createdAt: chrono::DateTime<chrono::Utc>,
    updatedAt: chrono::DateTime<chrono::Utc>,
    deletedAt: Option<chrono::DateTime<chrono::Utc>>,
}


#[derive(Debug)]
#[derive(Deserialize)]
struct NoteRemote {
    id: String,
    title: String,
    text: String,
    folderId: String,
    createdAt: chrono::DateTime<chrono::Utc>,
    updatedAt: chrono::DateTime<chrono::Utc>,
    deletedAt: Option<chrono::DateTime<chrono::Utc>>,
}


#[derive(Serialize, Deserialize)]
struct LoginData {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    token: String,
    userId: u32,
    rootFolder: SyncFolderInfo,
}

pub trait DbMigration {
    fn get_id(&self) -> i32;
    /// Apply a migration to the database using a transaction.
    fn up(&self, conn: &Connection);

    /// Revert a migration to the database using a transaction.
    fn down(&self, conn: &Connection);
}

struct InitialMigration;

impl DbMigration for InitialMigration {
    fn get_id(&self) -> i32 {
        return 1
    }

    fn up(&self, conn: &Connection) {
        conn.execute_batch(
            "
            BEGIN;
            CREATE TABLE IF NOT EXISTS 'folder' ('id' char(36) NOT NULL, 'createdAt' INTEGER, 'updatedAt' INTEGER, 'deletedAt' INTEGER, 'title' varchar NOT NULL, 'parentId' char(36), 'level' INT, 'userId' INT, PRIMARY KEY('id'), FOREIGN KEY('parentId') REFERENCES 'folder'('id'));
            CREATE TABLE IF NOT EXISTS 'note' ('id' char(36) NOT NULL, 'createdAt' INTEGER, 'updatedAt' INTEGER, 'deletedAt' INTEGER, 'title' varchar NOT NULL, 'text' text NOT NULL, 'folderId' char(36), 'level' INT, 'userId' INT, encrypted INT DEFAULT 0, PRIMARY KEY('id'), FOREIGN KEY('folderId') REFERENCES 'folder'('id'));
            CREATE TABLE IF NOT EXISTS 'favorites' ('noteId' char(36) NOT NULL, 'userId' INT NOT NULL);
            CREATE TABLE IF NOT EXISTS 'props'('key' char(100) NOT NULL, 'value' char(100), PRIMARY KEY('key'));
            CREATE TABLE IF NOT EXISTS 'migrations' ('id' INTEGER NOT NULL, PRIMARY KEY('id'));
            COMMIT;",
        );
    }

    fn down(&self, conn: &Connection)  {
    }
}

impl Worker {
    fn init(&self) {
        self.data.lock().unwrap().active = false;
        info!("Creating worker");
    }

    fn get_property(&self, key: &str) -> Option<String> {
        let query_result = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT value FROM props WHERE key = ?", params![key], |row| row.get(0));
        match query_result {
            Ok(v) => v,
            Err(e) => None,
        }
    }

    fn insert_or_update_props_record(&self, key: &str, value: &str) {
        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "INSERT OR REPLACE INTO props (key, value) VALUES (?, ?)",
            params![key, value],
        ).unwrap();
    }

    fn retrieve_login_data_from_db(&self, data: &mut WorkerData) {
        data.token = match self.get_property("token") {
            Some(v) => v,
            None => "".to_string(),            
        };

        data.userId = match self.get_property("userId") {
            Some(v) => v.parse().unwrap(),
            None => 0,            
        };
        
        data.email = match self.get_property("email") {
            Some(v) => v,
            None => "".to_string(),            
        };        
    }

    fn save_login_data_to_db(&self, data: &WorkerData) {
        self.insert_or_update_props_record("token", &data.token);
        self.insert_or_update_props_record("userId", &data.userId.to_string());
        self.insert_or_update_props_record("email", &data.email);
    }

    fn handle_init(&self, command_data: &[u8]) -> Vec<u8> {
        //Makes all the initalization work
        info!("Init command started");
        let mut data = self.data.lock().unwrap();

        if data.active {
            warn!("Already initialized");
            let mut res = proto::messages::EmptyResultResponse::new();
            res.success = false;
            return res.write_to_bytes().unwrap();
        }

        let init_data = parse_from_bytes::<proto::messages::InitData>(&command_data).unwrap();
        data.apiPath = init_data.get_apiPath().to_string();
        info!("Backend API {}", data.apiPath);
        info!("Data directory{}", init_data.get_dataPath());

        let dbPath = Path::new(&init_data.get_dataPath()).join("local.db");
        info!("Opening SQlite database with path {}", &dbPath.to_str().unwrap());
        {
            let mut conn = self.connection.lock().unwrap();
            *conn = Some(Connection::open(&dbPath.to_str().unwrap()).unwrap());
        }

        self.run_migrations();

        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;
        data.active = true;

        self.retrieve_login_data_from_db(&mut data);

        self.initialize_fts(init_data.get_dataPath());

        return res.write_to_bytes().unwrap();
    }

    fn run_migrations(&self) {
        let result = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT MAX(id) as lastAppliedId FROM migrations", NO_PARAMS, |r| r.get(0));
        let mut last_applied_migration : i32 = 0;
        match result {
            Ok(v) => {
              last_applied_migration = v;
            },
            Err(_) => {}
        }

        let migrations:Vec<&DbMigration> = vec![&InitialMigration{}];

        for m in migrations {
            if m.get_id() <= last_applied_migration {
                continue;
            }
            m.up(self.connection.lock().unwrap().as_ref().unwrap());

            self.connection.lock().unwrap().as_ref().unwrap().execute("INSERT INTO migrations VALUES (?)", params![m.get_id()]).unwrap();
        }
    }

    fn initialize_fts(&self, data_path: &str) {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("id", STRING | STORED);
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("text", TEXT);
        schema_builder.add_text_field("userId", TEXT | STORED);
        schema_builder.add_text_field("folderId", TEXT | STORED);

        let schema = schema_builder.build();

        let index_dir = Path::new(data_path).join("tantivy");
        if !index_dir.exists() {
            fs::create_dir(&index_dir);
        }

        let mmap_dir = MmapDirectory::open(index_dir).unwrap();

        {
            let mut fields = self.fields.write().unwrap();
            fields.insert("id".to_string(), schema.get_field("id").unwrap());
            fields.insert("title".to_string(), schema.get_field("title").unwrap());
            fields.insert("text".to_string(), schema.get_field("text").unwrap());
            fields.insert("userId".to_string(), schema.get_field("userId").unwrap());
            fields.insert("folderId".to_string(), schema.get_field("folderId").unwrap());
        }

        {
            let mut index_guard = self.index.lock().unwrap();
            *index_guard = Some(tantivy::Index::open_or_create(mmap_dir, schema.clone()).unwrap());
            let mut index_writer_guard = self.index_writer.lock().unwrap();
            *index_writer_guard = Some(index_guard.as_ref().unwrap().writer(50_000_000).unwrap());
            let mut index_reader_guard = self.index_reader.lock().unwrap();
            *index_reader_guard = Some(index_guard.as_ref().unwrap()
                                            .reader_builder()
                                            .reload_policy(ReloadPolicy::OnCommit)
                                            .try_into().unwrap());
        }


    }

    fn handle_set_token(&self, command_data: &[u8]) -> Vec<u8> {
        info!("Set token");
        let parsed = parse_from_bytes::<proto::messages::SetToken>(&command_data).unwrap();
        self.data.lock().unwrap().token = parsed.get_token().to_string();

        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn mark_note_as_deleted(&self, id: &str) {
        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "UPDATE note SET deletedAt = ? WHERE id = ?",
            params![self.get_time_in_millis(), &id]
        );
    }

    fn mark_folder_as_deleted(&self, id: &str) {
        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "UPDATE folder SET deletedAt = ? WHERE id = ?",
            params![self.get_time_in_millis(), &id]
        );
    }

    fn handle_remove_note(&self, command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::RemoveNote>(&command_data).unwrap();
        info!("Remove note with id {}", parsed.noteId);

        self.mark_note_as_deleted(&parsed.noteId);

        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_remove_folder(&self, command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::RemoveFolder>(&command_data).unwrap();
        info!("Remove folder with id {}", parsed.folderId);

        let allChildFoldersQuery = "WITH RECURSIVE children(id) AS ( \
            SELECT id FROM folder WHERE id = ? \
            UNION ALL \
            SELECT f.id \
            FROM folder f \
            JOIN children c \
            ON f.parentId = c.id) \
            SELECT id FROM children";

        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let mut stmt = conn.prepare(allChildFoldersQuery).unwrap();
        let folder_ids_iter = stmt.query_map(params![&parsed.folderId], |row| row.get(0)).unwrap();

        let mut ids_vec: Vec<String> = Vec::new();
        for folder_id_elem in folder_ids_iter {
            let folder_id: String = folder_id_elem.unwrap();
            let mut quoted = String::new();
            quoted.push_str("'");
            quoted.push_str(&folder_id);
            quoted.push_str("'");
            ids_vec.push(quoted);
        }

        let ids_joined_str = ids_vec.join(",");

        let mut folders_update_query = String::new();
        folders_update_query.push_str(&"UPDATE folder SET deletedAt = ? WHERE id IN (".to_string());
        folders_update_query.push_str(&ids_joined_str);
        folders_update_query.push_str(&")".to_string());

        let mut notes_update_query = String::new();
        notes_update_query.push_str(&"UPDATE note SET deletedAt = ? WHERE folderId IN (".to_string());
        notes_update_query.push_str(&ids_joined_str);
        notes_update_query.push_str(&")".to_string());

        let time_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        conn.execute(&folders_update_query, params![time_millis]).unwrap();
        conn.execute(&notes_update_query, params![time_millis]).unwrap();

        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_create_note(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::CreateNote>(&command_data).unwrap();
        info!("Create note with name {}", parsed.title);

        let new_note_uuid = Uuid::new_v4().to_hyphenated().to_string();

        let time_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let user_id = self.data.lock().unwrap().userId;

        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "INSERT INTO note (id, title, text, folderId, userId, level, createdAt, updatedAt) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![&new_note_uuid, &parsed.title, &parsed.text, &parsed.folderId, &user_id, 0, time_millis, time_millis],
        ).unwrap();

        let mut res = proto::messages::CreateNoteResponse::new();
        res.success = true;
        res.noteId = new_note_uuid.clone();

        let plain_text =  from_read(parsed.text.as_bytes(), 10000000);

        self.add_note_to_search_index(&new_note_uuid, &parsed.title, &plain_text, &parsed.folderId);
        return res.write_to_bytes().unwrap();
    }

    fn add_note_to_search_index(&self, id: &str, title: &str, text: &str, folder_id: &str) {
        let mut index_writer = self.index_writer.lock().unwrap();
        let fields_map = self.fields.read().unwrap();

        let mut indexed_doc = Document::default();
        indexed_doc.add_text(*fields_map.get("id").unwrap(), id);
        indexed_doc.add_text(*fields_map.get("title").unwrap(), title);
        indexed_doc.add_text(*fields_map.get("text").unwrap(), text);
        indexed_doc.add_text(*fields_map.get("folderId").unwrap(), folder_id);

        let user_id = self.data.lock().unwrap().userId;
        indexed_doc.add_text(*fields_map.get("userId").unwrap(), &user_id.to_string());

        index_writer.as_ref().unwrap().add_document(indexed_doc);
        index_writer.as_mut().unwrap().commit();
    }


    fn update_note_in_search_index(&self, id: &str, title: &str, text: &str, folder_id: &str) {
        let ir_guard = self.index_reader.lock().unwrap();
        let reader = ir_guard.as_ref().unwrap();
        let searcher = reader.searcher();

        let fields_map = self.fields.read().unwrap();
        let id_field = fields_map.get("id").unwrap();

        let id_term = Term::from_field_text(*id_field, id);

        let term_query = TermQuery::new(id_term.clone(), IndexRecordOption::Basic);
        let top_docs = searcher.search(&term_query, &TopDocs::with_limit(1)).unwrap();

        if let Some((_score, doc_address)) = top_docs.first() {
            let doc = searcher.doc(*doc_address).unwrap();

            let mut index_writer = self.index_writer.lock().unwrap();
            index_writer.as_ref().unwrap().delete_term(id_term);
        }

        self.add_note_to_search_index(id, title, text, folder_id);
    }

    fn has_folder_by_id(&self, id: &str) -> bool {
        let i: i64 = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT COUNT(*) FROM folder WHERE id = ?", params![id], |r| r.get(0)).unwrap();
        return i > 0;
    }

    fn insert_folder(&self, id: &str, title: &str, parentId: String, level: u32, userId: u32, created_at: i64, updated_at: i64, deleted_at: Option<i64>) {

        if parentId.len() > 0 {
            let parent_folder = self.get_local_folder_by_id(&parentId);
            if parent_folder.is_none() {
                info!("Parent folder {} for id {} not found", parentId, id);
                return;
            }
        }

        let new_folder_parent_data = if parentId.len() > 0 {
            rusqlite::types::Value::from(parentId)
        } else {
            rusqlite::types::Value::from(rusqlite::types::Null)
        };

        let deleted_at_data = match deleted_at {
            Some(T) => rusqlite::types::Value::from(deleted_at),
            None => rusqlite::types::Value::from(rusqlite::types::Null),
        };

        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "INSERT INTO folder (id, title, parentId, level, userId, createdAt, updatedAt, deletedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![id, title, &new_folder_parent_data, level, userId, created_at, updated_at, &deleted_at_data],
        ).unwrap();
    }

    fn insert_note(&self, id: &str, title: &str, folderId: &str, text: &str, userId: u32, level: u32, created_at: i64, updated_at: i64) -> bool {

        if folderId.len() > 0 {
            let parent_folder = self.get_local_folder_by_id(&folderId.clone());
            if parent_folder.is_none() {
                info!("Cannot retrieve parent folder {} for note {}", folderId, id);
                return false;
            }

            if parent_folder.unwrap().deletedAt.is_some() {
                info!("Parent folder {} for note {} is deleted", folderId, id);
                return false;
            }
        }

        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "INSERT INTO note (id, title, folderId, text, level, userId, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![id, title, folderId, text, level, userId, created_at, updated_at],
        ).unwrap();

        return true;
    }

    fn handle_create_folder(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::CreateFolder>(&command_data).unwrap();
        info!("Create folder with name {}", parsed.title);


        let mut new_folder_level = 0;

        if parsed.parentId.len() > 0 {
            let parent_level: u32 = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT level FROM folder WHERE id = ?", params![parsed.parentId], |row| row.get(0)).unwrap();
            new_folder_level = parent_level + 1;
        }

        let new_folder_uuid = Uuid::new_v4().to_hyphenated().to_string();
        let time_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let user_id = self.data.lock().unwrap().userId;
        self.insert_folder(&new_folder_uuid, &parsed.title, parsed.parentId, new_folder_level, user_id, time_millis, time_millis, None);

        let mut res = proto::messages::CreateFolderResponse::new();
        res.success = true;
        res.folderId = new_folder_uuid;

        return res.write_to_bytes().unwrap();
    }

    fn handle_get_notes_by_folder(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::GetNotesList>(&command_data).unwrap();
        info!("Get notes by folder name {}", parsed.folderId);

        let userId = self.data.lock().unwrap().userId;
        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let mut stmt = conn.prepare("SELECT id, folderId, title, createdAt, updatedAt FROM note WHERE deletedAt IS NULL AND folderId = ? ORDER BY updatedAt DESC").unwrap();
        let notes_iter = stmt.query_map(params![parsed.folderId], |row| {
            Ok(Note {
                id: row.get(0)?,
                folderId: row.get(1)?,
                title: row.get(2)?,
                text : "".to_string(),
                createdAt: row.get(3)?,
                updatedAt: row.get(4)?,
                userId: userId as i32,
                level: 0,
                deletedAt: None,
            })
        }).unwrap();

        let mut res = proto::messages::GetNotesListResponse::new();
        res.success = true;

        let mut notes_list_proto = protobuf::RepeatedField::<proto::messages::NoteShortInfo>::new();

        for n in notes_iter {
            let note = n.unwrap();
            let mut nsi = proto::messages::NoteShortInfo::new();
            nsi.id = note.id;
            nsi.folderId = note.folderId;
            nsi.title = note.title;
            nsi.createdAt = note.createdAt;
            nsi.updatedAt = note.updatedAt;
            notes_list_proto.push(nsi);
        }

        res.set_notes(notes_list_proto);

        return res.write_to_bytes().unwrap();
    }

    fn handle_get_all_notes(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::GetAllNotes>(&command_data).unwrap();
        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let userId = self.data.lock().unwrap().userId;
        let mut stmt = conn.prepare("SELECT id, folderId, title, createdAt, updatedAt, level FROM note WHERE deletedAt IS NULL AND userId = ? ORDER BY updatedAt DESC LIMIT ? OFFSET ?").unwrap();
        let notes_iter = stmt.query_map(params![userId, parsed.limit, parsed.offset], |row| {
            Ok(Note {
                id: row.get(0)?,
                folderId: row.get(1)?,
                title: row.get(2)?,
                text: "".to_string(),
                createdAt: row.get(3)?,
                updatedAt: row.get(4)?,
                deletedAt: None,
                userId: userId as i32,
                level: row.get(5)?,
            })
        }).unwrap();

        let mut res = proto::messages::GetNotesListResponse::new();
        res.success = true;

        let mut notes_list_proto = protobuf::RepeatedField::<proto::messages::NoteShortInfo>::new();

        for n in notes_iter {
            let note = n.unwrap();
            let mut nsi = proto::messages::NoteShortInfo::new();
            nsi.id = note.id;
            nsi.folderId = note.folderId;
            nsi.title = note.title;
            nsi.createdAt = note.createdAt;
            nsi.updatedAt = note.updatedAt;
            notes_list_proto.push(nsi);
        }

        res.set_notes(notes_list_proto);

        return res.write_to_bytes().unwrap();
    }

    fn get_local_note_by_id(&self, id: &str) -> Option<Note> {
        let query_result = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT id, title, folderId, userId, level, text, createdAt, updatedAt, deletedAt FROM note WHERE id = ?", params![id], |row| {
            Ok(Note {
                id: row.get(0)?,
                title: row.get(1)?,
                folderId: row.get(2)?,
                userId: row.get(3)?,
                level: row.get(4)?,
                text: row.get(5)?,
                createdAt: row.get(6)?,
                updatedAt: row.get(7)?,
                deletedAt: row.get(8)?,
            })
        });

        match query_result {
            Ok(v) => Some(v),
            Err(e) => None,
        }
    }

    fn handle_get_note_by_id(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::GetNoteById>(&command_data).unwrap();
        info!("Get note by id {}", parsed.noteId);

        let note = self.get_local_note_by_id(&parsed.noteId).unwrap();
        let mut res = proto::messages::GetNoteByIdResponse::new();
        res.success = true;
        res.id = note.id;
        res.folderId = note.folderId;
        res.title = note.title;
        res.text = note.text;

        return res.write_to_bytes().unwrap();
    }

    fn get_local_folder_by_id(&self, id: &str) -> Option<Folder> {
        let user_id = self.data.lock().unwrap().userId;
        let query_result = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT id, parentId, title, level, createdAt, updatedAt, deletedAt FROM folder WHERE id = ?", params![id], |row| {
            Ok(Folder {
                id: row.get(0)?,
                parentId: row.get(1)?,
                title: row.get(2)?,
                level: row.get(3)?,
                userId: user_id as i32,
                createdAt: row.get(4)?,
                updatedAt: row.get(5)?,
                deletedAt: row.get(6)?,
            })
        });

        match query_result {
            Ok(v) => Some(v),
            Err(e) => None,
        }
    }

    fn handle_get_folder_by_id(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::GetFolderById>(&command_data).unwrap();
        info!("Get folder by id {}", parsed.folderId);

        let folder = self.get_local_folder_by_id(&parsed.folderId).unwrap();

        let mut res = proto::messages::GetFolderByIdResponse::new();
        res.success = true;
        res.id = folder.id;
        res.parentId = match folder.parentId {
            Some(v) => v,
            None => "".to_string(),
        };
        res.title = folder.title;
        res.level = folder.level;
        res.createdAt = folder.createdAt;
        res.updatedAt = folder.updatedAt;

        return res.write_to_bytes().unwrap();
    }

    fn handle_login(&self,command_data: &[u8]) -> Vec<u8> {

        let parsed = parse_from_bytes::<proto::messages::Login>(&command_data).unwrap();

        info!("Login for account {}", parsed.email);

        let loginData = LoginData {
            email: parsed.email.clone(),
            password: parsed.password,
        };

        let client = reqwest::blocking::Client::new();
        let mut data = self.data.lock().unwrap();
        let mut endpoint = data.apiPath.clone();
        endpoint.push_str(&"/login".to_string());

        let loginResponseResult = client.post(&endpoint).json(&loginData).send();

        let loginResponseRaw = match loginResponseResult {
            Ok(result)  => result,
            Err(e) => {
                let mut res = proto::messages::LoginResponse::new();
                res.errorCode = 1;
                res.success = false;
                return res.write_to_bytes().unwrap();
            },
        };

        if loginResponseRaw.status() != StatusCode::OK {
            let mut res = proto::messages::LoginResponse::new();
            res.errorCode = 2;
            res.success = false;
            return res.write_to_bytes().unwrap();
        }

        let loginResponse : LoginResponse = loginResponseRaw.json().unwrap();
        data.token = loginResponse.token;
        data.userId = loginResponse.userId;
        data.email = parsed.email;

        let mut res = proto::messages::LoginResponse::new();
        res.success = true;

        self.save_login_data_to_db(&data);

        if !self.has_folder_by_id(&loginResponse.rootFolder.id) {
            self.insert_folder(&loginResponse.rootFolder.id, &loginResponse.rootFolder.title,
                               "".to_string(), 0, loginResponse.userId,
                                loginResponse.rootFolder.createdAt.timestamp_millis(),
                               loginResponse.rootFolder.updatedAt.timestamp_millis(), None);
        }

        return res.write_to_bytes().unwrap();
    }

    fn handle_register(&self,command_data: &[u8]) -> Vec<u8> {

        let parsed = parse_from_bytes::<proto::messages::Login>(&command_data).unwrap();

        info!("Register account for {}", parsed.email);

        let loginData = LoginData {
            email: parsed.email.clone(),
            password: parsed.password,
        };

        let client = reqwest::blocking::Client::new();
        let mut data = self.data.lock().unwrap();
        let mut endpoint = data.apiPath.clone();
        endpoint.push_str(&"/register".to_string());

        let loginResponseResult = client.post(&endpoint).json(&loginData).send();

        let loginResponseRaw = match loginResponseResult {
            Ok(result)  => result,
            Err(e) => {
                let mut res = proto::messages::LoginResponse::new();
                res.errorCode = 1;
                res.success = false;
                return res.write_to_bytes().unwrap();
            },
        };

        if loginResponseRaw.status() != StatusCode::OK {
            let mut res = proto::messages::LoginResponse::new();
            res.errorCode = 2;
            res.success = false;
            return res.write_to_bytes().unwrap();
        }

        let loginResponse : LoginResponse = loginResponseRaw.json().unwrap();
        data.token = loginResponse.token;
        data.userId = loginResponse.userId;
        data.email = parsed.email;

        let mut res = proto::messages::LoginResponse::new();
        res.success = true;

        self.save_login_data_to_db(&data);

        if !self.has_folder_by_id(&loginResponse.rootFolder.id) {
            self.insert_folder(&loginResponse.rootFolder.id, &loginResponse.rootFolder.title,
                               "".to_string(), 0, loginResponse.userId,
                               loginResponse.rootFolder.createdAt.timestamp_millis(),
                               loginResponse.rootFolder.updatedAt.timestamp_millis(), None);
        }

        return res.write_to_bytes().unwrap();
    }

    fn get_time_in_millis(&self) -> i64 {
        return SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    }

    fn millis_to_datetime(&self, millis: i64) -> chrono::DateTime<chrono::Utc> {
        let seconds = (millis / 1000) as i64;
        let nanos = ((millis % 1000) * 1_000_000) as u32;
        return chrono::DateTime::from_utc(NaiveDateTime::from_timestamp(seconds, nanos), chrono::Utc);
    }

    fn create_remote_folder(&self, folder: &Folder) {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let folderRemote = FolderRemoteUpload {
            id: &folder.id,
            title: &folder.title,
            parentId: match &folder.parentId {
                Some(v) => Some(v),
                None => None,
            },
            level: folder.level,
            userId: folder.userId,
            createdAt: self.millis_to_datetime(folder.createdAt),
            updatedAt: self.millis_to_datetime(folder.updatedAt),
            deletedAt: None
        };

        client.post(&self.get_endpoint("/api/upload-folder"))
            .headers(headers)
            .json(&folderRemote)
            .send();
    }

    fn update_remote_folder(&self, folder: &Folder) {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let folderRemote = FolderRemoteUpload {
            id: &folder.id,
            title: &folder.title,
            parentId: match &folder.parentId {
                Some(v) => Some(v),
                None => None,
            },
            level: folder.level,
            userId: folder.userId,
            createdAt: self.millis_to_datetime(folder.createdAt),
            updatedAt: self.millis_to_datetime(folder.updatedAt),
            deletedAt: match folder.deletedAt {
                Some(v) => Some(self.millis_to_datetime(v)),
                None => None,
            }
        };

        let textResponse = client.post(&self.get_endpoint("/api/update-folder"))
            .headers(headers)
            .json(&folderRemote)
            .send();
    }

    fn get_local_folders_map(&self) -> HashMap::<String, Folder> {
        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let userId = self.data.lock().unwrap().userId;
        let mut folders_stmt = conn.prepare("SELECT id, parentId, title, level, createdAt, updatedAt, deletedAt FROM folder WHERE userId = ?").unwrap();
        let folders_client_iter = folders_stmt.query_map(params![userId], |row| {
            Ok(Folder {
                id: row.get(0)?,
                parentId: row.get(1)?,
                title: row.get(2)?,
                level: row.get(3)?,
                userId: userId as i32,
                createdAt: row.get(4)?,
                updatedAt: row.get(5)?,
                deletedAt: row.get(6)?
            })
        }).unwrap();

        let mut folders_on_client_map = HashMap::<String, Folder>::new();
        for f in folders_client_iter {
            let folder = f.unwrap();
            folders_on_client_map.insert(folder.id.clone(), folder);
        }

        return folders_on_client_map;
    }
    fn sync_folders(&self, folders_remote: &Vec<SyncFolderInfo>) {
        info!("Syncing folders");
        let mut folders_on_client_map = self.get_local_folders_map();
        let mut folders_to_create = Vec::<&Folder>::new();
        let mut folders_to_update = Vec::<&Folder>::new();

        for remote_folder in folders_remote {
            let parent_id_remote = match &remote_folder.parentId {
                Some(v) => v.clone(),
                None => "".to_string(),
            };

            match folders_on_client_map.get(&remote_folder.id) {
                Some(local_folder) => {
                    let is_local_deleted = local_folder.deletedAt.is_some();
                    let is_remote_deleted = remote_folder.deletedAt.is_some();
                    let remote_folder_timestamp = remote_folder.updatedAt.timestamp_millis();

                    if is_remote_deleted && !is_local_deleted {
                        // Mark local folder as deleted
                        self.mark_folder_as_deleted(&local_folder.id);
                    } if !is_remote_deleted && is_local_deleted {
                        folders_to_update.push(local_folder);
                        // Update remote folder state
                    } else if remote_folder_timestamp > local_folder.updatedAt {
                        // Update local info
                        let time_millis = self.get_time_in_millis();
                        self.connection.lock().unwrap().as_ref().unwrap().execute(
                            "UPDATE folder SET title = ?, parentId, updatedAt = ? WHERE id = ?",
                            params![&remote_folder.title, parent_id_remote, self.get_time_in_millis(),&remote_folder.id]);
                    } else if remote_folder_timestamp < local_folder.updatedAt {
                        // Update remote info
                        folders_to_update.push(local_folder);
                    }

                }
                None => {
                    let deleted_at_millis = match remote_folder.deletedAt {
                        Some(v) => Some(v.timestamp_millis()),
                        None => None,
                    };

                    self.insert_folder(&remote_folder.id, &remote_folder.title, parent_id_remote, remote_folder.level as u32,
                                           remote_folder.userId as u32, remote_folder.createdAt.timestamp_millis(),
                                       remote_folder.updatedAt.timestamp_millis(), deleted_at_millis)
                }
            }
        }

        info!("Searching for new local folders");

        for folder in folders_on_client_map.values() {
            if !folders_remote.iter().any(|v| v.id.eq(&folder.id)) {
                folders_to_create.push(folder);
            }
        }

        info!("Updating {} existing folders on server", folders_to_update.len());

        for folder in folders_to_update {
            self.update_remote_folder(folder);
        }

        info!("Uploading {} new folders on server", folders_to_create.len());

        for folder in folders_to_create {
            self.create_remote_folder(folder);
        }
    }

    fn get_endpoint(&self, path: &str) -> String {
        let data = self.data.lock().unwrap();
        let mut endpoint = data.apiPath.clone();
        endpoint.push_str(path);
        return endpoint;
    }

    fn create_remote_note(&self, note: &Note) {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let noteRemote = NoteRemoteUpload {
            id: &note.id,
            title: &note.title,
            text: &note.text,
            folderId: &note.folderId,
            userId: note.userId,
            level: note.level,
            createdAt: self.millis_to_datetime(note.createdAt),
            updatedAt: self.millis_to_datetime(note.updatedAt),
            deletedAt: None
        };

        client.post(&self.get_endpoint("/api/upload-note"))
            .headers(headers)
            .json(&noteRemote)
            .send();
    }
    fn update_remote_note(&self, note: &Note) {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let noteRemote = NoteRemoteUpload {
            id: &note.id,
            title: &note.title,
            text: &note.text,
            folderId: &note.folderId,
            userId: note.userId,
            level: note.level,
            createdAt: self.millis_to_datetime(note.createdAt),
            updatedAt: self.millis_to_datetime(note.updatedAt),
            deletedAt: match note.deletedAt {
                Some(v) => Some(self.millis_to_datetime(v)),
                None => None,
            }
        };

        client.post(&self.get_endpoint("/api/update-note"))
            .headers(headers)
            .json(&noteRemote)
            .send();
    }

    fn make_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let data = self.data.lock().unwrap();
        let auth_header_value = ["Bearer ", &data.token].concat();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_header_value).unwrap());
        return headers;
    }

    fn get_remote_folder_by_id(&self, id: &str) -> SyncFolderInfo {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let noteResponse: SyncFolderInfo = client.get(&self.get_endpoint(&["/api/folder/", id].concat()))
            .headers(headers)
            .send()
            .unwrap().json().unwrap();
        return noteResponse;
    }

    fn get_remote_note_by_id(&self, id: &str) -> NoteRemote {
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let noteResponse: NoteRemote = client.get(&self.get_endpoint(&["/api/note/", id].concat()))
            .headers(headers)
            .send()
            .unwrap().json().unwrap();
        return noteResponse;
    }

    fn get_local_notes_map(&self) -> HashMap<String, Note> {
        let mut notes_on_client_map = HashMap::<String, Note>::new();
        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let userId = self.data.lock().unwrap().userId;
        let mut notes_stmt = conn.prepare("SELECT id, folderId, title, createdAt, updatedAt, deletedAt, level FROM note WHERE userId = ?").unwrap();
        let notes_client_iter = notes_stmt.query_map(params![userId], |row| {
            Ok(Note {
                id: row.get(0)?,
                folderId: row.get(1)?,
                title: row.get(2)?,
                text: "".to_string(),
                createdAt: row.get(3)?,
                updatedAt: row.get(4)?,
                deletedAt: row.get(5)?,
                level: row.get(6)?,
                userId: userId as i32,
            })
        }).unwrap();

        for n in notes_client_iter {
            let note = n.unwrap();
            notes_on_client_map.insert(note.id.clone(), note);
        }
        return notes_on_client_map;
    }

    fn sync_notes(&self, notes_remote: &Vec<SyncNoteShortInfo>) {
        info!("Syncing notes");
        let mut notes_on_client_map = self.get_local_notes_map();
        let mut notes_to_create = Vec::<&Note>::new();
        let mut notes_to_update = Vec::<&Note>::new();

        for remote_note in notes_remote {
            match notes_on_client_map.get(&remote_note.id) {
                Some(local_note) => {
                    let is_local_deleted = local_note.deletedAt.is_some();
                    let is_remote_deleted = remote_note.deletedAt.is_some();
                    let remote_note_timestamp = remote_note.updatedAt.timestamp_millis();

                    if is_remote_deleted && !is_local_deleted {
                        // Mark local note as deleted
                        self.mark_note_as_deleted(&local_note.id);
                    } if !is_remote_deleted && is_local_deleted {
                        info!("Note with id {} id deleted locally, mark to update", &local_note.id);
                        notes_to_update.push(local_note);
                        // Update remote folder state
                    } else if remote_note_timestamp > local_note.updatedAt {
                        info!("Updating local note {}", &local_note.id);
                        let note = self.get_remote_note_by_id(&remote_note.id);
                        self.connection.lock().unwrap().as_ref().unwrap().execute(
                            "UPDATE note SET title = ?, folderId = ?, text = ?, updatedAt = ? WHERE id = ?",
                            params![&note.title, &note.folderId, &note.text, note.updatedAt.timestamp_millis(), &note.id]
                        ).unwrap();
                        self.update_note_in_search_index(&note.id, &note.title, &note.text, &note.folderId);
                        // Update local info
                    } else if remote_note_timestamp < local_note.updatedAt {
                        // Update remote info
                        notes_to_update.push(local_note);
                    }

                }
                None => {
                    if remote_note.deletedAt.is_none() {
                        info!("Remote note {} is not found locally, uploading", &remote_note.id);
                        let note = self.get_remote_note_by_id(&remote_note.id);
                        let user_id = self.data.lock().unwrap().userId;
                        let inserted = self.insert_note(&note.id, &note.title, &note.folderId, &note.text, user_id, 0,
                                         note.createdAt.timestamp_millis(), note.updatedAt.timestamp_millis());
                        if inserted {
                            self.add_note_to_search_index(&note.id, &note.title, &note.text, &note.folderId);
                        }
                    }
                }
            }
        }

        info!("Searching for new local notes");

        for note in notes_on_client_map.values() {
            if !notes_remote.iter().any(|v| v.id.eq(&note.id)) {
                notes_to_create.push(note);
            }
        }

        info!("Updating {} existing notes on server", notes_to_update.len());

        for note in notes_to_update {
            let note_full_info = self.get_local_note_by_id(&note.id).unwrap();
            self.update_remote_note(&note_full_info);
        }

        info!("Uploading {} new notes to server", notes_to_create.len());

        for note in notes_to_create {
            let note_full_info = self.get_local_note_by_id(&note.id).unwrap();
            self.create_remote_note(&note_full_info);
        }
    }

    fn handle_synchronize(&self) -> Vec<u8> {
        info!("Synchronization started");

        info!("Retrieving sync data from server");
        let client = reqwest::blocking::Client::new();
        let mut headers = self.make_headers();

        let syncDataHolderResponseResult = client.get(&self.get_endpoint("/api/latest-sync-data"))
            .headers(headers)
            .send();

        let syncDataHolderResponse: SyncDataHolderResponse = match syncDataHolderResponseResult {
            Ok(result)  => result.json().unwrap(),
            Err(e) => {
                let mut res = proto::messages::EmptyResultResponse::new();
                res.success = false;
                return res.write_to_bytes().unwrap();
            },
        };

        info!("Sync data retrieval finished");

        self.sync_folders(&syncDataHolderResponse.folders);
        self.sync_notes(&syncDataHolderResponse.notes);

        info!("Synchronize finished");
        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_get_last_login_data(&self) -> Vec<u8> {
        info!("get_last_login_data");

        let data = self.data.lock().unwrap();
        let mut res = proto::messages::GetLastLoginDataResponse::new();
        res.token = data.token.clone();
        res.email = data.email.clone();
        res.userId = data.userId as i32;
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_get_root_folder(&self) -> Vec<u8> {
        info!("get_root_folder");
        let userId = self.data.lock().unwrap().userId;
        let folder = self.connection.lock().unwrap().as_ref().unwrap().query_row("SELECT id, title, createdAt, updatedAt FROM folder WHERE userId = ?", params![userId], |row| {
            Ok(Folder {
                id: row.get(0)?,
                title: row.get(1)?,
                level: 0,
                parentId: None,
                userId: userId as i32,
                createdAt: row.get(2)?,
                updatedAt: row.get(3)?,
                deletedAt: None
            })
        }).unwrap();

        let mut res = proto::messages::GetRootFolderResponse::new();
        res.folderId = folder.id;
        res.title = folder.title;
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_get_all_folders(&self) -> Vec<u8> {
        let guard = self.connection.lock().unwrap();
        let conn = guard.as_ref().unwrap();
        let data = self.data.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, parentId, title, level, createdAt, updatedAt FROM folder WHERE deletedAt IS NULL AND userId = ?").unwrap();
        let folders_iter = stmt.query_map(params![data.userId], |row| {
            Ok(Folder {
                id: row.get(0)?,
                parentId: row.get(1)?,
                title: row.get(2)?,
                level: row.get(3)?,
                userId: data.userId as i32,
                createdAt: row.get(4)?,
                updatedAt: row.get(5)?,
                deletedAt: None,
            })
        }).unwrap();

        let mut res = proto::messages::GetFoldersListResponse::new();
        res.success = true;

        let mut folders_list_proto = protobuf::RepeatedField::<proto::messages::Folder>::new();

        for n in folders_iter {
            let folder = n.unwrap();
            let mut nsi = proto::messages::Folder::new();
            nsi.id = folder.id;
            nsi.parentId = match folder.parentId {
                Some(v) => v,
                None => "".to_string(),
            };
            nsi.title = folder.title;
            nsi.level = folder.level as i32;
            nsi.createdAt = folder.createdAt;
            nsi.updatedAt = folder.updatedAt;
            folders_list_proto.push(nsi);
        }

        res.set_folders(folders_list_proto);

        return res.write_to_bytes().unwrap();
    }


    fn handle_update_folder(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::UpdateFolder>(&command_data).unwrap();
        info!("Update folder with name {}", parsed.title);

        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        return res.write_to_bytes().unwrap();
    }

    fn handle_update_note(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::UpdateNote> (&command_data).unwrap();
        info!("Update note with id {}", parsed.id);

        let time_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        self.connection.lock().unwrap().as_ref().unwrap().execute(
            "UPDATE note SET title = ?, folderId = ?, text = ?, updatedAt = ? WHERE id = ?",
            params![&parsed.title, &parsed.folderId, &parsed.text, time_millis, &parsed.id]
        ).unwrap();

        self.update_note_in_search_index(&parsed.id, &parsed.title, &parsed.text, &parsed.folderId);
        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;        
        
        return res.write_to_bytes().unwrap();
    }

    fn handle_get_notes_by_search(&self,command_data: &[u8]) -> Vec<u8> {
        let parsed = parse_from_bytes::<proto::messages::SearchNotes>(&command_data).unwrap();
        info!("Search by phrase {}", parsed.query);

        let ir_guard = self.index_reader.lock().unwrap();
        let reader = ir_guard.as_ref().unwrap();
        let searcher = reader.searcher();

        let fields_map = self.fields.read().unwrap();
        let id_field = fields_map.get("id").unwrap();
        let user_id_field = fields_map.get("userId").unwrap();
        let folder_id_field = fields_map.get("folderId").unwrap();

        let ix_guard = self.index.lock().unwrap();

        // in both title and body.
        let query_parser = QueryParser::for_index(ix_guard.as_ref().unwrap(), vec![*fields_map.get("title").unwrap(), *fields_map.get("text").unwrap()]);

        let str_query_result = query_parser.parse_query(&parsed.query);
        if str_query_result.is_err() {
            info!("Error parsing query {}", &parsed.query);
            let mut res = proto::messages::GetNotesListResponse::new();
            res.success = false;
            return res.write_to_bytes().unwrap();
        }

        let str_query = str_query_result.unwrap();

        let userId = self.data.lock().unwrap().userId;
        let id_term = Term::from_field_text(*user_id_field, &userId.to_string());
        let user_id_query: Box<dyn Query> = Box::new(TermQuery::new(id_term.clone(), IndexRecordOption::Basic));
        
        let combined_query = if parsed.folderId.len() > 0 {

            let folder_id_term = Term::from_field_text(*folder_id_field, &parsed.folderId.to_string());
            let folder_id_query: Box<dyn Query> = Box::new(TermQuery::new(folder_id_term.clone(), IndexRecordOption::Basic));

            BooleanQuery::from(vec![
                (Occur::Must, user_id_query),
                (Occur::Must, folder_id_query),
                (Occur::Must, str_query),
            ])
        } else {
            BooleanQuery::from(vec![
                (Occur::Must, user_id_query),
                (Occur::Must, str_query),
            ])
        };

        info!("Making search");

        let top_docs = searcher.search(&combined_query, &TopDocs::with_limit(100)).unwrap();

        info!("Search for {} finished, making response", parsed.query);
        let mut res = proto::messages::GetNotesListResponse::new();
        res.success = true;

        let mut notes_list_proto = protobuf::RepeatedField::<proto::messages::NoteShortInfo>::new();

        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address).unwrap();
            let field_values = retrieved_doc.field_values();
            let note_id = field_values.into_iter().find(|x| x.field() == *id_field).unwrap().value().text().unwrap();

            let note_result = self.get_local_note_by_id(note_id);
            if  note_result.is_none() {
                info!("Cannot find note by id {}", note_id);
                continue;
            }

            let note = note_result.unwrap();

            let mut nsi = proto::messages::NoteShortInfo::new();
            nsi.id = note.id;
            nsi.folderId = note.folderId;
            nsi.title = note.title;
            nsi.createdAt = note.createdAt;
            nsi.updatedAt = note.updatedAt;
            notes_list_proto.push(nsi);
        }

        res.set_notes(notes_list_proto);

        return res.write_to_bytes().unwrap();
    }

    fn handle_unrecognized(&self) -> Vec<u8> {
        info!("Unrecognized command");
        return Vec::new();
    }

    fn handle(&self, command: i8, data: &[u8], size: usize) -> Vec<u8>{
        match command {      
            1 => self.handle_init(&data),
            2 => self.handle_create_note(&data),
            3 => self.handle_get_notes_by_folder(&data),
            4 => self.handle_set_token(&data),
            5 => self.handle_get_note_by_id(&data),
            6 => self.handle_get_folder_by_id(&data),
            7 => self.handle_synchronize(),
            8 => self.handle_login(&data),
            9 => self.handle_get_last_login_data(),
            10 => self.handle_get_root_folder(),
            11 => self.handle_get_all_folders(),   
            12 => self.handle_get_all_notes(&data),
            13 => self.handle_create_folder(&data),
            14 => self.handle_update_note(&data),
            15 => self.handle_update_folder(&data),
            16 => self.handle_remove_note(&data),
            17 => self.handle_remove_folder(&data),
            18 => self.handle_get_notes_by_search(&data),
            19 => self.handle_register(&data),
            _ => self.handle_unrecognized(),
        }        
    }

    fn set_active(&self, value: bool) {
        let data = self.data.lock().unwrap().active = value;
    }
}

impl AsyncWorker {
    fn init(&self) {
        self.inner.init();
    }

    fn handle(&self, command: i8, data: &[u8], size: usize) -> Vec<u8> {
        return self.inner.handle(command, data,size);
    }

    fn handle_async(&self, command: i8, data: &[u8], size: usize) -> Vec<u8> {
        let mut res = proto::messages::EmptyResultResponse::new();
        res.success = true;

        let mut local_self = self.inner.clone();
        let shared_data = Arc::new(Mutex::new(data.to_vec())).clone();
        let sender = Arc::clone(&self.sender);
        thread::spawn(move || {
            let result = local_self.handle(command, &shared_data.lock().unwrap(), size);
            sender.lock().unwrap().send(result);
        });

        return res.write_to_bytes().unwrap();
    }
}

lazy_static! {
    pub static ref WORKER: AsyncWorker = {
        let (tx, rx) = mpsc::channel();
        let mut worker = AsyncWorker {
            inner : Arc::new(Worker {
                data : Mutex::new( WorkerData {
                    active: false,
                    token: "".to_string(),
                    apiPath: "".to_string(),
                    email: "".to_string(),
                    userId: 0,
                }),
                connection: Mutex::new(None),
                index: Mutex::new(None),
                index_reader: Mutex::new(None),
                index_writer: Mutex::new(None),
                fields: RwLock::new(HashMap::<String, tantivy::schema::Field>::new()),
                }),
            sender: Arc::new(Mutex::new(tx)),
            receiver: Arc::new(Mutex::new(rx)),
        };
        worker.init();
        worker
    };
}


pub fn handle_command(command: i8, data: &[u8], size: usize) -> Vec<u8> {        
    return WORKER.handle(command, data, size);
}

pub fn handle_async_command(command: i8, data: &[u8], size: usize) -> Vec<u8> {
    return WORKER.handle_async(command, data, size);
}