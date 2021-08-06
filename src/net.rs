use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Get(Result<Option<String>, String>),
    Set(Result<(), String>),
    Remove(Result<(), String>),
}

impl Response {
    pub fn set(result: Result<(), String>) -> Self {
        Response::Set(result)
    }

    pub fn get(result: Result<Option<String>, String>) -> Self {
        Response::Get(result)
    }

    pub fn remove(result: Result<(), String>) -> Self {
        Response::Remove(result)
    }
}
