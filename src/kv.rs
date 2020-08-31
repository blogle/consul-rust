use std::collections::HashMap;
use std::rc::Rc;

use crate::errors::Error;
use crate::errors::Result;
use crate::request::{delete, get, get_vec, put};
use crate::{Client, QueryMeta, QueryOptions, WriteMeta, WriteOptions};

use serde::de::DeserializeOwned;
use serde::Serialize;

#[serde(default)]
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize, Debug)]
struct KVResult {
    Key: String,
    CreateIndex: Option<u64>,
    ModifyIndex: Option<u64>,
    LockIndex: Option<u64>,
    Flags: Option<u64>,
    Value: String,
    Session: Option<String>,
}

#[derive(Clone, Default)]
pub struct KVPair<T> {
    pub Key: String,
    pub CreateIndex: Option<u64>,
    pub ModifyIndex: Option<u64>,
    pub LockIndex: Option<u64>,
    pub Flags: Option<u64>,
    pub Value: Rc<T>,
    pub Session: Option<String>,
}


impl<T> From<KVResult> for KVPair<T> where T: DeserializeOwned {
    fn from(result: KVResult) -> KVPair<T> {

        let bytes = base64::decode(&result.Value).unwrap();
        let decoded = std::str::from_utf8(&bytes).unwrap();
        let value = serde_json::from_str(&decoded).unwrap();

        KVPair {
            Key: result.Key,
            CreateIndex: result.CreateIndex,
            ModifyIndex: result.ModifyIndex,
            LockIndex: result.LockIndex,
            Flags: result.Flags,
            Value: Rc::new(value),
            Session: result.Session,
        }
    }
}

pub trait KV {

    fn acquire<T: Serialize>(
        &self,
        _: &KVPair<T>,
        _: Option<&WriteOptions>,
    ) -> Result<(bool, WriteMeta)>;

    fn delete(&self, _: &str, _: Option<&WriteOptions>) -> Result<(bool, WriteMeta)>;

    fn get<T: DeserializeOwned>(&self, _: &str, _: Option<&QueryOptions>) -> Result<(Option<KVPair<T>>, QueryMeta)>;

    //fn list<T: DeserializeOwned>(&self, _: &str, _: Option<&QueryOptions>) -> Result<(Vec<KVPair<T>>, QueryMeta)>;

    fn put<T: Serialize>(
        &self,
        _: &KVPair<T>,
        _: Option<&WriteOptions>,
    ) -> Result<(bool, WriteMeta)>;

    fn release<T: Serialize>(
        &self,
        _: &KVPair<T>,
        _: Option<&WriteOptions>,
    ) -> Result<(bool, WriteMeta)>;
}

impl KV for Client {
    fn acquire<T>(&self, pair: &KVPair<T>, o: Option<&WriteOptions>) -> Result<(bool, WriteMeta)>
    where
        T: Serialize,
    {
        let mut params = HashMap::new();
        if let Some(i) = pair.Flags {
            if i != 0 {
                params.insert(String::from("flags"), i.to_string());
            }
        }
        if let Some(ref session) = pair.Session {
            params.insert(String::from("acquire"), session.to_owned());
            let path = format!("/v1/kv/{}", pair.Key);
            put(&path, Some(&pair.Value), &self.config, params, o)
        } else {
            Err(Error::from("Session flag is required to acquire lock"))
        }
    }

    fn delete(&self, key: &str, options: Option<&WriteOptions>) -> Result<(bool, WriteMeta)> {
        let path = format!("/v1/kv/{}", key);
        delete(&path, &self.config, HashMap::new(), options)
    }

    fn get<T>(
        &self,
        key: &str,
        options: Option<&QueryOptions>,
    ) -> Result<(Option<KVPair<T>>, QueryMeta)>
        where T: DeserializeOwned
    {
        let path = format!("/v1/kv/{}", key);
        let response: Result<(Vec<KVResult>, QueryMeta)> = get(&path, &self.config, HashMap::new(), options);
        response.map(|(results, meta)| {
            let maybe_first = results.first();
            let kv_pair = maybe_first.map(|kv_result| {
                let owned_result = kv_result.clone();
                owned_result.into()
            });


            (kv_pair, meta)
        })
    }

    //fn list<T>(&self, prefix: &str, o: Option<&QueryOptions>) -> Result<(Vec<KVPair<T>>, QueryMeta)>
    //    where T: DeserializeOwned
    //{
    //    let mut params = HashMap::new();
    //    params.insert(String::from("recurse"), String::from(""));
    //    let path = format!("/v1/kv/{}", prefix);
    //    let response: Result<(Vec<KVResult>, QueryMeta)> = get(&path, &self.config, params, o);
    //    response.map(|(results, meta)| {
    //        let key_values = results.iter()
    //            .map(|el| el.clone().into())
    //            .collect();

    //        (key_values, meta)
    //    })
    //}

    fn put<T>(&self, pair: &KVPair<T>, o: Option<&WriteOptions>) -> Result<(bool, WriteMeta)>
    where
        T: Serialize,
    {
        let mut params = HashMap::new();
        if let Some(i) = pair.Flags {
            if i != 0 {
                params.insert(String::from("flags"), i.to_string());
            }
        }
        let path = format!("/v1/kv/{}", pair.Key);
        put(&path, Some(&pair.Value), &self.config, params, o)
    }

    fn release<T>(&self, pair: &KVPair<T>, o: Option<&WriteOptions>) -> Result<(bool, WriteMeta)>
    where
        T: Serialize,
    {
        let mut params = HashMap::new();
        if let Some(i) = pair.Flags {
            if i != 0 {
                params.insert(String::from("flags"), i.to_string());
            }
        }
        if let Some(ref session) = pair.Session {
            params.insert(String::from("release"), session.to_owned());
            let path = format!("/v1/kv/{}", pair.Key);
            put(&path, Some(&pair.Value), &self.config, params, o)
        } else {
            Err(Error::from("Session flag is required to release a lock"))
        }
    }
}
