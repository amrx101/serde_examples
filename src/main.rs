use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::prelude::*;
use std::collections::{HashMap, BTreeMap};
use serde_json::Value;
use std::fmt::Display;
use std::str::FromStr;

use serde::de::{self, Deserializer};

#[derive(Debug, Serialize, Deserialize)]
pub struct G2Data {
    #[serde(default, deserialize_with="from_str_optional")]
    can_id: Option<i32>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default, deserialize_with="from_str_optional")]
    timestamp: Option<i32>,
    #[serde(default)]
    start_timestamp: Option<String>,
    #[serde(default)]
    end_timestamp: Option<String>,
    #[serde(default)]
    release_name: Option<String>,
    #[serde(default)]
    bms_version: Option<String>,
    #[serde(default)]
    charger_version: Option<String>,
    #[serde(default)]
    sim_ccid: Option<String>,
    #[serde(default)]
    sim_cimi: Option<String>,
    #[serde(default)]
    mender_artifact_ver: Option<String>,
    #[serde(default)]
    mcu_version: Option<String>,
    #[serde(default)]
    vin: Option<String>,
    #[serde(default)]
    bike_type: Option<String>,
    #[serde(default)]
    motor_version: Option<String>,
    #[serde(default)]
    system_boot_time: Option<String>,
    #[serde(default, deserialize_with="from_str_optional")]
    mode: Option<i32>,
    error_code: Option<String>,
    #[serde(default, deserialize_with="from_str_optional")]
    is_valid: Option<i32>,
    #[serde(rename = "manifest-version")]
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_X_MPS2: Option<f64>,
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_Y_MPS2: Option<f64>,
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_Z_MPS2: Option<f64>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_X_DEG: Option<f64>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_Y_DEG: Option<f64>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_Z_DEG: Option<f64>
}

fn from_str_optional<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where T: FromStr,
          T::Err: Display,
          D: serde::Deserializer<'de>
{
    // println!("{:?}", deserializer);
    let deser_res: Result<Value, _> = serde::Deserialize::deserialize(deserializer);
    match deser_res {
        Ok(Value::String(s)) => T::from_str(&s).map_err(serde::de::Error::custom).map(Option::from),
        Ok(v) => {
            println!("string expected but found something else: {}", v);
            return Ok(None);
        },
        Err(_) => Ok(None)
    }
}

// fn from_str_long<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
// where T: FromStr,
//       T::Err: Display,
//       D: serde::Deserializer<'de>
// {
    
// }

// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&code=use%20serde%3A%3ADeserialize%3B%0Ause%20serde_json%3A%3Ajson%3B%0Ause%20std%3A%3Acollections%3A%3ABTreeMap%3B%0A%0A%23%5Bderive(Deserialize%2C%20Debug)%5D%0Astruct%20User%20%7B%0A%20%20%20%20fingerprint%3A%20Option%3CString%3E%2C%0A%20%20%20%20location%3A%20String%2C%0A%7D%0A%0Afn%20main()%20%7B%0A%20%20%20%20let%20mut%20m%3A%20BTreeMap%3CString%2C%20String%3E%20%3D%20BTreeMap%3A%3Anew()%3B%0A%20%20%20%20m.insert(%22fingerprint%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20m.insert(%22location%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20%2F%2F%20The%20type%20of%20%60j%60%20is%20%60serde_json%3A%3AValue%60%0A%20%20%20%20let%20j%20%3D%20json!(%7B%0A%20%20%20%20%20%20%20%20%2F%2F%20%22fingerprint%22%3A%20%220xF9BA143B95FF6D82%22%2C%0A%20%20%20%20%20%20%20%20%22location%22%3A%20%22Menlo%20Park%2C%20CA%22%0A%20%20%20%20%7D)%3B%0A%20%20%20%20%0A%20%20%20%20let%20d%20%3D%20json!(m)%3B%0A%0A%20%20%20%20let%20u%3A%20User%20%3D%20serde_json%3A%3Afrom_value(j).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20u)%3B%0A%20%20%20%20let%20du%3A%20User%20%3D%20serde_json%3A%3Afrom_value(d).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20du)%3B%0A%7D
// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=f265edc1b9e5fd4485a83da40fd01785


fn main() {
    println!("Hello world");
    // can_raw
    let data = r#"
        {

            "mender_artifact_ver": "11",
            "ACC_X_MPS2": "99.6",
            "value": "wkkw"
        }"#;
    let v: G2Data = serde_json::from_str(data).unwrap();
    println!("v is {:?}", v);

}