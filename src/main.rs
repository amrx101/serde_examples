use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::prelude::*;
use std::collections::{HashMap, BTreeMap};
use serde_json;

#[derive(Debug, Serialize, Deserialize)]
pub struct G2Data {
    #[serde(default)]
    can_id: Option<i32>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
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
    #[serde(default)]
    mode: Option<i32>,
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    is_valid: Option<i32>,
    #[serde(default)]
    ACC_X_MPS2: Option<f64>,
    #[serde(default)]
    ACC_Y_MPS2: Option<f64>,
    #[serde(default)]
    ACC_Z_MPS2: Option<f64>,
    #[serde(default)]
    GYR_X_DEG: Option<f64>,
    #[serde(default)]
    GYR_Y_DEG: Option<f64>,
    #[serde(default)]
    GYR_Z_DEG: Option<f64>,
}

fn main() {
    println!("Hello world");
    let data = r#"
        {
            "can_id": 11
        }"#;
    let v: G2Data = serde_json::from_str(data).unwrap();
    println!("v is {:?}", v);

}