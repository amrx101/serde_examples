use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::prelude::*;
use std::collections::{HashMap, BTreeMap};
use serde_json::{Value, json};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Instant;
use derive_more::From;

use serde::de::{self, Deserializer};

#[derive(Debug, From)]
pub enum MyError{
    SerdeSerializer(String),
}


#[derive(Debug, Serialize, Deserialize)]
pub struct G2Data {
    #[serde(default, deserialize_with="from_str_optional")]
    can_id: Option<i32>,
    #[serde(default)]
    data: Option<String>,
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
    #[serde(default, deserialize_with="from_str_optional")]
    ttff_s: Option<f32>,
    error_code: Option<String>,
    #[serde(default, deserialize_with="from_str_optional")]
    is_valid: Option<i32>,
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_X_MPS2: Option<f32>,
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_Y_MPS2: Option<f32>,
    #[serde(default, deserialize_with="from_str_optional")]
    ACC_Z_MPS2: Option<f32>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_X_DEG: Option<f32>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_Y_DEG: Option<f32>,
    #[serde(default, deserialize_with="from_str_optional")]
    GYR_Z_DEG: Option<f32>,
    same: f32,

}

#[derive(Debug, Serialize, Deserialize)]
pub struct G2DataRes {
    #[serde(default)]
    can_id: Option<i32>,
    #[serde(default)]
    data: Option<String>,
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
    ttff_s: Option<f32>,
    error_code: Option<String>,
    #[serde(default)]
    is_valid: Option<i32>,
    #[serde(default)]
    ACC_X_MPS2: Option<f32>,
    #[serde(default)]
    ACC_Y_MPS2: Option<f32>,
    #[serde(default)]
    ACC_Z_MPS2: Option<f32>,
    #[serde(default)]
    GYR_X_DEG: Option<f32>,
    #[serde(default)]
    GYR_Y_DEG: Option<f32>,
    #[serde(default)]
    GYR_Z_DEG: Option<f32>,
    same: f32,

}


fn from_str_optional<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where T: FromStr,
          T::Err: Display,
          D: serde::Deserializer<'de>
{
    // println!("{:?}", deserializer);
    let deser_res: Result<Value, _> = serde::Deserialize::deserialize(deserializer);
    println!("{:?}", deser_res);
    match deser_res {
        Ok(Value::String(s)) => T::from_str(&s).map_err(serde::de::Error::custom).map(Option::from),
        Ok(v) => {
            println!("strssssing expected but found something else: {}", v);
            // return Ok(Value::Number(s));
            return Ok(None);
        },
        Err(_) => Ok(None)
    }
}

// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&code=use%20serde%3A%3ADeserialize%3B%0Ause%20serde_json%3A%3Ajson%3B%0Ause%20std%3A%3Acollections%3A%3ABTreeMap%3B%0A%0A%23%5Bderive(Deserialize%2C%20Debug)%5D%0Astruct%20User%20%7B%0A%20%20%20%20fingerprint%3A%20Option%3CString%3E%2C%0A%20%20%20%20location%3A%20String%2C%0A%7D%0A%0Afn%20main()%20%7B%0A%20%20%20%20let%20mut%20m%3A%20BTreeMap%3CString%2C%20String%3E%20%3D%20BTreeMap%3A%3Anew()%3B%0A%20%20%20%20m.insert(%22fingerprint%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20m.insert(%22location%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20%2F%2F%20The%20type%20of%20%60j%60%20is%20%60serde_json%3A%3AValue%60%0A%20%20%20%20let%20j%20%3D%20json!(%7B%0A%20%20%20%20%20%20%20%20%2F%2F%20%22fingerprint%22%3A%20%220xF9BA143B95FF6D82%22%2C%0A%20%20%20%20%20%20%20%20%22location%22%3A%20%22Menlo%20Park%2C%20CA%22%0A%20%20%20%20%7D)%3B%0A%20%20%20%20%0A%20%20%20%20let%20d%20%3D%20json!(m)%3B%0A%0A%20%20%20%20let%20u%3A%20User%20%3D%20serde_json%3A%3Afrom_value(j).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20u)%3B%0A%20%20%20%20let%20du%3A%20User%20%3D%20serde_json%3A%3Afrom_value(d).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20du)%3B%0A%7D
// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=f265edc1b9e5fd4485a83da40fd01785




fn tt() -> Result<Vec<u8>, MyError> {
    println!("Hello world");
    // can_raw
    let mut file = File::open("/home/amit/rust_samples/avr_ser/ge2.avsc").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let schema = Schema::parse_str(&contents).unwrap();
    let vv = schema.canonical_form();
    // println!("{}", vv);

    let mut res: Vec<u8> = Vec::new();
    
    let mut codec_writer = Writer::with_codec(&schema, res, Codec::Deflate);
    let data = r#"
        {

            "mender_artifact_ver": "11",
            "ACC_X_MPS2": "99.6",
            "value": "wkkw",
            "same": 43.4,
            "ACC_Y_MPS2": "100"
        }"#;
    // let rr:BTreeMap<String, String> = serde_json::from_str(data).unwrap();
    // let j = json!(rr);
    // let vv: G2Data = serde_json::from_value(j).unwrap();
    // println!("{:?}", vv);
    // codec_writer.append_ser(vv).unwrap();
    // codec_writer.flush().unwrap();

    let now = Instant::now();
    for n in 1..10 {
        let v: G2Data = serde_json::from_str(data).unwrap();
        // println!("v==={:?}", v);
        match codec_writer.append_ser(v) {
            Ok(f) => f,
            Err(e) => return Err(MyError::SerdeSerializer(e.to_string()))
        };
    }
    match codec_writer.flush() {
        Ok(v) => v,
        Err(e) => return Err(MyError::SerdeSerializer(e.to_string()),)
    };
    let elasped = now.elapsed();
    println!("EL{:?}", elasped);
    let ec = codec_writer.into_inner();
    let l_e = ec.len();
    println!("len with compression={:?}", l_e);
    // println!("{:?}", std::any::TypeId::of::<ec>());
    // println!("{:?}", res.len());
    let reader = Reader::with_schema(&schema, &ec[..]).unwrap();
    println!("All good");

    for record in reader {
        println!("{:?}", from_value::<G2DataRes>(&record.unwrap()));
    }
    Ok(ec)

}

fn main(){
    let dd = tt().unwrap();
    println!("{:?}", dd.len());
}