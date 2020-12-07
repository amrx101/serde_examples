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
use std::io::{self, prelude::*, BufReader};
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions, Notification};
use std::{thread, time::Duration};
// use avrow::{from_value as fv, Codec as cc, Reader as rr, Schema as sc, Writer as w, Record as Rr};


use serde::de::{self, Deserializer};

#[derive(Debug, From)]
pub enum MyError{
    SerdeSerializer(String),
}

use flate2::Compression;
use flate2::write::DeflateEncoder;


// These are Rust 
#[derive(Debug, Serialize, Deserialize)]
pub struct G2Data {
    #[serde(default)]
    can_id: Option<String>,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default, deserialize_with="from_str_optional")]
    timestamp: Option<f64>,
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
    #[serde(default)]
    LAT_DEG: Option<String>,
    #[serde(default)]
    LON_DEG: Option<String>,
    #[serde(default)]
    ALT_M: Option<String>,
    #[serde(default)]
    Accuracy:Option<String>,
    #[serde(default)]
    incognito_mode: Option<String>

}

#[derive(Debug, Serialize, Deserialize)]
pub struct G2DataRes {
    #[serde(default)]
    can_id: Option<String>,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    timestamp: Option<f64>,
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
    #[serde(default)]
    LAT_DEG: Option<String>,
    #[serde(default)]
    LON_DEG: Option<String>,
    #[serde(default)]
    ALT_M: Option<String>,
    #[serde(default)]
    Accuracy:Option<String>,
    #[serde(default)]
    incognito_mode:Option<String>

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
            return Ok(None);
        },
        Err(_) => Ok(None)
    }
}

fn get_can_data()-> G2Data {
    let can_data = r#"
        {
            "key": "BMS_Cell3",
            "value": "3.5231",
            "timestamp": "1589441481.885"
        }
    "#;
    serde_json::from_str(can_data).unwrap()
}

fn get_imu_data() -> G2Data {
    let imu_data = r#"{
        "ACC_X_MPS2": "33213.322",
        "ACC_Y_MPS2": "323.909803",
        "ACC_Z_MPS2": "2121.443",
        "GYR_X_DEG": "2323.111",
        "GYR_Y_DEG": "223.11274",
        "GYR_Z_DEG": "3434.2211"
    }"#;
    serde_json::from_str(imu_data).unwrap()
}
// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&code=use%20serde%3A%3ADeserialize%3B%0Ause%20serde_json%3A%3Ajson%3B%0Ause%20std%3A%3Acollections%3A%3ABTreeMap%3B%0A%0A%23%5Bderive(Deserialize%2C%20Debug)%5D%0Astruct%20User%20%7B%0A%20%20%20%20fingerprint%3A%20Option%3CString%3E%2C%0A%20%20%20%20location%3A%20String%2C%0A%7D%0A%0Afn%20main()%20%7B%0A%20%20%20%20let%20mut%20m%3A%20BTreeMap%3CString%2C%20String%3E%20%3D%20BTreeMap%3A%3Anew()%3B%0A%20%20%20%20m.insert(%22fingerprint%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20m.insert(%22location%22.to_owned()%2C%20%22aa%22.to_owned())%3B%0A%20%20%20%20%2F%2F%20The%20type%20of%20%60j%60%20is%20%60serde_json%3A%3AValue%60%0A%20%20%20%20let%20j%20%3D%20json!(%7B%0A%20%20%20%20%20%20%20%20%2F%2F%20%22fingerprint%22%3A%20%220xF9BA143B95FF6D82%22%2C%0A%20%20%20%20%20%20%20%20%22location%22%3A%20%22Menlo%20Park%2C%20CA%22%0A%20%20%20%20%7D)%3B%0A%20%20%20%20%0A%20%20%20%20let%20d%20%3D%20json!(m)%3B%0A%0A%20%20%20%20let%20u%3A%20User%20%3D%20serde_json%3A%3Afrom_value(j).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20u)%3B%0A%20%20%20%20let%20du%3A%20User%20%3D%20serde_json%3A%3Afrom_value(d).unwrap()%3B%0A%20%20%20%20println!(%22%7B%3A%23%3F%7D%22%2C%20du)%3B%0A%7D
// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=f265edc1b9e5fd4485a83da40fd01785

fn get_digital_twin_data()-> G2Data{
    let mot_data = r#"{
        "key": "MOT_NPres_RPM",
        "value": "121.12",
        "timestamp": "1589441481.885"
    }"#;
    serde_json::from_str(mot_data).unwrap()
}

fn get_dt_soc_data() -> Vec<G2Data>{
    let mut res:Vec<G2Data> = Vec::new();
    let cell_1 = r#"{
        "key": "BMS_Cell_Temp_2",
        "value": "1232.32",
        "timestamp": "1589441481.885"
    }"#;
    let v1 = serde_json::from_str(cell_1).unwrap();
    res.push(v1);
    let cell_2 = r#"{
        "key": "BMS_Cell_Temp_3",
        "value": "1232.32",
        "timestamp": "1589441481.885"
    }"#;
    let v2 = serde_json::from_str(cell_2).unwrap();
    res.push(v2);
    res
}

fn get_scheme_config() -> Schema {
    let mut file = File::open("/home/amit/rust_samples/avr_ser/ge2.avsc").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let schema = Schema::parse_str(&contents).unwrap();
    schema
}


fn tt() -> Result<Vec<u8>, MyError> {
    let schema = get_scheme_config();
    let vv = schema.canonical_form();

    let mut res: Vec<u8> = Vec::new();
    
    // This is the codec writer. It consumes a schema, a place holder and a Codec.
    let mut codec_writer = Writer::with_codec(&schema, res, Codec::Deflate);

    let now = Instant::now();
    // create 10 records based on the string and ask codec to keep it for future.
    for n in 1..10 {
        let v: G2Data = get_digital_twin_data();
        match codec_writer.append_ser(v) {
            Ok(f) => f,
            Err(e) => return Err(MyError::SerdeSerializer(e.to_string()))
        };
    }
    let soc_data = get_dt_soc_data();
    for data in soc_data {
        match codec_writer.append_ser(data) {
            Ok(f) => f,
            Err(e) => return Err(MyError::SerdeSerializer(e.to_string()))
        };
    }


    // do the serialization and compression.
    match codec_writer.flush() {
        Ok(v) => v,
        Err(e) => return Err(MyError::SerdeSerializer(e.to_string()),)
    };
    let elasped = now.elapsed();

    // serilalized bytes
    let ec = codec_writer.into_inner();
    Ok(ec)
}

fn compress_deflate(uncompressed_buffer: &[u8]) -> Vec<u8> {
	let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
	e.write(uncompressed_buffer).unwrap();
	e.finish().expect("Deflate: Failed to compress data")
}

fn main(){
    let schema = get_scheme_config();
    let broker = "172.19.0.166";
    let port = 1883;

    let reconnection_options = ReconnectOptions::Always(10);
    let mqtt_options = MqttOptions::new("test-pubsub2", broker, port)
                                    .set_keep_alive(10)
                                    .set_inflight(3)
                                    .set_request_channel_capacity(3)
                                    .set_reconnect_opts(reconnection_options)
                                    .set_clean_session(false);

    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).unwrap();
    mqtt_client.subscribe("/devices/digital_twin/events/v1/buffered_channel", QoS::AtLeastOnce).unwrap();

    thread::spawn(move || {
        loop {
            let payload = tt().unwrap();
            thread::sleep(Duration::from_millis(1000));
            mqtt_client.publish("/devices/digital_twin/events/v1/buffered_channel", QoS::AtLeastOnce, false, payload).unwrap();
        }
    });

    for notification in notifications {

        match notification {
            Notification::Publish(v) => {
                let payload = v.payload;
                println!("payload={:?}", payload);

            },
            _ => println!("{:?}", notification),
        };
    }

}


