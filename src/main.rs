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


// These are RustLang specific struct of our Data. Python equivalent would be a class 
// with these attributes.
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

// Same comment as above.
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

/// Ignore this.
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

/// Sample MOT_NPres_RPM data, key, value and timestamp
fn get_digital_twin_data()-> G2Data{
    let mot_data = r#"{
        "key": "MOT_NPres_RPM",
        "value": "121.12",
        "timestamp": "1589441481.885"
    }"#;
    serde_json::from_str(mot_data).unwrap()
}

/// Sample SOCData. key, value and timestamp.
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

/// Parse Apache Avro config file and create a Schema Object.
fn get_scheme_config() -> Schema {
    let mut file = File::open("/home/amit/rust_samples/avr_ser/ge2.avsc").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let schema = Schema::parse_str(&contents).unwrap();
    schema
}


/// This function creates a serialized payload.
fn get_serialized_payload() -> Result<Vec<u8>, MyError> {
    let schema = get_scheme_config();
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

/// This is for reference. If one would wanna do a manual decompression.
fn compress_deflate(uncompressed_buffer: &[u8]) -> Vec<u8> {
	let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
	e.write(uncompressed_buffer).unwrap();
	e.finish().expect("Deflate: Failed to compress data")
}


/// We will publish and recieve sample data.
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

    // Publisher thread.
    thread::spawn(move || {
        loop {
            let payload = get_serialized_payload().unwrap();
            thread::sleep(Duration::from_millis(1000));
            mqtt_client.publish("/devices/digital_twin/events/v1/buffered_channel", QoS::AtLeastOnce, false, payload).unwrap();
        }
    });

    // Current thread acts as client on the topic.
    // In this thread we will take raw bytes that we receive and try to 
    // create a Rustlang object out the bytes. Ideally one would try to do the 
    // same for other languages.
    for notification in notifications {
        match notification {
            Notification::Publish(v) => {
                let payload = v.payload;
                // We have extracted the payload. now we will attempt to deserialize the bytes.
                // We create a Reader with our Schema file. 
                // Note: This lib parses the header to identify the compression algorithm and applies
                // appropriate algo if required.
                let reader = Reader::with_schema(&schema, &payload[..]).unwrap();   // --> This is a reader
                // a vec to collect data.
                let mut vec_d: Vec<G2DataRes> = Vec::new();

                // our payload was a list of data, so one by one we deseriaize each one.
                for record in reader {
                    let mut d = from_value::<G2DataRes>(&record.unwrap()).unwrap();    // -> raw bytes to an Object 
                    vec_d.push(d);
                }
                println!("Deserialized data={:?}", vec_d);

            },
            _ => println!("{:?}", notification),
        };
    }

}


