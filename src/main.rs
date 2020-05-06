use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::prelude::*;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: Option<String>,
    b: Option<String>,
    c: Option<String>,
}

fn main() -> Result<(), Error> {
    let filename = "av.avsc";
    // let contents = fs::read_to_string(filename).
    //     expect("No Rad");
    // println!("contents are {:?}", contents);

    let mut file = File::open("/home/amit/rust_samples/avr_ser/av.avsc")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let schema = Schema::parse_str(&contents)?;


    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    let mut check : BTreeMap<String, Option<String>> = BTreeMap::new();
    check.insert("a".to_owned(), Some("100".to_owned()));
    check.insert("b".to_owned(), Some("200".to_owned()));
    check.insert("c".to_owned(), None);
    for (key, value) in check.iter(){
        record.put(key, value.clone());
    }

   writer.append(record)?;

    // let test = Test {
    //     a: "27".to_owned(),
    //     b: "foo".to_owned(),
    //     c: None,
    // };

    // writer.append_ser(test)?;

    writer.flush()?;

    let input = writer.into_inner();
    let reader = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", from_value::<Test>(&record?));
    }
    Ok(())
}

#[test]
    fn test_default() -> Result<(), Error> {

        #[derive(Debug, Serialize, Deserialize)]
        struct Test {
            a: String,
            b: String,
            c: Option<String>,
        }

        let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"}
            ]
        }
        "#;

        let raw_schema_r = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c",  "type": ["null", "string"], "default": "null"}
            ]
        }"#;

        let schema = Schema::parse_str(raw_schema)?;
        let schema_r = Schema::parse_str(raw_schema_r)?;

        println!("==============");
        println!("{:?}", schema);

        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", "antonio".to_owned());
        record.put("b", "antonio".to_owned());

        writer.append(record)?;

        writer.flush()?;

        let input = writer.into_inner();
        let reader = Reader::with_schema(&schema_r, &input[..])?;

        for record in reader {
            println!("{:?}", from_value::<Test>(&record?));
        }
        Ok(())
     }