use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;
use serde::{Serialize, Deserialize};
use std::fs;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: String,
    b: String,
    c: Option<String>,
}

fn main() -> Result<(), Error> {
    let filename = "av.avsc";
    let contents = fs::read_to_string(filename).
        expect("No Rad");
    println!("contents are {:?}", contents);

    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "string"},
                {"name": "c",  "type": ["null", "string"]}
            ]
        }
    "#;

    let schema = Schema::parse_str(raw_schema)?;


    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    let mut check : BTreeMap<String, String> = BTreeMap::new();
    check.insert("a".to_owned(), "100".to_owned());
    check.insert("b".to_owned(), "200".to_owned());
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