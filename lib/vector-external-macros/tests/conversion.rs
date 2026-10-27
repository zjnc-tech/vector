use std::collections::{BTreeMap, HashMap};

use vector_core::event::{
    external::{EncodeErrorKind, PathSegment},
    Event, Value,
};
use vector_external_macros::ExternalSourceEvent;

#[derive(ExternalSourceEvent, serde::Serialize)]
struct Allocation {
    pid: u32,
    size: u64,
}

#[test]
fn converts_and_checks_integer_boundary() {
    let event = Event::try_from(Allocation {
        pid: 123,
        size: i64::MAX as u64,
    })
    .unwrap();
    assert_eq!(event.as_log().get("pid"), Some(&Value::Integer(123)));
    assert_eq!(event.as_log().get("size"), Some(&Value::Integer(i64::MAX)));

    let error = Event::try_from(Allocation {
        pid: 123,
        size: i64::MAX as u64 + 1,
    })
    .unwrap_err();
    assert_eq!(error.kind, EncodeErrorKind::IntegerOverflow);
    assert_eq!(error.path, vec![PathSegment::Field("size")]);
}

type Samples = Vec<f32>;

#[derive(ExternalSourceEvent)]
#[external(event_module = "vector_core::event")]
struct Collections {
    r#type: String,
    #[external(rename = "process.pid")]
    process: i16,
    samples: Samples,
    names: Vec<String>,
    sizes: Vec<u64>,
    labels: HashMap<String, String>,
    counts: BTreeMap<String, i32>,
    temperatures: HashMap<String, f64>,
}

fn collections() -> Collections {
    Collections {
        r#type: "分配".into(),
        process: -12,
        samples: vec![1.5, -2.5],
        names: vec!["gpu0".into(), "gpu1".into()],
        sizes: vec![],
        labels: HashMap::from([("device".into(), "gpu0".into())]),
        counts: BTreeMap::from([("calls".into(), 2)]),
        temperatures: HashMap::from([("gpu0".into(), 42.5)]),
    }
}

#[test]
fn converts_containers_aliases_and_literal_names() {
    let event = Event::try_from(collections()).unwrap();
    let Value::Object(fields) = event.as_log().value() else {
        panic!("expected object")
    };
    assert_eq!(fields["type"], Value::from("分配"));
    assert_eq!(fields["process.pid"], Value::Integer(-12));
    assert!(!fields.contains_key("process"));
    assert_eq!(fields["sizes"], Value::Array(vec![]));
    assert_eq!(
        fields["samples"],
        Value::Array(vec![Value::from(1.5), Value::from(-2.5)])
    );
    assert_eq!(
        fields["names"],
        Value::Array(vec![Value::from("gpu0"), Value::from("gpu1")])
    );
    assert_eq!(
        event.as_log().get("labels.device"),
        Some(&Value::from("gpu0"))
    );
    assert_eq!(event.as_log().get("counts.calls"), Some(&Value::Integer(2)));
    assert_eq!(
        event.as_log().get("temperatures.gpu0"),
        Some(&Value::from(42.5))
    );
}

#[test]
fn reports_container_locations() {
    let mut input = collections();
    input.sizes = vec![1, u64::MAX];
    let error = Event::try_from(input).unwrap_err();
    assert_eq!(
        error.path,
        vec![PathSegment::Field("sizes"), PathSegment::Index(1)]
    );

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut input = collections();
        input.temperatures.insert("gpu0".into(), value);
        let error = Event::try_from(input).unwrap_err();
        assert_eq!(error.kind, EncodeErrorKind::NonFiniteFloat);
        assert_eq!(
            error.path,
            vec![
                PathSegment::Field("temperatures"),
                PathSegment::Key("gpu0".into())
            ]
        );
    }
}

#[derive(ExternalSourceEvent)]
struct AllScalars {
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    isize: isize,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    usize: usize,
    f32: f32,
    f64: f64,
    string: String,
}

#[test]
fn supports_all_declared_scalars() {
    let event = Event::try_from(AllScalars {
        i8: -1,
        i16: -1,
        i32: -1,
        i64: -1,
        isize: -1,
        u8: 1,
        u16: 1,
        u32: 1,
        u64: 1,
        usize: 1,
        f32: 1.5,
        f64: 1.5,
        string: String::new(),
    })
    .unwrap();
    for field in ["i8", "i16", "i32", "i64", "isize"] {
        assert_eq!(event.as_log().get(field), Some(&Value::Integer(-1)));
    }
    for field in ["u8", "u16", "u32", "u64", "usize"] {
        assert_eq!(event.as_log().get(field), Some(&Value::Integer(1)));
    }
}
