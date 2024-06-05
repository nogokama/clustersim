use dslab_scheduling::monitoring::{ResourceLoad, ResourcePoint};

#[test]
fn test_monitoring() {
    let mut load = ResourceLoad::new_fraction(0., 100.0, Some(10.0));
    load.update(50., 5.);
    load.update(0., 11.);
    load.update(100., 0.);

    let expected = vec![ResourcePoint {
        value: 0.25,
        time: 10.,
    }];
    assert_eq!(load.dump(), expected);
}
